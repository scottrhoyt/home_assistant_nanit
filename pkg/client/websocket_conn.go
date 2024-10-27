package client

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/combmag/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sacOO7/gowebsocket"
	"google.golang.org/protobuf/proto"
)

// WebsocketMessageHandler - message handler
type WebsocketMessageHandler func(*Message, *WebsocketConnection)

// WebsocketConnection - ready websocket connection
type WebsocketConnection struct {
	socket *gowebsocket.Socket

	msgHandlersMu sync.RWMutex
	msgHandlers   []WebsocketMessageHandler

	resHandlersMu sync.RWMutex
	resHandlers   map[int32]unhandledRequest

	lastRequestID int32
}

// NewWebsocketConnection - constructor
func NewWebsocketConnection(socket *gowebsocket.Socket) *WebsocketConnection {
	return &WebsocketConnection{
		socket:        socket,
		resHandlers:   make(map[int32]unhandledRequest),
		lastRequestID: 0,
	}
}

// Add this method to expose the WebsocketConnection
func (manager *WebsocketConnectionManager) GetConnection() *WebsocketConnection {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if manager.readyState != nil {
		return manager.readyState.Connection
	}
	return nil
}

// RegisterMessageHandler - registers handler which will be called whenever new message is received
func (conn *WebsocketConnection) RegisterMessageHandler(handler WebsocketMessageHandler) {
	conn.msgHandlersMu.Lock()
	conn.msgHandlers = append(conn.msgHandlers, handler)
	conn.msgHandlersMu.Unlock()
}

// SendMessage - low-level helper for sending raw message
// Note: Use SendRequest() for requests
func (conn *WebsocketConnection) SendMessage(m *Message) {
	var msg *zerolog.Event

	if *m.Type == Message_KEEPALIVE {
		msg = log.Trace()
	} else {
		msg = log.Debug()
	}

	msg.Stringer("data", m).Msg("Sending message")

	bytes := getMessageBytes(m)
	log.Trace().Bytes("rawdata", bytes).Msg("Sending data")

	conn.socket.SendBinary(bytes)
}

// SendRequest - sends request to the cam and returns await function. Await function waits for the response and returns it
func (conn *WebsocketConnection) SendRequest(reqType RequestType, requestData *Request) func(time.Duration) (*Response, error) {
	// Build request
	id := atomic.AddInt32(&conn.lastRequestID, 1)

	requestData.Id = utils.ConstRefInt32(id)
	requestData.Type = RequestType(reqType).Enum()

	m := &Message{
		Type:    Message_Type(Message_REQUEST).Enum(),
		Request: requestData,
	}

	// Response handling
	resC := make(chan *Response, 1)

	conn.resHandlersMu.Lock()
	conn.resHandlers[id] = unhandledRequest{
		Request: m.Request,
		HandleResponse: func(res *Response) {
			select {
			case <-resC:
				log.Printf("Request ID: %d, StatusCode: %v, StatusMessage: %s", id, res.StatusCode, res.GetStatusMessage())
				return // Channel already closed (ie. timeout)
			default:
				resC <- res
			}
		},
	}
	conn.resHandlersMu.Unlock()

	// Send request
	conn.SendMessage(m)

	// Return awaiter
	return func(timeout time.Duration) (*Response, error) {
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			resa := <-resC
			close(resC)
			log.Info().Interface("Timeout Error: %v", resa.StatusCode).Msg(("Timeout"))
			return nil, errors.New("Request timeout.")
		case res := <-resC:
			close(resC)
			timer.Stop()

			if res.StatusCode == nil {
				return res, errors.New("No status code received")
			} else if *res.StatusCode != 200 {
				if res.GetStatusMessage() != "" {
					return res, errors.New(res.GetStatusMessage())
				}

				return res, fmt.Errorf("Unexpected status code %v", *res.StatusCode)
			}

			return res, nil
		}
	}
}

type unhandledRequest struct {
	Request        *Request
	HandleResponse func(response *Response)
}

func (conn *WebsocketConnection) handleResponse(r *Response) {
	requestID := *r.RequestId
	requestType := *r.RequestType

	conn.resHandlersMu.RLock()
	unhandledReqCandidate, ok := conn.resHandlers[requestID]
	conn.resHandlersMu.RUnlock()

	if ok && requestType == *unhandledReqCandidate.Request.Type {
		conn.resHandlersMu.Lock()
		delete(conn.resHandlers, requestID)
		conn.resHandlersMu.Unlock()

		unhandledReqCandidate.HandleResponse(r)
	}
}

func (conn *WebsocketConnection) handleMessage(m *Message) {
	if *m.Type == Message_RESPONSE && m.Response != nil {
		conn.handleResponse(m.Response)
	}

	conn.msgHandlersMu.RLock()
	subscribedHandlers := make([]WebsocketMessageHandler, len(conn.msgHandlers))
	copy(subscribedHandlers, conn.msgHandlers)
	conn.msgHandlersMu.RUnlock()

	for _, handler := range subscribedHandlers {
		handler(m, conn)
	}
}

func getMessageBytes(data *Message) []byte {
	out, err := proto.Marshal(data)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to marshal data")
	}

	return out
}
