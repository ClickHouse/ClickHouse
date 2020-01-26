/**
 *  Connection implementation
 *
 *  This is the implementation of the connection - a class that can only be
 *  constructed by the connection class itselves and that has all sorts of
 *  methods that are only useful inside the library
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "watchable.h"
#include "connectionhandler.h"
#include "channelimpl.h"
#include "copiedbuffer.h"
#include "monitor.h"
#include "login.h"
#include <unordered_map>
#include <memory>
#include <queue>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class Connection;
class Buffer;
class Frame;

/**
 *  Class definition
 */
class ConnectionImpl : public Watchable
{
protected:
    /**
     *  The parent connection object
     *  @var    Connection
     */
    Connection *_parent;

    /**
     *  The connection handler
     *  @var    ConnectionHandler
     */
    ConnectionHandler *_handler;

    /**
     *  State of the connection
     *  The current state is the last frame sent to the server
     *  @var    enum
     */
    enum {
        state_protocol,             // protocol headers are being passed
        state_handshake,            // busy with the handshake to open the connection
        state_connected,            // connection is set up and ready for communication
        state_closing,              // connection is busy closing (we have sent the close frame)
        state_closed                // connection is closed
    } _state = state_protocol;

    /**
     *  Has the close() method been called? If this is true, we automatically
     *  send a close-frame after all pending operations are finsihed.
     *  @var    bool
     */
    bool _closed = false;

    /**
     *  All channels that are active
     *  @var    std::unordered_map<uint16_t, std::shared_ptr<ChannelImpl>>
     */
    std::unordered_map<uint16_t, std::shared_ptr<ChannelImpl>> _channels;

    /**
     *  The last unused channel ID
     *  @var    uint16_t
     */
    uint16_t _nextFreeChannel = 1;

    /**
     *  Max number of channels (0 for unlimited)
     *  @var    uint16_t
     */
    uint16_t _maxChannels = 0;

    /**
     *  Max frame size
     *  @var    uint32_t
     */
    uint32_t _maxFrame = 4096;

    /**
     *  Number of expected bytes that will hold the next incoming frame
     *  We start with seven because that is the header of a frame
     *  @var    uint32_t
     */
    uint32_t _expected = 7;

    /**
     *  The login for the server (login, password)
     *  @var    Login
     */
    Login _login;

    /**
     *  Vhost to connect to
     *  @var    string
     */
    std::string _vhost;

    /**
     *  Queued messages that should be sent after the connection has been established
     *  @var    queue
     */
    std::queue<CopiedBuffer> _queue;
    
    /**
     *  Helper method to send the close frame
     *  Return value tells if the connection is still valid
     *  @return bool
     */
    bool sendClose();

    /**
     *  Is any channel waiting for an answer on a synchronous call?
     *  @return bool
     */
    bool waitingChannels() const;

    /**
     *  Is the channel waiting for a response from the peer (server)
     *  @return bool
     */
    bool waiting() const;
    
    /**
     *  Helper method for the fail() method
     *  @param  monitor
     *  @param  message
     *  @return bool
     */
    bool fail(const Monitor &monitor, const char *message);

private:
    /**
     *  Construct an AMQP object based on full login data
     *
     *  The first parameter is a handler object. This handler class is
     *  an interface that should be implemented by the caller.
     *
     *  Note that the constructor is private to ensure that nobody can construct
     *  this class, only the real Connection class via a friend construct
     *
     *  @param  parent          Parent connection object
     *  @param  handler         Connection handler
     *  @param  login           Login data
     */
    ConnectionImpl(Connection *parent, ConnectionHandler *handler, const Login &login, const std::string &vhost);

public:
    /**
     *  Copy'ing connections is impossible
     *  @param  connection
     */
    ConnectionImpl(const ConnectionImpl &connection) = delete;

    /**
     *  Destructor
     */
    virtual ~ConnectionImpl();

    /**
     *  No assignments of other connections
     *  @param  connection
     *  @return ConnectionImpl
     */
    ConnectionImpl &operator=(const ConnectionImpl &connection) = delete;

    /**
     *  What is the state of the connection - is the protocol handshake completed?
     *  @return bool
     */
    bool protocolOk() const
    {
        // must be busy doing the connection handshake, or already connected
        return _state == state_handshake || _state == state_connected;
    }

    /**
     *  Mark the protocol as being ok
     *  @param  server      properties sent by the server
     *  @param  client      properties to be send back
     */
    void setProtocolOk(const Table &server, Table &client)
    {
        // if object is destructed
        Monitor monitor(this);
        
        // check if user-space wants to set these properties
        _handler->onProperties(_parent, server, client);
        
        // leap out if userspace destructed the object
        if (!monitor.valid()) return;

        // move on to handshake state
        if (_state == state_protocol) _state = state_handshake;
    }

    /**
     *  Are we fully connected and ready for instructions? This is true after the initial
     *  protocol and login handshake were completed.
     *  @return bool
     */
    bool ready() const
    {
        // state must be connected
        return _state == state_connected;
    }

    /**
     *  Are we closing down?
     *  @return bool
     */
    bool closing() const
    {
        // state must be connected
        return _state == state_closing;
    }

    /**
     *  Are we closed?
     *  @return bool
     */
    bool closed() const
    {
        // state must be connected
        return _state == state_closed;
    }
    
    /**
     *  Is the connection in a usable state / not yet closed?
     *  @return bool
     */
    bool usable() const
    {
        return (_state == state_protocol || _state == state_handshake || _state == state_connected) && !_closed;
    }

    /**
     *  Mark the connection as ready
     */
    void setReady();

    /**
     *  Retrieve the login data
     *  @return Login
     */
    const Login &login() const
    {
        return _login;
    }

    /**
     *  Retrieve the vhost
     *  @return string
     */
    const std::string &vhost() const
    {
        return _vhost;
    }

    /**
     *  Store the max number of channels and max number of frames
     *  @param  channels    max number of channels
     *  @param  size        max frame size
     */
    void setCapacity(uint16_t channels, uint32_t size)
    {
        _maxChannels = channels;
        _maxFrame = size;
    }

    /**
     *  The max frame size
     *  @return uint32_t
     */
    uint32_t maxFrame() const
    {
        return _maxFrame;
    }

    /**
     *  The max payload size for body frames
     *  @return uint32_t
     */
    uint32_t maxPayload() const
    {
        // 8 bytes for header and end-of-frame byte
        return _maxFrame - 8;
    }

    /**
     *  The number of bytes that can best be passed to the next call to the parse() method
     *  @return uint32_t
     */
    uint32_t expected() const
    {
        return _expected;
    }

    /**
     *  Add a channel to the connection, and return the channel ID that it
     *  is allowed to use, or 0 when no more ID's are available
     *  @param  channel
     *  @return uint16_t
     */
    uint16_t add(const std::shared_ptr<ChannelImpl> &channel);

    /**
     *  Remove a channel
     *  @param  channel
     */
    void remove(const ChannelImpl *channel);

    /**
     *  Parse the buffer into a recognized frame
     *
     *  Every time that data comes in on the connection, you should call this method to parse
     *  the incoming data, and let it handle by the AMQP library. This method returns the number
     *  of bytes that were processed.
     *
     *  If not all bytes could be processed because it only contained a partial frame, you should
     *  call this same method later on when more data is available. The AMQP library does not do
     *  any buffering, so it is up to the caller to ensure that the old data is also passed in that
     *  later call.
     *
     *  @param  buffer      buffer to decode
     *  @return             number of bytes that were processed
     */
    uint64_t parse(const Buffer &buffer);

    /**
     *  Fail all pending - this can be called by user-space when it is recognized that the 
     *  underlying connection is lost. All error-handlers for all operations and open
     *  channels will be called. This will _not_ call ConnectionHandler::onError() method.
     *  
     *  @return bool
     */
    bool fail(const char *message);

    /**
     *  Close the connection
     *  This will also close all channels
     *  @return bool
     */
    bool close();

    /**
     *  Send a frame over the connection
     *
     *  This is an internal method that you normally do not have to call yourself
     *
     *  @param  frame       the frame to send
     *  @return bool
     */
    bool send(const Frame &frame);

    /**
     *  Send buffered data over the connection
     *
     *  @param  buffer      the buffer with data to send
     */
    bool send(CopiedBuffer &&buffer);

    /**
     *  Get a channel by its identifier
     *
     *  This method only works if you had already created the channel before.
     *  This is an internal method that you will not need if you cache the channel
     *  object.
     *
     *  @param  number          channel identifier
     *  @return channel         the channel object, or nullptr if not yet created
     */
    std::shared_ptr<ChannelImpl> channel(int number)
    {
        auto iter = _channels.find(number);
        return iter == _channels.end() ? nullptr : iter->second;
    }

    /**
     *  Report an error message
     *  @param  message
     */
    void reportError(const char *message);

    /**
     *  Report that the connection is closed
     */
    void reportClosed()
    {
        // change state
        _state = state_closed;

        // inform the handler
        _handler->onClosed(_parent);
    }

    /**
     *  Retrieve the amount of channels this connection has
     *  @return std::size_t
     */
    std::size_t channels() const
    {
        return _channels.size();
    }

    /**
     *  Set the heartbeat timeout
     *  @param  heartbeat       suggested heartbeat timeout by server
     *  @return uint16_t        accepted heartbeat timeout from client
     */
    uint16_t setHeartbeat(uint16_t heartbeat)
    {
        // pass to the handler
        return _handler->onNegotiate(_parent, heartbeat);
    }

    /**
     *  Report a heartbeat to the connection handler
     */
    void reportHeartbeat()
    {
        // pass to handler
        _handler->onHeartbeat(_parent);
    }
    
    /**
     *  Send a heartbeat to keep the connection alive
     *  @return bool
     */
    bool heartbeat();

    /**
     *  The actual connection is a friend and can construct this class
     */
    friend class Connection;
    friend class ChannelImpl;
};

/**
 *  End of namespace
 */
}

