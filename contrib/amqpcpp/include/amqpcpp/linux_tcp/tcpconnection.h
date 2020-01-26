/**
 *  TcpConnection.h
 *
 *  Extended Connection object that creates a TCP connection for the
 *  IO between the client application and the RabbitMQ server.
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class TcpState;
class TcpChannel;

/**
 *  Class definition
 */
class TcpConnection :
    private ConnectionHandler,
    private Watchable,
    private TcpParent
{
private:
    /**
     *  User-space handler object
     *  @var    TcpHandler
     */
    TcpHandler *_handler;

    /**
     *  The state of the TCP connection - this state objecs changes based on 
     *  the state of the connection (resolving, connected or closed)
     *  @var    std::unique_ptr<TcpState>
     */
    std::unique_ptr<TcpState> _state;

    /**
     *  The underlying AMQP connection
     *  @var    Connection
     */
    Connection _connection;

    /**
     *  The channel may access out _connection
     *  @friend
     */
    friend TcpChannel;
    

    /**
     *  Method that is called when the RabbitMQ server and your client application  
     *  exchange some properties that describe their identity.
     *  @param  connection      The connection about which information is exchanged
     *  @param  server          Properties sent by the server
     *  @param  client          Properties that are to be sent back
     */
    virtual void onProperties(Connection *connection, const Table &server, Table &client) override;

    /**
     *  Method that is called when the heartbeat frequency is negotiated.
     *  @param  connection      The connection that suggested a heartbeat interval
     *  @param  interval        The suggested interval from the server
     *  @return uint16_t        The interval to use
     */
    virtual uint16_t onNegotiate(Connection *connection, uint16_t interval) override;

    /**
     *  Method that is called by the connection when data needs to be sent over the network
     *  @param  connection      The connection that created this output
     *  @param  buffer          Data to send
     *  @param  size            Size of the buffer
     */
    virtual void onData(Connection *connection, const char *buffer, size_t size) override;

    /**
     *  Method that is called when the server sends a heartbeat to the client
     *  @param  connection      The connection over which the heartbeat was received
     */
    virtual void onHeartbeat(Connection *connection) override
    {
        // pass on to tcp handler
        _handler->onHeartbeat(this);
    }

    /**
     *  Method called when the connection ends up in an error state
     *  @param  connection      The connection that entered the error state
     *  @param  message         Error message
     */
    virtual void onError(Connection *connection, const char *message) override;

    /**
     *  Method that is called when the AMQP connection is established
     *  @param  connection      The connection that can now be used
     */
    virtual void onReady(Connection *connection) override
    {
        // pass on to the handler
        _handler->onReady(this);
    }

    /**
     *  Method that is called when the connection was closed.
     *  @param  connection      The connection that was closed and that is now unusable
     */
    virtual void onClosed(Connection *connection) override;
    
    /**
     *  Method that is called when the tcp connection has been established
     *  @param  state
     */
    virtual void onConnected(TcpState *state) override
    {
        // pass on to the handler
        _handler->onConnected(this);
    }

    /**
     *  Method that is called when the connection is secured
     *  @param  state
     *  @param  ssl
     *  @return bool
     */
    virtual bool onSecured(TcpState *state, const SSL *ssl) override
    {
        // pass on to user-space
        return _handler->onSecured(this, ssl);
    }

    /**
     *  Method to be called when data was received
     *  @param  state
     *  @param  buffer
     *  @return size_t
     */
    virtual size_t onReceived(TcpState *state, const Buffer &buffer) override
    {
        // pass on to the connection
        return _connection.parse(buffer);
    }
    
    /**
     *  Method to be called when we need to monitor a different filedescriptor
     *  @param  state
     *  @param  fd
     *  @param  events
     */
    virtual void onIdle(TcpState *state, int socket, int events) override
    {
        // pass on to user-space
        return _handler->monitor(this, socket, events);
    }

    /**
     *  Method that is called when an error occurs (the connection is lost)
     *  @param  state
     *  @param  error
     *  @param  connected
     */
    virtual void onError(TcpState *state, const char *message, bool connected) override;

    /**
     *  Method to be called when it is detected that the connection was lost
     *  @param  state
     */
    virtual void onLost(TcpState *state) override;
    
    /**
     *  The expected number of bytes
     *  @return size_t
     */
    virtual size_t expected() override
    {
        // pass on to the connection
        return _connection.expected();
    }

public:
    /**
     *  Constructor
     *  @param  handler         User implemented handler object
     *  @param  hostname        The address to connect to
     */
    TcpConnection(TcpHandler *handler, const Address &address);
    
    /**
     *  No copying
     *  @param  that
     */
    TcpConnection(const TcpConnection &that) = delete;
    
    /**
     *  Destructor
     */
    virtual ~TcpConnection() noexcept;

    /**
     *  The filedescriptor that is used for this connection
     *  @return int
     */
    int fileno() const;

    /**
     *  Process the TCP connection
     * 
     *  This method should be called when the filedescriptor that is registered
     *  in the event loop becomes active. You should pass in a flag holding the
     *  flags AMQP::readable or AMQP::writable to indicate whether the descriptor
     *  was readable or writable, or bitwise-or if it was both
     * 
     *  @param  fd              The filedescriptor that became readable or writable
     *  @param  events          What sort of events occured?
     */
    void process(int fd, int flags);
    
    /**
     *  Close the connection in an elegant fashion. This closes all channels and the 
     *  TCP connection. Note that the connection is not immediately closed: first all
     *  pending operations are completed, and then an AMQP closing-handshake is
     *  performed. If you pass a parameter "immediate=true" the connection is 
     *  immediately closed, without waiting for earlier commands (and your handler's
     *  onError() method is called about the premature close)
     *  @return bool
     */
    bool close(bool immediate = false);
    
    /**
     *  Is the connection connected, meaning: it has passed the login handshake?
     *  @return bool
     */
    bool ready() const
    {
        return _connection.ready();
    }
    
    /**
     *  Is the connection in a usable state / not yet closed or being closed
     *  When a connection is usable, you can send further commands over it. When it is
     *  unusable, it may still be connected and finished queued commands.
     *  @return bool
     */
    bool usable() const
    {
        return _connection.usable();
    }
    
    /**
     *  Is the connection closed and full dead? The entire TCP connection has been discarded.
     *  @return bool
     */
    bool closed() const;
    
    /**
     *  The max frame size. Useful if you set up a buffer to parse incoming data: it does not have to exceed this size.
     *  @return uint32_t
     */
    uint32_t maxFrame() const
    {
        return _connection.maxFrame();
    }

    /**
     *  The number of bytes that can best be passed to the next call to the parse() method.
     *  @return uint32_t
     */
    uint32_t expected() const
    {
        return _connection.expected();
    }

    /**
      *  Return the number of channels this connection has.
      *  @return std::size_t
      */
    std::size_t channels() const
    {
        // return the number of channels this connection has
        return _connection.channels();
    }

    /**
     *  The number of outgoing bytes queued on this connection.
     *  @return std::size_t
     */
    std::size_t queued() const;
    
    /**
     *  Send a heartbeat
     *  @return bool
     */
    bool heartbeat()
    {
        return _connection.heartbeat();
    }
};

/**
 *  End of namespace
 */
}
