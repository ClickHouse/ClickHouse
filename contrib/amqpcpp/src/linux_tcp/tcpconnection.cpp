/**
 *  TcpConnection.cpp
 *
 *  Implementation file for the TCP connection
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */

/**
 *  Dependencies
 */
#include "includes.h"
#include "tcpresolver.h"
#include "tcpstate.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Constructor
 *  @param  handler         User implemented handler object
 *  @param  hostname        The address to connect to
 */
TcpConnection::TcpConnection(TcpHandler *handler, const Address &address) :
    _handler(handler),
    _state(new TcpResolver(this, address.hostname(), address.port(), address.secure())),
    _connection(this, address.login(), address.vhost()) 
{
    // tell the handler
    _handler->onAttached(this);
}

/**
 *  Destructor
 */
TcpConnection::~TcpConnection() noexcept = default;

/**
 *  The filedescriptor that is used for this connection
 *  @return int
 */
int TcpConnection::fileno() const
{
    // pass on to the state object
    return _state->fileno();
}

/**
 *  The number of outgoing bytes queued on this connection.
 *  @return std::size_t
 */
std::size_t TcpConnection::queued() const
{
    return _state->queued();
}

/**
 *  Is the connection closed and full dead? The entire TCP connection has been discarded.
 *  @return bool
 */
bool TcpConnection::closed() const
{
    return _state->closed();
}

/**
 *  Process the TCP connection
 *  This method should be called when the filedescriptor that is registered
 *  in the event loop becomes active. You should pass in a flag holding the
 *  flags AMQP::readable or AMQP::writable to indicate whether the descriptor
 *  was readable or writable, or bitwise-or if it was both
 *  @param  fd              The filedescriptor that became readable or writable
 *  @param  events          What sort of events occured?
 */
void TcpConnection::process(int fd, int flags)
{
    // monitor the object for destruction, because you never know what the user
    Monitor monitor(this);

    // remember the old state
    auto *oldstate = _state.get();
        
    // pass on the the state, that returns a new impl
    auto *newstate = _state->process(monitor, fd, flags);

    // if the state did not change, we do not have to update a member,
    // when the newstate is nullptr, the object is (being) destructed
    // and we do not have to do anything else either
    if (newstate == nullptr || newstate == oldstate) return;

    // wrap the new state in a unique-ptr so that so that the old state
    // is not destructed before the new one is assigned
    std::unique_ptr<TcpState> ptr(newstate);
    
    // swap the two pointers (this ensures that the last operation of this
    // method is to destruct the old state, which possible results in calls
    // to user-space and the destruction of "this"
    _state.swap(ptr);

}

/**
 *  Close the connection
 *  @return bool
 */
bool TcpConnection::close(bool immediate)
{
    // if no immediate disconnect is needed, we can simply start the closing handshake
    if (!immediate) return _connection.close();

    // failing the connection could destruct "this"
    Monitor monitor(this);
    
    // fail the connection / report the error to user-space
    bool failed = _connection.fail("connection prematurely closed by client");
    
    // stop if object was destructed
    if (!monitor.valid()) return true;

    // tell the handler that the connection was closed
    if (failed) _handler->onError(this, "connection prematurely closed by client");

    // stop if object was destructed
    if (!monitor.valid()) return true;
    
    // change the state
    _state.reset(new TcpClosed(this));
    
    // done, we return true because the connection is closed
    return true;
}

/**
 *  Method that is called when the RabbitMQ server and your client application  
 *  exchange some properties that describe their identity.
 *  @param  connection      The connection about which information is exchanged
 *  @param  server          Properties sent by the server
 *  @param  client          Properties that are to be sent back
 */
void TcpConnection::onProperties(Connection *connection, const Table &server, Table &client)
{
    // tell the handler
    return _handler->onProperties(this, server, client);
}

/**
 *  Method that is called when the heartbeat frequency is negotiated.
 *  @param  connection      The connection that suggested a heartbeat interval
 *  @param  interval        The suggested interval from the server
 *  @return uint16_t        The interval to use
 */
uint16_t TcpConnection::onNegotiate(Connection *connection, uint16_t interval)
{
    // tell the max-frame size
    _state->maxframe(connection->maxFrame());
    
    // tell the handler
    return _handler->onNegotiate(this, interval);
}

/**
 *  Method that is called by the connection when data needs to be sent over the network
 *  @param  connection      The connection that created this output
 *  @param  buffer          Data to send
 *  @param  size            Size of the buffer
 */
void TcpConnection::onData(Connection *connection, const char *buffer, size_t size)
{
    // send the data over the connection
    _state->send(buffer, size);
}

/**
 *  Method called when the AMQP connection ends up in an error state
 *  @param  connection      The connection that entered the error state
 *  @param  message         Error message
 */
void TcpConnection::onError(Connection *connection, const char *message)
{
    // monitor to check if "this" is destructed
    Monitor monitor(this);
    
    // tell this to the user
    _handler->onError(this, message);
    
    // object could be destructed by user-space
    if (!monitor.valid()) return;
    
    // tell the state that the connection should be closed asap
    _state->close();
}

/**
 *  Method that is called when the connection was closed.
 *  @param  connection      The connection that was closed and that is now unusable
 */
void TcpConnection::onClosed(Connection *connection)
{
    // tell the state that the connection should be closed asap
    _state->close();
    
    // report to the handler
    _handler->onClosed(this);
}

/**
 *  Method that is called when an error occurs (the connection is lost)
 *  @param  state
 *  @param  error
 *  @param  connected
 */
void TcpConnection::onError(TcpState *state, const char *message, bool connected)
{
    // monitor to check if all operations are active
    Monitor monitor(this);
    
    // if there are still pending operations, they should be reported as error
    bool failed = _connection.fail(message);
    
    // stop if object was destructed
    if (!monitor.valid()) return;

    // tell the handler
    if (failed) _handler->onError(this, message);

    // if the object is still connected, we only have to report the error and
    // we wait for the subsequent call to the onLost() method
    if (connected || !monitor.valid()) return;
    
    // tell the handler that no further events will be fired
    _handler->onDetached(this);
}

/**
 *  Method to be called when it is detected that the connection was lost
 *  @param  state
 */
void TcpConnection::onLost(TcpState *state)
{
    // monitor to check if "this" is destructed
    Monitor monitor(this);
    
    // tell the handler
    _handler->onLost(this);
    
    // leap out if object was destructed
    if (!monitor.valid()) return;
    
    // tell the handler that no further events will be fired
    _handler->onDetached(this);
}

/**
 *  End of namespace
 */
}

