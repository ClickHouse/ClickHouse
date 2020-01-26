/**
 *  TcpState.h
 *
 *  Base class / interface of the various states of the TCP connection
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
 *  Class definition
 */
class TcpState
{
protected:
    /**
     *  Parent object that constructed the state
     *  @var TcpParent
     */
    TcpParent *_parent;

protected:
    /**
     *  Protected constructor
     *  @param  parent      The parent object
     *  @param  handler     User-supplied handler class
     */
    TcpState(TcpParent *parent) : 
        _parent(parent) {}

    /**
     *  Protected "copy" constructor
     *  @param  state       Original TcpState object
     */
    TcpState(const TcpState *state) :
        _parent(state->_parent) {}

public:
    /**
     *  Virtual destructor
     */
    virtual ~TcpState() = default;

    /**
     *  The filedescriptor of this connection
     *  @return int
     */
    virtual int fileno() const { return -1; }

    /**
     *  The number of outgoing bytes queued on this connection.
     *  @return size_t
     */
    virtual std::size_t queued() const { return 0; }
    
    /**
     *  Is this a closed / dead state?
     *  @return bool
     */
    virtual bool closed() const { return false; }

    /**
     *  Process the filedescriptor in the object
     * 
     *  This method should return the handler object that will be responsible for
     *  all future readable/writable events for the file descriptor, or nullptr
     *  if the underlying connection object has already been destructed by the
     *  user and it would be pointless to set up a new handler.
     * 
     *  @param  monitor     Monitor that can be used to check if the tcp connection is still alive
     *  @param  fd          The filedescriptor that is active
     *  @param  flags       AMQP::readable and/or AMQP::writable
     *  @return             New implementation object
     */
    virtual TcpState *process(const Monitor &monitor, int fd, int flags)
    {
        // default implementation does nothing and preserves same implementation
        return this;
    }
    
    /**
     *  Send data over the connection
     *  @param  buffer      Buffer to send
     *  @param  size        Size of the buffer
     */
    virtual void send(const char *buffer, size_t size)
    {
        // default does nothing
    }

    /**
     *  Gracefully start closing the connection
     */
    virtual void close() {}

    /**
     *  Install max-frame size
     *  @param  heartbeat   suggested heartbeat
     */
    virtual void maxframe(size_t maxframe) {}
};

/**
 *  End of namespace
 */
}

