/**
 *  TcpConnected.h
 * 
 *  The actual tcp connection - this is the "_impl" of a tcp-connection after
 *  the hostname was resolved into an IP address
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "tcpoutbuffer.h"
#include "tcpinbuffer.h"
#include "tcpextstate.h"
#include "poll.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class TcpConnected : public TcpExtState
{
private:
    /**
     *  The outgoing buffer
     *  @var TcpOutBuffer
     */
    TcpOutBuffer _out;
    
    /**
     *  An incoming buffer
     *  @var TcpInBuffer
     */
    TcpInBuffer _in;
    
    /**
     *  Cached reallocation instruction
     *  @var size_t
     */
    size_t _reallocate = 0;
    
    /**
     *  Did the user ask to elegantly close the connection?
     *  @var bool
     */
    bool _closed = false;

    
    /**
     *  Helper method to report an error
     *  This method also assumes that result <= 0 is an error, unless the operation can be retried.
     *  @param  result      Result of the previous call to read() or write()
     *  @return bool        Was an error reported?
     */
    bool reportError(ssize_t result)
    {
        // positive return values are no errors
        if (result > 0) return false;
        
        // some errors are ok and do not (necessarily) mean that we're disconnected
        if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return false;

        // tell the parent that it failed (but not if the connection was elegantly closed)
        if (!_closed) _parent->onError(this, "connection lost");

        // done
        return true;
    }
    
    /**
     *  Construct the final state
     *  @param  monitor     Object that monitors whether connection still exists
     *  @return TcpState*
     */
    TcpState *finalState(const Monitor &monitor)
    {
        // if the object is still in a valid state, we can treat the connection
        // as closed otherwise there is no point in moving to a next state
        return monitor.valid() ? new TcpClosed(this) : nullptr;
    }
    
public:
    /**
     *  Constructor
     *  @param  state       The previous state
     *  @param  buffer      The buffer that was already built
     */
    TcpConnected(TcpExtState *state, TcpOutBuffer &&buffer) : 
        TcpExtState(state),
        _out(std::move(buffer)),
        _in(4096)
    {
        // if there is already an output buffer, we have to send out that first
        if (_out) _out.sendto(_socket);
        
        // tell the handler to monitor the socket, if there is an out
        _parent->onIdle(this, _socket, _out ? readable | writable : readable);
    }
    
    /**
     *  Destructor
     */
    virtual ~TcpConnected() noexcept = default;

    /**
     *  Number of bytes in the outgoing buffer
     *  @return std::size_t
     */
    virtual std::size_t queued() const override { return _out.size(); }

    /**
     *  Process the filedescriptor in the object
     *  @param  monitor     Monitor to check if the object is still alive
     *  @param  fd          Filedescriptor that is active
     *  @param  flags       AMQP::readable and/or AMQP::writable
     *  @return             New state object
     */
    virtual TcpState *process(const Monitor &monitor, int fd, int flags) override
    {
        // must be the socket
        if (fd != _socket) return this;

        // can we write more data to the socket?
        if (flags & writable)
        {
            // send out the buffered data
            auto result = _out.sendto(_socket);
            
            // are we in an error state? (0 bytes sent is weird, but not necessarily an error)
            if (result < 0 && reportError(result)) return finalState(monitor);
            
            // if we still have a buffer, we keep on monitoring
            if (_out) return this;
            
            // if we do not expect to send more data, we can close the connection for writing
            if (_closed) shutdown(_socket, SHUT_WR);
            
            // check for readability (to find more data, or to be notified that connection is gone)
            _parent->onIdle(this, _socket, readable);
        }
        
        // should we check for readability too?
        if (flags & readable)
        {
            // read data from buffer
            ssize_t result = _in.receivefrom(_socket, _parent->expected());
            
            // did we encounter end-of-file or are we in an error state?
            if (reportError(result)) return finalState(monitor);
            
            // we need a local copy of the buffer - because it is possible that "this"
            // object gets destructed halfway through the call to the parse() method
            TcpInBuffer buffer(std::move(_in));
            
            // parse the buffer
            auto processed = _parent->onReceived(this, buffer);

            // "this" could be removed by now, check this
            if (!monitor.valid()) return nullptr;
            
            // shrink buffer
            buffer.shrink(processed);
            
            // restore the buffer as member
            _in = std::move(buffer);
            
            // do we have to reallocate?
            if (_reallocate) _in.reallocate(_reallocate); 
            
            // we can remove the reallocate instruction
            _reallocate = 0;
        }
        
        // keep same object
        return this;
    }

    /**
     *  Send data over the connection
     *  @param  buffer      buffer to send
     *  @param  size        size of the buffer
     */
    virtual void send(const char *buffer, size_t size) override
    {
        // we stop sending when connection is closed
        if (_closed) return;
        
        // is there already a buffer of data that can not be sent?
        if (_out) return _out.add(buffer, size);

        // there is no buffer, send the data right away
        auto result = ::send(_socket, buffer, size, AMQP_CPP_MSG_NOSIGNAL);

        // number of bytes sent
        size_t bytes = result < 0 ? 0 : result;

        // ok if all data was sent
        if (bytes >= size) return;
    
        // add the data to the buffer
        _out.add(buffer + bytes, size - bytes);
        
        // start monitoring the socket to find out when it is writable
        _parent->onIdle(this, _socket, readable | writable);
    }
    
    /**
     *  Gracefully close the connection
     */
    virtual void close() override 
    {
        // do nothing if already closed
        if (_closed) return;

        // remember that the connection is closed
        _closed = true;
        
        // wait until the outgoing buffer is all gone
        if (_out) return;
        
        // we will shutdown the socket in a very elegant way, we notify the peer 
        // that we will not be sending out more write operations
        shutdown(_socket, SHUT_WR);
        
        // we still monitor the socket for readability to see if our close call was
        // confirmed by the peer
        _parent->onIdle(this, _socket, readable);
    }

    /**
     *  Install max-frame size
     *  @param  heartbeat   suggested heartbeat
     */
    virtual void maxframe(size_t maxframe) override
    {
        // remember that we have to reallocate (_in member can not be accessed because it is moved away)
        _reallocate = maxframe;
    }
};
    
/**
 *  End of namespace
 */
}

