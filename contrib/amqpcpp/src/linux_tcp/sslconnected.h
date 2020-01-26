/**
 * SslConnected.h
 *
 * The actual tcp connection over SSL
 *
 * @copyright 2018 copernica BV
 */
 
/** 
  * Include guard
  */
#pragma once

/**
 * Dependencies
 */
#include "tcpoutbuffer.h"
#include "tcpinbuffer.h"
#include "poll.h"
#include "sslwrapper.h"
#include "sslshutdown.h"

/**
 * Set up namespace
 */
namespace AMQP {

/** 
 * Class definition
 */ 
class SslConnected : public TcpExtState 
{
private:
    /**
     *  The SSL structure
     *  @var SslWrapper
     */
    SslWrapper _ssl;

    /**
     *  The outgoing buffer
     *  @var TcpBuffer
     */
    TcpOutBuffer _out;

    /**
     *  The incoming buffer
     *  @var TcpInBuffer
     */
    TcpInBuffer _in;
    
    /**
     *  Are we now busy with sending or receiving?
     *  @var enum
     */
    enum State {
        state_idle,
        state_sending,
        state_receiving,
        state_error
    } _state;
    
    /**
     *  Should we close the connection after we've finished all operations?
     *  @var bool
     */
    bool _closed = false;
    
    /**
     *  Cached reallocation instruction
     *  @var size_t
     */
    size_t _reallocate = 0;
    

    /**
     *  Proceed with the next operation after the previous operation was
     *  a success, possibly changing the filedescriptor-monitor
     *  @return TcpState*
     */
    TcpState *proceed()
    {
        // if we still have an outgoing buffer we want to send out data
        if (_out)
        {
            // let's wait until the socket becomes writable
            _parent->onIdle(this, _socket, readable | writable);
        }
        else if (_closed)
        {
            // start the state that closes the connection
            return new SslShutdown(this, std::move(_ssl));
        }
        else
        {
            // let's wait until the socket becomes readable
            _parent->onIdle(this, _socket, readable);
        }
        
        // done
        return this;
    }
    
    /**
     *  Method to repeat the previous call
     *  @param  monitor     monitor to check if connection object still exists
     *  @param  state       the state that we were in
     *  @param  result      result of an earlier SSL_get_error call
     *  @return TcpState*
     */
    TcpState *repeat(const Monitor &monitor, enum State state, int error)
    {
        // if we are not able to repeat the call, we are in an error state and should tear down the connection
        if (!repeat(state, error)) return monitor.valid() ? new TcpClosed(this) : nullptr;

        // if the socket was closed in the meantime and we are not sending anything any more, we should initialize the shutdown sequence
        if (_closed && _state == state_idle) return new SslShutdown(this, std::move(_ssl));
        
        // otherwise, we just continue as we were, since the calls should be repeated in the future
        else return this;
    }

    /**
     *  Method to repeat the previous call, which has then been
     *  @param  state       the state that we were in
     *  @param  result      result of an earlier SSL_get_error call
     *  @return bool
     */
    bool repeat(enum State state, int error)
    {
        // check the error
        switch (error) {
        case SSL_ERROR_WANT_READ:
            // remember state
            _state = state;
            
            // the operation must be repeated when readable
            _parent->onIdle(this, _socket, readable);
            
            // allow chaining
            return true;
        
        case SSL_ERROR_WANT_WRITE:
            // remember state
            _state = state;
            
            // wait until socket becomes writable again
            _parent->onIdle(this, _socket, readable | writable);

            // we are done
            return true;

        // this case doesn't actually happen when repeat is called, since it will only be returned when
        // the result > 0 and therefore there is no error. it is here just to be sure.
        case SSL_ERROR_NONE:
            // we're ready for the next instruction from userspace
            _state = state_idle;
            
            // if we still have an outgoing buffer we want to send out data, otherwise we just read
            _parent->onIdle(this, _socket, _out ? readable | writable : readable);

            // nothing is wrong, we are done
            return true;
            
        default:
            // we are now in an error state
            _state = state_error;

            // report an error to user-space
            _parent->onError(this, "ssl protocol error");

            // ssl level error, we have to tear down the tcp connection
            return false;
        }
    }
    
    /**
     *  Parse the received buffer
     *  @param  monitor     object to check the existance of the connection object
     *  @param  size        number of bytes available
     *  @return TcpState
     */
    TcpState *parse(const Monitor &monitor, size_t size)
    {
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
        if (!_reallocate) return this;
        
        // reallocate the buffer
        _in.reallocate(_reallocate); 
        
        // we can remove the reallocate instruction
        _reallocate = 0;
        
        // done
        return this;
    }
    
    /**
     *  Check if the socket is readable
     *  @return bool
     */
    bool isReadable() const
    {
        // object to poll a socket
        Poll poll(_socket);
        
        // wait until socket is readable, but do not block
        return poll.readable(false);
    }

    /**
     *  Check if the socket is writable
     *  @return bool
     */
    bool isWritable() const
    {
        // object to poll a socket
        Poll poll(_socket);
        
        // wait until socket is writable, but do not block
        return poll.writable(false);
    }
    
    /**
     *  Perform a write operation
     *  @param  monitor         object to check the existance of the connection object
     *  @return TcpState*
     */
    TcpState *write(const Monitor &monitor)
    {
        // assume default state
        _state = state_idle;
        
        // because the output buffer contains a lot of small buffers, we can do multiple 
        // operations till the buffer is empty (but only if the socket is not also
        // readable, because then we want to read that data first instead of endless writes
        do
        {
            // try to send more data from the outgoing buffer
            auto result = _out.sendto(_ssl);
            
            // we may have to repeat the operation on failure
            if (result > 0) continue;
            
            // check for error
            auto error = OpenSSL::SSL_get_error(_ssl, result);

            // the operation failed, we may have to repeat our call
            return repeat(monitor, state_sending, error);
        }
        while (_out && !isReadable());
        
        // proceed with the read operation or the event loop
        return isReadable() ? receive(monitor) : proceed();
    }

    /**
     *  Perform a receive operation
     *  @param  monitor         object to check the existance of the connection object
     *  @return TcpState
     */
    TcpState *receive(const Monitor &monitor)
    {
        // we are going to check for errors after the openssl operations, so we make 
        // sure that the error queue is currently completely empty
        OpenSSL::ERR_clear_error();

        // start a loop
        do
        {
            // assume default state
            _state = state_idle;

            // read data from ssl into the buffer
            auto result = _in.receivefrom(_ssl, _parent->expected());
            
            // if this is a failure, we are going to repeat the operation
            if (result <= 0) return repeat(monitor, state_receiving, OpenSSL::SSL_get_error(_ssl, result));

            // go process the received data
            auto *nextstate = parse(monitor, result);
            
            // leap out if we moved to a different state
            if (nextstate != this) return nextstate;
        }
        while (OpenSSL::SSL_pending(_ssl) > 0);
        
        // proceed with the write operation or the event loop
        return _out && isWritable() ? write(monitor) : proceed();
    }

    
public:
    /**
     *  Constructor
     *  @param  state       The previous state
     *  @param  ssl         The SSL structure
     *  @param  buffer      The buffer that was already built
     */
    SslConnected(TcpExtState *state, SslWrapper &&ssl, TcpOutBuffer &&buffer) : 
        TcpExtState(state),
        _ssl(std::move(ssl)),
        _out(std::move(buffer)),
        _in(4096),
        _state(_out ? state_sending : state_idle)
    {
        // tell the handler to monitor the socket if there is an out
        _parent->onIdle(this, _socket, _state == state_sending ? readable | writable : readable); 
    }
    
    /**
     *  Destructor
     */
    virtual ~SslConnected() noexcept = default;
    
    /**
     *  Number of bytes in the outgoing buffer
     *  @return std::size_t
     */
    virtual std::size_t queued() const override { return _out.size(); }

    /**
     *  Process the filedescriptor in the object
     *  @param  monitor     Object that can be used to find out if connection object is still alive
     *  @param  fd          The filedescriptor that is active
     *  @param  flags       AMQP::readable and/or AMQP::writable
     *  @return             New implementation object
     */
    virtual TcpState *process(const Monitor &monitor, int fd, int flags) override
    {
        // the socket must be the one this connection writes to
        if (fd != _socket) return this;
        
        // if we were busy with a write operation, we have to repeat that
        if (_state == state_sending) return write(monitor);
        
        // same is true for read operations, they should also be repeated
        if (_state == state_receiving) return receive(monitor);

        // if we are in an error state, we close the tcp connection
        if (_state == state_error) return new TcpClosed(this);
        
        // if the socket is readable, we are going to receive data
        if (flags & readable) return receive(monitor);
        
        // socket is not readable (so it must be writable), do we have data to write?
        if (_out) return write(monitor);
        
        // the only scenario in which we can end up here is the socket should be
        // closed, but instead of moving to the shutdown-state right, we call proceed()
        // because that function is a little more careful
        return proceed();
    }

    /**
     *  Send data over the connection
     *  @param  buffer      buffer to send
     *  @param  size        size of the buffer
     */
    virtual void send(const char *buffer, size_t size) override
    {
        // do nothing if already busy closing
        if (_closed) return;
        
        // if we're not idle, we can just add bytes to the buffer and we're done
        if (_state != state_idle) return _out.add(buffer, size);

        // clear ssl-level error
        OpenSSL::ERR_clear_error();

        // get the result
        int result = OpenSSL::SSL_write(_ssl, buffer, size);  

        // if the result is larger than zero, we are successful
        if (result > 0) return; 
            
        // check for error
        auto error = OpenSSL::SSL_get_error(_ssl, result);

        // put the data in the outgoing buffer
        _out.add(buffer, size);

        // the operation failed, we may have to repeat our call. this may detect that
        // ssl is in an error state, however that is ok because it will set an internal 
        // state to the error state so that on the next calls to state-changing objects, 
        // the tcp socket will be torn down
        if (repeat(state_sending, error)) return;

        // the repeat call failed, so we are going to find out with a readable file descriptor
        _parent->onIdle(this, _socket, readable);
    }

    /**
     *  Gracefully close the connection
     */
    virtual void close() override 
    { 
        // remember that the object is going to be closed
        _closed = true;
        
        // if the previous operation is still in progress we can wait for that
        if (_state != state_idle) return;
        
        // let's wait until the socket becomes writable (because then we can start the shutdown)
        _parent->onIdle(this, _socket, readable | writable);
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
