/**
 *  SslShutdown.h
 *
 *  Class that takes care of the final handshake to close a SSL connection
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class SslShutdown : public TcpExtState
{
private:
    /**
     *  The SSL context
     *  @var SslWrapper
     */
    SslWrapper _ssl;
    

    /**
     *  Proceed with the next operation after the previous operation was
     *  a success, possibly changing the filedescriptor-monitor
     *  @param  monitor         object to check if connection still exists
     *  @return TcpState*
     */
    virtual TcpState *proceed(const Monitor &monitor)
    {
        // next state is to close the connection
        return new TcpClosed(this);
    }
        
    /**
     *  Method to repeat the previous call
     *  @param  monitor     object to check if connection still exists
     *  @param  result      result of an earlier openssl operation
     *  @return TcpState*
     */
    TcpState *repeat(const Monitor &monitor, int result)
    {
        // error was returned, so we must investigate what is going on
        auto error = OpenSSL::SSL_get_error(_ssl, result);

        // check the error
        switch (error) {
        case SSL_ERROR_WANT_READ:
            // the operation must be repeated when readable
            _parent->onIdle(this, _socket, readable);
            return this;
        
        case SSL_ERROR_WANT_WRITE:
            // wait until socket becomes writable again
            _parent->onIdle(this, _socket, readable | writable);
            return this;
            
        default:
            // go to the final state (if not yet disconnected)
            return proceed(monitor);
        }
    }
    

public:
    /**
     *  Constructor
     *  @param  state       Previous state
     *  @param  ssl         The SSL structure
     */
    SslShutdown(TcpExtState *state, SslWrapper &&ssl) : 
        TcpExtState(state),
        _ssl(std::move(ssl))
    {
        // wait until the socket is accessible
        _parent->onIdle(this, _socket, readable | writable); 
    }
    
    /**
     *  No copying
     *  @param  that
     */
    SslShutdown(const SslShutdown &that) = delete;
    
    /**
     * Destructor
     */
    virtual ~SslShutdown() noexcept = default;
    
    /**
     *  Process the filedescriptor in the object    
     *  @param  monitor     Object to check if connection still exists
     *  @param  fd          The filedescriptor that is active
     *  @param  flags       AMQP::readable and/or AMQP::writable
     *  @return             New implementation object
     */
    virtual TcpState *process(const Monitor &monitor, int fd, int flags) override
    {
        // the socket must be the one this connection writes to
        if (fd != _socket) return this;

        // we are going to check for errors after the openssl operations, so we make 
        // sure that the error queue is currently completely empty
        OpenSSL::ERR_clear_error();
        
        // close the connection
        auto result = OpenSSL::SSL_shutdown(_ssl);
        
        // on result==0 we need an additional call
        while (result == 0) result = OpenSSL::SSL_shutdown(_ssl);
        
        // if this is a success, we can proceed with the event loop
        if (result > 0) return proceed(monitor);
            
        // the operation failed, we may have to repeat our call
        else return repeat(monitor, result);
    }
};

/**
 *  End of namespace
 */
}
