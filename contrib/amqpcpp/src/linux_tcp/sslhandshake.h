/**
 *  SslHandshake.h
 *
 *  Implementation of the TCP state that is responsible for setting
 *  up the STARTTLS handshake.
 *
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "tcpoutbuffer.h"
#include "sslconnected.h"
#include "poll.h"
#include "sslwrapper.h"
#include "sslcontext.h"

/**
 *  Set up namespace
 */
namespace AMQP { 

/**
 *  Class definition
 */
class SslHandshake : public TcpExtState
{
private:
    /**
     *  SSL structure
     *  @var SslWrapper
     */
    SslWrapper _ssl;

    /**
     *  The outgoing buffer
     *  @var TcpOutBuffer
     */
    TcpOutBuffer _out;
    
    
    /**
     *  Report a new state
     *  @param  monitor
     *  @return TcpState
     */
    TcpState *nextstate(const Monitor &monitor)
    {
        // check if the handler allows the connection
        bool allowed = _parent->onSecured(this, _ssl);
        
        // leap out if the user space function destructed the object
        if (!monitor.valid()) return nullptr;

        // if connection is allowed, we move to the next state
        if (allowed) return new SslConnected(this, std::move(_ssl), std::move(_out));
        
        // report that the connection is broken
        _parent->onError(this, "TLS connection has been rejected");
        
        // the onError method could have destructed this object
        if (!monitor.valid()) return nullptr;
        
        // shutdown the connection
        return new SslShutdown(this, std::move(_ssl));
    }
    
    /**
     *  Helper method to report an error
     *  @param  monitor
     *  @return TcpState*
     */
    TcpState *reportError(const Monitor &monitor)
    {
        // we have an error - report this to the user
        _parent->onError(this, "failed to setup ssl connection");
        
        // stop if connection is gone
        if (!monitor.valid()) return nullptr;

        // done, shutdown the tcp connection
        return new TcpClosed(this);
    }
    
    /**
     *  Proceed with the handshake
     *  @param  events      the events to wait for on the socket
     *  @return TcpState
     */
    TcpState *proceed(int events)
    {
        // tell the handler that we want to listen for certain events
        _parent->onIdle(this, _socket, events);
        
        // allow chaining
        return this;
    }
    
public:
    /**
     *  Constructor
     *  @param  state       Earlier state
     *  @param  hostname    The hostname to connect to
     *  @param  context     SSL context
     *  @param  buffer      The buffer that was already built
     *  @throws std::runtime_error
     */
    SslHandshake(TcpExtState *state, const std::string &hostname, TcpOutBuffer &&buffer) : 
        TcpExtState(state),
        _ssl(SslContext(OpenSSL::TLS_client_method())),
        _out(std::move(buffer))
    {
        // we will be using the ssl context as a client
        OpenSSL::SSL_set_connect_state(_ssl);
        
        // associate domain name with the connection
        OpenSSL::SSL_ctrl(_ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, (void *)hostname.data());
        
        // associate the ssl context with the socket filedescriptor
        if (OpenSSL::SSL_set_fd(_ssl, _socket) == 0) throw std::runtime_error("failed to associate filedescriptor with ssl socket");
        
        // we are going to wait until the socket becomes writable before we start the handshake
        _parent->onIdle(this, _socket, writable);
    }
    
    /**
     *  Destructor
     */
    virtual ~SslHandshake() noexcept = default;

    /**
     *  Number of bytes in the outgoing buffer
     *  @return std::size_t
     */
    virtual std::size_t queued() const override { return _out.size(); }
    
    /**
     *  Process the filedescriptor in the object
     *  @param  monitor     Object to check if connection still exists
     *  @param  fd          Filedescriptor that is active
     *  @param  flags       AMQP::readable and/or AMQP::writable
     *  @return             New state object
     */
    virtual TcpState *process(const Monitor &monitor, int fd, int flags) override
    {
        // must be the socket
        if (fd != _socket) return this;

        // we are going to check for errors after the openssl operations, so we make 
        // sure that the error queue is currently completely empty
        OpenSSL::ERR_clear_error();

        // start the ssl handshake
        int result = OpenSSL::SSL_do_handshake(_ssl);
        
        // if the connection succeeds, we can move to the ssl-connected state
        if (result == 1) return nextstate(monitor);
        
        // error was returned, so we must investigate what is going on
        auto error = OpenSSL::SSL_get_error(_ssl, result);
        
        // check the error
        switch (error) {
        case SSL_ERROR_WANT_READ:   return proceed(readable);
        case SSL_ERROR_WANT_WRITE:  return proceed(readable | writable);
        default:                    return reportError(monitor);
        }
    }

    /**
     *  Send data over the connection
     *  @param  buffer      buffer to send
     *  @param  size        size of the buffer
     */
    virtual void send(const char *buffer, size_t size) override
    {
        // the handshake is still busy, outgoing data must be cached
        _out.add(buffer, size); 
    }
};
    
/**
 *  End of namespace
 */
}

