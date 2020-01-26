/**
 *  TcpParent.h
 *
 *  Interface to be implemented by the parent of a tcp-state. This is
 *  an _internal_ interface that is not relevant for user-space applications.
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <openssl/ssl.h>

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class TcpState;
class Buffer;

/**
 *  Class definition
 */
class TcpParent
{
public:
    /**
     *  Destructor
     */
    virtual ~TcpParent() = default;


    /**
     *  Method that is called when the TCP connection has been established
     *  @param  state
     */
    virtual void onConnected(TcpState *state) = 0;

    /**
     *  Method that is called when the connection is secured
     *  @param  state
     *  @param  ssl
     *  @return bool
     */
    virtual bool onSecured(TcpState *state, const SSL *ssl) = 0;

    /**
     *  Method to be called when data was received
     *  @param  state
     *  @param  buffer
     *  @return size_t
     */
    virtual size_t onReceived(TcpState *state, const Buffer &buffer) = 0;
    
    /**
     *  Method to be called when we need to monitor a different filedescriptor
     *  @param  state
     *  @param  fd
     *  @param  events
     */
    virtual void onIdle(TcpState *state, int socket, int events) = 0;

    /**
     *  Method that is called when an error occurs (the connection is lost)
     *  @param  state
     *  @param  error
     *  @param  connected
     */
    virtual void onError(TcpState *state, const char *message, bool connected = true) = 0;

    /**
     *  Method to be called when it is detected that the connection was lost
     *  @param  state
     */
    virtual void onLost(TcpState *state) = 0;
    
    /**
     *  The expected number of bytes
     *  @return size_t
     */
    virtual size_t expected() = 0;
};

/**
 *  End of namespace
 */
}
