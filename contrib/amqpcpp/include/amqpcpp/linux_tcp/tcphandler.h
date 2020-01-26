/**
 *  TcpHandler.h
 *
 *  Interface to be implemented by the caller of the AMQP library in case
 *  the "Tcp" class is being used as alternative for the ConnectionHandler
 *  class.
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
class TcpConnection;

/**
 *  Class definition
 */
class TcpHandler
{
public:
    /**
    *  Destructor
    */
    virtual ~TcpHandler() = default;

    /**
     *  Method that is called immediately after a connection has been constructed. 
     *  @param  connection      The connection object that was just constructed
     */
    virtual void onAttached(TcpConnection *connection)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }
    
    /**
     *  Method that is called when the TCP connection ends up in a connected state
     *  This method is called after the TCP connection has been set up, but before
     *  the (optional) secure TLS connection is ready, and before the AMQP login 
     *  handshake has been completed. If this step has been set, the onLost()
     *  method will also always be called when the connection is closed.
     *  @param  connection  The TCP connection
     */
    virtual void onConnected(TcpConnection *connection) 
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }

    /**
     *  Method that is called after a TCP connection has been set up and the initial 
     *  TLS handshake is finished too, but right before the AMQP login handshake is
     *  going to take place and the first data is going to be sent over the connection. 
     *  This method allows you to inspect the TLS certificate and other connection 
     *  properties, and to break up the connection if you find it not secure enough. 
     *  The default implementation considers all connections to be secure, even if the 
     *  connection has a self-signed or even invalid certificate. To be more strict, 
     *  override this method, inspect the certificate and return false if you do not 
     *  want to use the connection. The passed in SSL pointer is a pointer to a SSL 
     *  structure from the openssl library. This method is only called for secure 
     *  connections (connection with an amqps:// address).
     *  @param  connection      The connection for which TLS was just started
     *  @param  ssl             Pointer to the SSL structure that can be inspected
     *  @return bool            True to proceed / accept the connection, false to break up
     */
    virtual bool onSecured(TcpConnection *connection, const SSL *ssl)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
        (void) ssl;

        // default implementation: do not inspect anything, just allow the connection
        return true;
    }

    /**
     *  Method that is called when the RabbitMQ server and your client application  
     *  exchange some properties that describe their identity.
     *  @param  connection      The connection about which information is exchanged
     *  @param  server          Properties sent by the server
     *  @param  client          Properties that are to be sent back
     */
    virtual void onProperties(TcpConnection *connection, const Table &server, Table &client)
    {
        // make sure compilers dont complaint about unused parameters
        (void) connection;
        (void) server;
        (void) client;
    }

    /**
     *  Method that is called when the heartbeat frequency is negotiated
     *  between the server and the client. Applications can override this method
     *  if they want to use a different heartbeat interval (for example: return 0
     *  to disable heartbeats)
     *  @param  connection      The connection that suggested a heartbeat interval
     *  @param  interval        The suggested interval from the server
     *  @return uint16_t        The interval to use
     *
     *  @see ConnectionHandler::onNegotiate
     */
    virtual uint16_t onNegotiate(TcpConnection *connection, uint16_t interval)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
        (void) interval;

        // default implementation, suggested heartbeat is ok
        return interval;
    }

    /**
     *  Method that is called after the AMQP login handshake has been completed
     *  and the connection object is ready for sending out actual AMQP instructions
     *  @param  connection  The TCP connection
     */
    virtual void onReady(TcpConnection *connection) 
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }

    /**
     *  Method that is called when the server sends a heartbeat to the client
     *  @param  connection      The connection over which the heartbeat was received
     *  @see    ConnectionHandler::onHeartbeat
     */
    virtual void onHeartbeat(TcpConnection *connection) 
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }
    
    /**
     *  Method that is called when the connection ends up in an error state
     *  This could either be an error at the AMQP level, but could also
     *  be an error at the TCP of SSL level (like a broken connection).
     *  If the connection is connected (the onConnected() method was called
     *  before), the onLost() method is going to be called too.
     *  @param  connection  The TCP connection
     *  @param  message     Error message
     */
    virtual void onError(TcpConnection *connection, const char *message) 
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
        (void) message;
    }

    /**
     *  Method that is called when the AMQP protocol was gracefully ended. 
     *  This is the counter-part of a call to connection.close(). Note that
     *  the underlying TCP connection is still alive, and onLost() and
     *  onDetached() (see below) are going to be called too.
     *  @param  connection  The TCP connection
     */
    virtual void onClosed(TcpConnection *connection) 
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }
    
    /**
     *  Method that is called when the TCP connection is lost or closed. This
     *  is always called if you have also received a call to onConnected().
     *  @param  connection  The TCP connection
     */
    virtual void onLost(TcpConnection *connection) 
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }

    /**
     *  Method that is called when the handler will no longer be notified.
     *  This is the last call to your handler, and it is typically used
     *  to clean up stuff.
     *  @param  connection      The connection that is being destructed
     */
    virtual void onDetached(TcpConnection *connection)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }

    /**
     *  Monitor a filedescriptor for readability or writability
     * 
     *  When a TCP connection is opened, it creates a non-blocking socket 
     *  connection. This method is called to inform you about this socket,
     *  so that you can include it in the event loop. When the socket becomes
     *  active, you should call the "process()" method in the Tcp class.
     * 
     *  The flags is AMQP::readable if the filedescriptor should be monitored
     *  for readability, AMQP::writable if it is to be monitored for writability,
     *  or AMQP::readable | AMQP::writable if it has to be checked for both.
     *  If flags has value 0, the filedescriptor should be removed from the
     *  event loop.
     * 
     *  @param  connection  The TCP connection object that is reporting
     *  @param  fd          The filedescriptor to be monitored
     *  @param  flags       Should the object be monitored for readability or writability?
     */
    virtual void monitor(TcpConnection *connection, int fd, int flags) = 0;
};
    
/**
 *  End of namespace
 */
}

