/**
 *  ConnectionHandler.h
 *
 *  Interface that should be implemented by the caller of the library and
 *  that is passed to the AMQP connection. This interface contains all sorts
 *  of methods that are called when data needs to be sent, or when the
 *  AMQP connection ends up in a broken state.
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
#include <cstdint>
#include <stddef.h>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class Connection;

/**
 *  Class definition
 */
class ConnectionHandler
{
public:
    /**
    *  Destructor
    */
    virtual ~ConnectionHandler() = default;
    
    /**
     *  When the connection is being set up, the client and server exchange
     *  some information. This includes for example their name and version, 
     *  copyright statement and the operating system name. Nothing in this 
     *  exchange of information is very relevant for the actual AMQP protocol, 
     *  but by overriding this method you can read out the information that 
     *  was sent by the server, and you can decide which information you 
     *  want to send back that describe the client. In RabbitMQ's management 
     *  console the client-properties are visible on the "connections" tab, 
     *  which could be helpful in certain scenarios, like debugging.
     * 
     *  The read-only "server" parameter contains the information sent by 
     *  the server, while the "client" table may be filled with information
     *  about your application. The AMQP protocol says that this table should
     *  at least be filled with data for the "product", "version", "platform", 
     *  "copyright" and "information" keys. However, you do not have to 
     *  override this method, and even when you do, you do not have to ensure 
     *  that these properties are indeed set, because the AMQP-CPP library 
     *  takes care of filling in properties that were not explicitly set.
     * 
     *  @param  connection      The connection about which information is exchanged
     *  @param  server          Properties sent by the server
     *  @param  client          Properties that are to be sent back
     */
    virtual void onProperties(Connection *connection, const Table &server, Table &client)
    {
        // make sure compilers dont complaint about unused parameters
        (void) connection;
        (void) server;
        (void) client;
    }
    
    /**
     *  Method that is called when the heartbeat frequency is negotiated
     *  between the server and the client durion connection setup. You 
     *  normally do not have to override this method, because in the default 
     *  implementation the suggested heartbeat is simply rejected by the client.
     *
     *  However, if you want to enable heartbeats you can override this 
     *  method. You should "return interval" if you want to accept the
     *  heartbeat interval that was suggested by the server, or you can
     *  return an alternative value if you want a shorter or longer interval.
     *  Return 0 if you want to disable heartbeats.
     * 
     *  If heartbeats are enabled, you yourself are responsible to send
     *  out a heartbeat every *interval / 2* number of seconds by calling
     *  the Connection::heartbeat() method.
     *
     *  @param  connection      The connection that suggested a heartbeat interval
     *  @param  interval        The suggested interval from the server
     *  @return uint16_t        The interval to use
     */
    virtual uint16_t onNegotiate(Connection *connection, uint16_t interval)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
        (void) interval;

        // default implementation, disable heartbeats
        return 0;
    }

    /**
     *  Method that is called by AMQP-CPP when data has to be sent over the 
     *  network. You must implement this method and send the data over a
     *  socket that is connected with RabbitMQ.
     *
     *  Note that the AMQP library does no buffering by itself. This means
     *  that this method should always send out all data or do the buffering
     *  itself.
     *
     *  @param  connection      The connection that created this output
     *  @param  buffer          Data to send
     *  @param  size            Size of the buffer
     */
    virtual void onData(Connection *connection, const char *buffer, size_t size) = 0;

    /**
     *  Method that is called when the AMQP-CPP library received a heartbeat 
     *  frame that was sent by the server to the client.
     *
     *  You do not have to do anything here, the client sends back a heartbeat
     *  frame automatically, but if you like, you can implement/override this
     *  method if you want to be notified of such heartbeats
     *
     *  @param  connection      The connection over which the heartbeat was received
     */
    virtual void onHeartbeat(Connection *connection) 
    { 
        // make sure compilers dont complain about unused parameters
        (void) connection;
    }

    /**
     *  When the connection ends up in an error state this method is called.
     *  This happens when data comes in that does not match the AMQP protocol,
     *  or when an error message was sent by the server to the client.
     *
     *  After this method is called, the connection no longer is in a valid
     *  state and can no longer be used.
     *
     *  This method has an empty default implementation, although you are very
     *  much advised to implement it. When an error occurs, the connection
     *  is no longer usable, so you probably want to know.
     *
     *  @param  connection      The connection that entered the error state
     *  @param  message         Error message
     */
    virtual void onError(Connection *connection, const char *message)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;
        (void) message;
    }

    /**
     *  Method that is called when the login attempt succeeded. After this method
     *  is called, the connection is ready to use, and the RabbitMQ server is
     *  ready to receive instructions.
     * 
     *  According to the AMQP protocol, you must wait for the connection to become
     *  ready (and this onConnected method to be called) before you can start
     *  sending instructions to RabbitMQ. However, if you prematurely do send 
     *  instructions, this AMQP-CPP library caches all methods that you call 
     *  before the connection is ready and flushes them the moment the connection
     *  has been set up, so technically there is no real reason to wait for this 
     *  method to be called before you send the first instructions.
     *
     *  @param  connection      The connection that can now be used
     */
    virtual void onReady(Connection *connection) 
    { 
        // make sure compilers dont complain about unused parameters
        (void) connection; 
    }

    /**
     *  Method that is called when the AMQP connection was closed.
     *
     *  This is the counter part of a call to Connection::close() and it confirms
     *  that the connection was _correctly_ closed. Note that this only applies
     *  to the AMQP connection, the underlying TCP connection is not managed by
     *  AMQP-CPP and is still active.
     *
     *  @param  connection      The connection that was closed and that is now unusable
     */
    virtual void onClosed(Connection *connection) 
    { 
        // make sure compilers dont complain about unused parameters
        (void) connection; 
    }
};

/**
 *  End of namespace
 */
}

