AMQP-CPP
========

[![Build Status](https://travis-ci.org/CopernicaMarketingSoftware/AMQP-CPP.svg?branch=master)](https://travis-ci.org/CopernicaMarketingSoftware/AMQP-CPP)
[![Build status](https://ci.appveyor.com/api/projects/status/7xoc4y0flm0045yy/branch/master?svg=true)](https://ci.appveyor.com/project/mvdwerve/amqp-cpp/branch/master)

**Are you upgrading from AMQP-CPP 3 to AMQP-CPP 4?** [Please read the upgrade instructions](#upgrading)

AMQP-CPP is a C++ library for communicating with a RabbitMQ message broker. The
library can be used to parse incoming data from a RabbitMQ server, and to
generate frames that can be sent to a RabbitMQ server.

This library has a layered architecture, and allows you - if you like - to
completely take care of the network layer. If you want to set up and manage the
network connections yourself, the AMQP-CPP library will not make a connection to
RabbitMQ by itself, nor will it create sockets and/or perform IO operations. As
a user of this library, you create the socket connection and implement a certain
interface that you pass to the AMQP-CPP library and that the library will use
for IO operations.

Intercepting this network layer is however optional, the AMQP-CPP library also
comes with a predefined TCP and TLS module that can be used if you trust the AMQP 
library to take care of the network (and optional TLS) handling. In that case, the 
AMQP-CPP library does all the system and library calls to set up network connections 
and send and receive the (possibly encrypted) data.

This layered architecture makes the library extremely flexible and portable: it
does not necessarily rely on operating system specific IO calls, and can be
easily integrated into any kind of event loop. If you want to implement the AMQP
protocol on top of some [unusual other communication layer](https://tools.ietf.org/html/rfc1149),
this library can be used for that - but if you want to use it with regular TCP
connections, setting it up is just as easy.

AMQP-CPP is fully asynchronous and does not do any blocking (system) calls, so
it can be used in high performance applications without the need for threads.

The AMQP-CPP library uses C++11 features, so if you intend use it, please make
sure that your compiler is up-to-date and supports C++11.

**Note for the reader:** This readme file has a peculiar structure. We start
explaining the pure and hard core low level interface in which you have to
take care of opening socket connections yourself. In reality, you probably want
to use the simpler TCP interface that is being described [later on](#tcp-connections).


ABOUT
=====

This library is created and maintained by Copernica (www.copernica.com), and is
used inside the MailerQ (www.mailerq.com) and Yothalot (www.yothalot.com) applications. 
MailerQ is a tool for sending large volumes of email, using AMQP message queues, and Yothalot 
is a big data processing map/reduce framework.

Do you appreciate our work and are you looking for high quality email solutions?
Then check out our other commercial and open source solutions:

* Copernica Marketing Suite (www.copernica.com)
* MailerQ on-premise MTA (www.mailerq.com)
* Responsive Email web service (www.responsiveemail.com)
* SMTPeter cloud based SMTP server (www.smtpeter.com)
* PHP-CPP bridge between PHP and C++ (www.php-cpp.com)
* PHP-JS bridge between PHP and Javascript (www.php-js.com)
* Yothalot big data processor (www.yothalot.com)


INSTALLING
==========

AMQP-CPP comes with an optional Linux-only TCP module that takes care of the 
network part required for the AMQP-CPP core library. If you use this module, you 
are required to link with `pthread` and `dl`.

There are two methods to compile AMQP-CPP: CMake and Make. CMake is platform portable 
and works on all systems, while the Makefile only works on Linux. The two methods 
create both a shared and a static version of the AMQP-CPP library. Building of a
shared library is currently not supported on Windows. 

After building there are two relevant files to #include when you use the library.

 File                | Include when?
---------------------|--------------------------------------------------------
 amqpcpp.h           | Always needed for the core features
 amqpcpp/linux_tcp.h | If using the Linux-only TCP module

On Windows you are required to define `NOMINMAX` when compiling code that includes public AMQP-CPP header files.

## Using cmake

The CMake file supports both building and installing. You can choose not to use 
the install functionality, and instead manually use the build output at `build/bin/`. Keep
in mind that the TCP module is only supported for Linux. An example install method 
would be:

```bash
mkdir build
cd build
cmake .. [-DAMQP-CPP_BUILD_SHARED] [-DAMQP-CPP_LINUX_TCP]
cmake --build .. --target install
```

 Option                  | Default | Meaning
-------------------------|---------|-----------------------------------------------------------------------
 AMQP-CPP_BUILD_SHARED   | OFF     | Static lib(ON) or shared lib(OFF)? Shared is not supported on Windows.
 AMQP-CPP_LINUX_TCP      | OFF     | Should the Linux-only TCP module be built?

## Using make

Compiling and installing AMQP-CPP with make is as easy as running `make` and 
then `make install`. This will install the full version of AMQP-CPP, including 
the system specific TCP module. If you do not need the additional TCP module 
(because you take care of handling the network stuff yourself), you can also 
compile a pure form of the library. Use `make pure` and `make install` for that.

When you compile an application that uses the AMQP-CPP library, do not
forget to link with the library. For gcc and clang the linker flag is -lamqpcpp.
If you use the fullblown version of AMQP-CPP (with the TCP module), you also
need to pass the -lpthread and -ldl linker flags, because the TCP module uses a 
thread for running an asynchronous and non-blocking DNS hostname lookup, and it
must be linked with the "dl" library to allow dynamic lookups for functions from
the openssl library if a secure connection to RabbitMQ has to be set up.


HOW TO USE AMQP-CPP
===================

As we mentioned above, the library can be used in a network-agnostic fashion.
It then does not do any IO by itself, and you need to pass an object to the
library that the library can use for IO. So, before you start using the
library, you first need to create a class that extends from the
ConnectionHandler base class. This is a class with a number of methods that are
called by the library every time it wants to send out data, or when it needs to
inform you that an error occured.

````c++
#include <amqpcpp.h>

class MyConnectionHandler : public AMQP::ConnectionHandler
{
    /**
     *  Method that is called by the AMQP library every time it has data
     *  available that should be sent to RabbitMQ.
     *  @param  connection  pointer to the main connection object
     *  @param  data        memory buffer with the data that should be sent to RabbitMQ
     *  @param  size        size of the buffer
     */
    virtual void onData(AMQP::Connection *connection, const char *data, size_t size)
    {
        // @todo
        //  Add your own implementation, for example by doing a call to the
        //  send() system call. But be aware that the send() call may not
        //  send all data at once, so you also need to take care of buffering
        //  the bytes that could not immediately be sent, and try to send
        //  them again when the socket becomes writable again
    }

    /**
     *  Method that is called by the AMQP library when the login attempt
     *  succeeded. After this method has been called, the connection is ready
     *  to use.
     *  @param  connection      The connection that can now be used
     */
    virtual void onReady(AMQP::Connection *connection)
    {
        // @todo
        //  add your own implementation, for example by creating a channel
        //  instance, and start publishing or consuming
    }

    /**
     *  Method that is called by the AMQP library when a fatal error occurs
     *  on the connection, for example because data received from RabbitMQ
     *  could not be recognized.
     *  @param  connection      The connection on which the error occured
     *  @param  message         A human readable error message
     */
    virtual void onError(AMQP::Connection *connection, const char *message)
    {
        // @todo
        //  add your own implementation, for example by reporting the error
        //  to the user of your program, log the error, and destruct the
        //  connection object because it is no longer in a usable state
    }

    /**
     *  Method that is called when the connection was closed. This is the
     *  counter part of a call to Connection::close() and it confirms that the
     *  AMQP connection was correctly closed.
     *
     *  @param  connection      The connection that was closed and that is now unusable
     */
    virtual void onClosed(AMQP::Connection *connection) 
    {
        // @todo
        //  add your own implementation, for example by closing down the
        //  underlying TCP connection too
    }


};
````
After you've implemented the ConnectionHandler class (which is entirely up to
you), you can start using the library by creating a Connection object, and one 
or more Channel objects:

````c++
// create an instance of your own connection handler
MyConnectionHandler myHandler;

// create a AMQP connection object
AMQP::Connection connection(&myHandler, AMQP::Login("guest","guest"), "/");

// and create a channel
AMQP::Channel channel(&connection);

// use the channel object to call the AMQP method you like
channel.declareExchange("my-exchange", AMQP::fanout);
channel.declareQueue("my-queue");
channel.bindQueue("my-exchange", "my-queue", "my-routing-key");
````

A number of remarks about the example above. First you may have noticed that we've
created all objects on the stack. You are of course also free to create them
on the heap with the C++ operator 'new'. That works just as well, and is in real
life code probably more useful as you normally want to keep your handlers, connection
and channel objects around for a longer time.

But more importantly, you can see in the example above that we instantiated the
channel object directly after we made the connection object, and we also
started declaring exchanges and queues right away. However, under the hood, a handshake
protocol is executed between the server and the client when the Connection
object is first created. During this handshake procedure it is not permitted to send
other instructions (like opening a channel or declaring a queue). It would therefore have been better
if we had first waited for the connection to be ready (implement the MyConnectionHandler::onReady() method),
and create the channel object only then. But this is not strictly necessary.
The methods that are called before the handshake is completed are cached by the
AMQP library and will be executed the moment the handshake is completed and the
connection becomes ready for use.


PARSING INCOMING DATA
=====================

The ConnectionHandler class has a method onData() that is called by the library
every time that it wants to send out data. We've explained that it is up to you to
implement that method. Inside your ConnectionHandler::onData() method, you can for
example call the "send()" or "write()" system call to send out the data to
the RabbitMQ server. But what about data in the other direction? How does the
library receive data back from RabbitMQ?

In this raw setup, the AMQP-CPP library does not do any IO by itself and it is
therefore also not possible for the library to receive data from a
socket. It is again up to you to do this. If, for example, you notice in your
event loop that the socket that is connected with the RabbitMQ server becomes
readable, you should read out that socket (for example by using the recv() system
call), and pass the received bytes to the AMQP-CPP library. This is done by
calling the parse() method in the Connection object.

The Connection::parse() method gets two parameters, a pointer to a buffer of
data that you just read from the socket, and a parameter that holds the size of
this buffer. The code snippet below comes from the Connection.h C++ header file.

````c++
/**
 *  Parse data that was received from RabbitMQ
 *
 *  Every time that data comes in from RabbitMQ, you should call this method to parse
 *  the incoming data, and let it handle by the AMQP-CPP library. This method returns
 *  the number of bytes that were processed.
 *
 *  If not all bytes could be processed because it only contained a partial frame,
 *  you should call this same method later on when more data is available. The
 *  AMQP-CPP library does not do any buffering, so it is up to the caller to ensure
 *  that the old data is also passed in that later call.
 *
 *  @param  buffer      buffer to decode
 *  @param  size        size of the buffer to decode
 *  @return             number of bytes that were processed
 */
size_t parse(char *buffer, size_t size)
{
    return _implementation.parse(buffer, size);
}
````

You should do all the book keeping for the buffer yourselves. If you for example
call the Connection::parse() method with a buffer of 100 bytes, and the method
returns that only 60 bytes were processed, you should later call the method again,
with a buffer filled with the remaining 40 bytes. If the method returns 0, you should
make a new call to parse() when more data is available, with a buffer that contains
both the old data, and the new data.

To optimize your calls to the parse() method, you _could_ use the Connection::expected()
and Connection::maxFrame() methods. The expected() method returns the number of bytes
that the library prefers to receive next. It is pointless to call the parse() method
with a smaller buffer, and it is best to call the method with a buffer of exactly this
size. The maxFrame() returns the max frame size for AMQP messages. If you read your
messages into a reusable buffer, you could allocate this buffer up to this size, so that
you never will have to reallocate.


TCP CONNECTIONS
===============

Although the AMQP-CPP library gives you extreme flexibility by letting you setup
your own network connections, the reality is that most if not all AMQP connections
use the TCP protocol. To help you out, the library therefore also comes with a
TCP module that takes care of setting up the network connections, and sending
and receiving the data.

If you want to use this TCP module, you should not use the AMQP::Connection
and AMQP::Channel classes that you saw above, but the alternative AMQP::TcpConnection
and AMQP::TcpChannel classes instead. You also do not have to create your own class
that implements the "AMQP::ConnectionHandler" interface - but a class that inherits
from "AMQP::TcpHandler" instead. This AMQP::TcpHandler class contains a set of
methods that you can override to intercept all sort of events that occur during the
TCP and AMQP connection lifetime. Overriding these methods is mostly optional, because
almost all have a default implementation. But you do need to implement the 
"monitor()" method, as that is needed by the AMQP-CPP library to interact with
the main event loop:

````c++
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>

class MyTcpHandler : public AMQP::TcpHandler
{
    /**
     *  Method that is called by the AMQP library when a new connection
     *  is associated with the handler. This is the first call to your handler
     *  @param  connection      The connection that is attached to the handler
     */
    virtual void onAttached(AMQP::TcpConnection *connection) override
    {
        // @todo
        //  add your own implementation, for example initialize things
        //  to handle the connection.
    }

    /**
     *  Method that is called by the AMQP library when the TCP connection 
     *  has been established. After this method has been called, the library
     *  still has take care of setting up the optional TLS layer and of
     *  setting up the AMQP connection on top of the TCP layer., This method 
     *  is always paired with a later call to onLost().
     *  @param  connection      The connection that can now be used
     */
    virtual void onConnected(AMQP::TcpConnection *connection) override
    {
        // @todo
        //  add your own implementation (probably not needed)
    }

    /**
     *  Method that is called when the secure TLS connection has been established. 
     *  This is only called for amqps:// connections. It allows you to inspect
     *  whether the connection is secure enough for your liking (you can
     *  for example check the server certicate). The AMQP protocol still has
     *  to be started.
     *  @param  connection      The connection that has been secured
     *  @param  ssl             SSL structure from openssl library
     *  @return bool            True if connection can be used
     */
    virtual bool onSecured(AMQP::TcpConnection *connection, const SSL *ssl) override
    {
        // @todo
        //  add your own implementation, for example by reading out the
        //  certificate and check if it is indeed yours
        return true;
    }

    /**
     *  Method that is called by the AMQP library when the login attempt
     *  succeeded. After this the connection is ready to use.
     *  @param  connection      The connection that can now be used
     */
    virtual void onReady(AMQP::TcpConnection *connection) override
    {
        // @todo
        //  add your own implementation, for example by creating a channel
        //  instance, and start publishing or consuming
    }

    /**
     *  Method that is called by the AMQP library when a fatal error occurs
     *  on the connection, for example because data received from RabbitMQ
     *  could not be recognized, or the underlying connection is lost. This
     *  call is normally followed by a call to onLost() (if the error occured
     *  after the TCP connection was established) and onDetached().
     *  @param  connection      The connection on which the error occured
     *  @param  message         A human readable error message
     */
    virtual void onError(AMQP::TcpConnection *connection, const char *message) override
    {
        // @todo
        //  add your own implementation, for example by reporting the error
        //  to the user of your program and logging the error
    }

    /**
     *  Method that is called when the AMQP protocol is ended. This is the
     *  counter-part of a call to connection.close() to graceful shutdown
     *  the connection. Note that the TCP connection is at this time still 
     *  active, and you will also receive calls to onLost() and onDetached()
     *  @param  connection      The connection over which the AMQP protocol ended
     */
    virtual void onClosed(AMQP::TcpConnection *connection) override 
    {
        // @todo
        //  add your own implementation (probably not necessary, but it could
        //  be useful if you want to do some something immediately after the
        //  amqp connection is over, but do not want to wait for the tcp 
        //  connection to shut down
    }

    /**
     *  Method that is called when the TCP connection was closed or lost.
     *  This method is always called if there was also a call to onConnected()
     *  @param  connection      The connection that was closed and that is now unusable
     */
    virtual void onLost(AMQP::TcpConnection *connection) override 
    {
        // @todo
        //  add your own implementation (probably not necessary)
    }

    /**
     *  Final method that is called. This signals that no further calls to your
     *  handler will be made about the connection.
     *  @param  connection      The connection that can be destructed
     */
    virtual void onDetached(AMQP::TcpConnection *connection) override 
    {
        // @todo
        //  add your own implementation, like cleanup resources or exit the application
    } 

    /**
     *  Method that is called by the AMQP-CPP library when it wants to interact
     *  with the main event loop. The AMQP-CPP library is completely non-blocking,
     *  and only make "write()" or "read()" system calls when it knows in advance
     *  that these calls will not block. To register a filedescriptor in the
     *  event loop, it calls this "monitor()" method with a filedescriptor and
     *  flags telling whether the filedescriptor should be checked for readability
     *  or writability.
     *
     *  @param  connection      The connection that wants to interact with the event loop
     *  @param  fd              The filedescriptor that should be checked
     *  @param  flags           Bitwise or of AMQP::readable and/or AMQP::writable
     */
    virtual void monitor(AMQP::TcpConnection *connection, int fd, int flags) override
    {
        // @todo
        //  add your own implementation, for example by adding the file
        //  descriptor to the main application event loop (like the select() or
        //  poll() loop). When the event loop reports that the descriptor becomes
        //  readable and/or writable, it is up to you to inform the AMQP-CPP
        //  library that the filedescriptor is active by calling the
        //  connection->process(fd, flags) method.
    }
};
````

You see that there are many methods in TcpHandler that you can implement. The most important 
one is "monitor()". This method is used to integrate the AMQP filedescriptors in your
application's event loop. For some popular event loops (libev, libuv, libevent), we 
have already added example handler objects (see the next section for that). All the 
other methods are optional to override. It often is a good idea to override the
onError() method to log or report errors and onDetached() for cleaning up stuff.
AMQP-CPP has it's own buffers if you send instructions prematurely, but if you
intend to send a lot of data over the connection, it also is a good idea to 
implement the onReady() method and delay your calls until the AMQP connection 
has been fully set up.

Using the TCP module of the AMQP-CPP library is easier than using the
raw AMQP::Connection and AMQP::Channel objects, because you do not have to
create the sockets and connections yourself, and you also do not have to take
care of buffering network data. The example that we gave above, looks slightly 
different if you make use of the TCP module:

````c++
// create an instance of your own tcp handler
MyTcpHandler myHandler;

// address of the server
AMQP::Address address("amqp://guest:guest@localhost/vhost");

// create a AMQP connection object
AMQP::TcpConnection connection(&myHandler, address);

// and create a channel
AMQP::TcpChannel channel(&connection);

// use the channel object to call the AMQP method you like
channel.declareExchange("my-exchange", AMQP::fanout);
channel.declareQueue("my-queue");
channel.bindQueue("my-exchange", "my-queue", "my-routing-key");
````

SECURE CONNECTIONS
==================

The TCP module of AMQP-CPP also supports setting up secure connections. If your
RabbitMQ server accepts SSL connections, you can specify the address to your
server using the amqps:// protocol:

````c++
// init the SSL library (this works for openssl 1.1, for openssl 1.0 use SSL_library_init())
OPENSSL_init_ssl(0, NULL);

// address of the server (secure!)
AMQP::Address address("amqps://guest:guest@localhost/vhost");

// create a AMQP connection object
AMQP::TcpConnection connection(&myHandler, address);
````

There are two things to take care of if you want to create a secure connection: 
(1) you must link your application with the -lssl flag (or use dlopen()), and (2) 
you must initialize the openssl library by calling OPENSSL_init_ssl(). This 
initializating must take place before you let you application connect to RabbitMQ. 
This is necessary because AMQP-CPP needs access to the openssl library to set up 
secure connections. It can only access this library if you have linked your 
application with this library, or if you have loaded this library at runtime 
using dlopen()). 

Linking openssl is the normal thing to do. You just have to add the `-lssl` flag
to your linker. If you however do not want to link your application with openssl, 
you can also load the openssl library at runtime, and pass in the pointer to the 
handle to AMQP-CPP:

````c++
// dynamically open the openssl library
void *handle = dlopen("/path/to/openssl.so", RTLD_LAZY);

// tell AMQP-CPP library where the handle to openssl can be found
AMQP::openssl(handle);

// @todo call functions to initialize openssl, and create the AMQP connection
// (see exampe above)
````

By itself, AMQP-CPP does not check if the created TLS connection is sufficient
secure. Whether the certificate is expired, self-signed, missing or invalid: for
AMQP-CPP it all doesn't matter and the connection is simply permitted. If you
want to be more strict (for example: if you want to verify the server's certificate),
you must do this yourself by implementing the "onSecured()" method in your handler
object:

````c++
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>

class MyTcpHandler : public AMQP::TcpHandler
{
    /**
     *  Method that is called right after the TLS connection has been created.
     *  In this method you can check the connection properties (like the certificate)
     *  and return false if you find it not secure enough
     *  @param  connection      the connection that has just completed the tls handshake
     *  @param  ssl             SSL structure from the openssl library
     *  @return bool            true if connection is secure enough to start the AMQP protocol
     */
    virtual bool onSecured(AMQP::TcpConnection *connection, const SSL *ssl) override
    {
        // @todo call functions from the openssl library to check the certificate,
        // like SSL_get_peer_certificate() or SSL_get_verify_result().
        // For now we always allow the connection to proceed
        return true;
    }
    
    /**
     *  All other methods (like onConnected(), onError(), etc) are left out of this
     *  example, but would be here if this was an actual user space handler class.
     */
};
````

The SSL pointer that is passed to the onSecured() method refers to the "SSL"
structure from the openssl library.


EXISTING EVENT LOOPS
====================

Both the pure AMQP::Connection as well as the easier AMQP::TcpConnection class
allow you to integrate AMQP-CPP in your own event loop. Whether you take care
of running the event loop yourself (for example by using the select() system
call), or if you use an existing library for it (like libevent, libev or libuv),
you can implement the "monitor()" method to watch the file descriptors and
hand over control back to AMQP-CPP when one of the sockets become active.

For libev, libuv and libevent users, we have even implemented an example 
implementation, so that you do not even have to do this. Instead of implementing 
the monitor() method yourself, you can use the AMQP::LibEvHandler, 
AMQP::LibUvHandler or AMQP:LibEventHandler classes instead:

````c++
#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>

int main()
{
    // access to the event loop
    auto *loop = EV_DEFAULT;

    // handler for libev (so we don't have to implement AMQP::TcpHandler!)
    AMQP::LibEvHandler handler(loop);

    // make a connection
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://localhost/"));

    // we need a channel too
    AMQP::TcpChannel channel(&connection);

    // create a temporary queue
    channel.declareQueue(AMQP::exclusive).onSuccess([&connection](const std::string &name, uint32_t messagecount, uint32_t consumercount) {

        // report the name of the temporary queue
        std::cout << "declared queue " << name << std::endl;

        // now we can close the connection
        connection.close();
    });

    // run the loop
    ev_run(loop, 0);

    // done
    return 0;
}
````

The AMQP::LibEvHandler and AMQP::LibEventHandler classes are extended AMQP::TcpHandler
classes, with an implementation of the monitor() method that simply adds the
filedescriptor to the event loop. If you use this class, it is recommended not to
instantiate it directly (like we did in the example), but to create your own
"MyHandler" class that extends from it, and in which you also implement the
onError() method to report possible connection errors to your end users.

Currently, we have example TcpHandler implementations for libev, libuv,
libevent, and Boost's asio. For other event loops we do not yet have
such examples. The quality of the libboostasio is however debatable: it was
not developed and is not maintained by the original AMQP-CPP developers, and 
it has a couple of open issues.

| TCP Handler Impl        | Header File Location   |  Sample File Location     |
| ----------------------- | ---------------------- | ------------------------- |
| Boost asio (io_service) | include/libboostasio.h | examples/libboostasio.cpp |  
| libev                   | include/libev.h        | examples/libev.cpp        |
| libevent                | include/libevent.h     | examples/libevent.cpp     |
| libuv                   | include/libuv.h        | examples/libuv.cpp        |

HEARTBEATS
==========

The AMQP protocol supports *heartbeats*. If this heartbeat feature is enabled, the
client and the server negotiate a heartbeat interval during connection setup, and
they agree to send at least *some kind of data* over the connection during every
iteration of that interval. The normal data that is sent over the connection (like
publishing or consuming messages) is normally sufficient to keep the connection alive,
but if the client or server was idle during the negotiated interval time, a dummy
heartbeat message must be sent instead.

The default behavior of the AMQP-CPP library is to disable heartbeats. The
proposed heartbeat interval of the server during connection setup (the server
normally suggests an interval of 60 seconds) is vetoed by the AMQP-CPP library so
no heartbeats are ever needed to be sent over the connection. This means that you
can safely keep your AMQP connection idle for as long as you like, and/or run long
lasting algorithms after you've consumed a message from RabbitMQ, without having
to worry about the connection being idle for too long.

You can however choose to enable these heartbeats. If you want to enable heartbeats,
you should implement the onNegotiate() method inside your ConnectionHandler or
TcpHandler class and have it return the interval that you find appropriate.

````c++
#include <amqpcpp.h>

class MyTcpHandler : public AMQP::TcpHandler
{
    /**
     *  Method that is called when the server tries to negotiate a heartbeat
     *  interval, and that is overridden to get rid of the default implementation
     *  (which vetoes the suggested heartbeat interval), and accept the interval
     *  instead.
     *  @param  connection      The connection on which the error occured
     *  @param  interval        The suggested interval in seconds
     */
    virtual uint16_t onNegotiate(AMQP::TcpConnection *connection, uint16_t interval)
    {
        // we accept the suggestion from the server, but if the interval is smaller
        // that one minute, we will use a one minute interval instead
        if (interval < 60) interval = 60;

        // @todo
        //  set a timer in your event loop, and make sure that you call
        //  connection->heartbeat() every _interval_ seconds if no other
        //  instruction was sent in that period.

        // return the interval that we want to use
        return interval;
    }
};
````

If you enable heartbeats, it is your own responsibility to ensure that the
```connection->heartbeat()``` method is called at least once during this period,
or that you call one of the other channel or connection methods to send data
over the connection. Heartbeats are sent by the server too, RabbitMQ also ensures
that _some data_ is sent over the connection from the server to the client 
during the heartbeat interval. It is also your responnsibility to shutdown 
the connection if you find out that the server stops sending data during 
this period.

If you use the AMQP::LibEvHandler event loop implementation, heartbeats are 
enabled by default, and all these checks are automatically performed.


CHANNELS
========

In the above example we created a channel object. A channel is a sort of virtual
connection, and it is possible to create many channels that all use
the same connection.

AMQP instructions are always sent over a channel, so before you can send the first
command to the RabbitMQ server, you first need a channel object. The channel
object has many methods to send instructions to the RabbitMQ server. It for
example has methods to declare queues and exchanges, to bind and unbind them,
and to publish and consume messages. You can best take a look at the channel.h
C++ header file for a list of all available methods. Every method in it is well
documented.

All operations that you can perform on a channel are non-blocking. This means
that it is not possible for a method (like Channel::declareExchange()) to
immediately return 'true' or 'false'. Instead, almost every method of the Channel
class returns an instance of the 'Deferred' class. This 'Deferred' object can be
used to install handlers that will be called in case of success or failure.

For example, if you call the channel.declareExchange() method, the AMQP-CPP library
will send a message to the RabbitMQ message broker to ask it to declare the
queue. However, because all operations in the library are asynchronous, the
declareExchange() method can not return 'true' or 'false' to inform you whether
the operation was succesful or not. Only after a while, after the instruction
has reached the RabbitMQ server, and the confirmation from the server has been
sent back to the client, the library can report the result of the declareExchange()
call.

To prevent any blocking calls, the channel.declareExchange() method returns a
'Deferred' result object, on which you can set callback functions that will be
called when the operation succeeds or fails.

````c++
// create a channel (or use TcpChannel if you're using the Tcp module)
Channel myChannel(&connection);

// declare an exchange, and install callbacks for success and failure
myChannel.declareExchange("my-exchange")

    .onSuccess([]() {
        // by now the exchange is created
    })

    .onError([](const char *message) {
        // something went wrong creating the exchange
    });
````

As you can see in the above example, we call the declareExchange() method, and
treat its return value as an object, on which we immediately install a lambda
callback function to handle success, and to handle failure.

Installing the callback methods is optional. If you're not interested in the
result of an operation, you do not have to install a callback for it. Next
to the onSuccess() and onError() callbacks that can be installed, you can also
install a onFinalize() method that gets called directly after the onSuccess()
and onError() methods, and that can be used to set a callback that should
run in either case: when the operation succeeds or when it fails.

The signature for the onError() method is always the same: it gets one parameter
with a human readable error message. The onSuccess() function has a different
signature depending on the method that you call. Most onSuccess() functions
(like the one we showed for the declareExchange() method) do not get any
parameters at all. Some specific onSuccess callbacks receive extra parameters
with additional information.


CHANNEL CALLBACKS
=================

As explained, most channel methods return a 'Deferred' object on which you can
install callbacks using the Deferred::onError() and Deferred::onSuccess() methods.

The callbacks that you install on a Deferred object, only apply to one specific
operation. If you want to install a generic error callback for the entire channel,
you can so so by using the Channel::onError() method. Next to the Channel::onError()
method, you can also install a callback to be notified when the channel is ready
for sending the first instruction to RabbitMQ.

````c++
// create a channel (or use TcpChannel if you use the Tcp module)
Channel myChannel(&connection);

// install a generic channel-error handler that will be called for every
// error that occurs on the channel
myChannel.onError([](const char *message) {

    // report error
    std::cout << "channel error: " << message << std::endl;
});

// install a generic callback that will be called when the channel is ready
// for sending the first instruction
myChannel.onReady([]() {

    // send the first instructions (like publishing messages)
});
````

In theory, you should wait for the onReady() callback to be called before you
send any other instructions over the channel. In practice however, the AMQP library
caches all instructions that were sent too early, so that you can use the
channel object right after it was constructed.


CHANNEL ERRORS
==============

It is important to realize that any error that occurs on a channel,
invalidates the entire channel, including all subsequent instructions that
were already sent over it. This means that if you call multiple methods in a row,
and the first method fails, all subsequent methods will not be executed either:

````c++
Channel myChannel(&connection);
myChannel.declareQueue("my-queue");
myChannel.declareExchange("my-exchange");
````

If the first declareQueue() call fails in the example above, the second
myChannel.declareExchange() method will not be executed, even when this
second instruction was already sent to the server. The second instruction will be
ignored by the RabbitMQ server because the channel was already in an invalid
state after the first failure.

You can overcome this by using multiple channels:

````c++
Channel channel1(&connection);
Channel channel2(&connection);
channel1.declareQueue("my-queue");
channel2.declareExchange("my-exchange");
````

Now, if an error occurs with declaring the queue, it will not have consequences
for the other call. But this comes at a small price: setting up the extra channel
requires and extra instruction to be sent to the RabbitMQ server, so some extra
bytes are sent over the network, and some additional resources in both the client
application and the RabbitMQ server are used (although this is all very limited).

If possible, it is best to make use of this feature. For example, if you have an important AMQP
connection that you use for consuming messages, and at the same time you want
to send another instruction to RabbitMQ (like declaring a temporary queue), it is
best to set up a new channel for this 'declare' instruction. If the declare fails,
it will not stop the consumer, because it was sent over a different channel.

The AMQP-CPP library allows you to create channels on the stack. It is not
a problem if a channel object gets destructed before the instruction was received by
the RabbitMQ server:

````c++
void myDeclareMethod(AMQP::Connection *connection)
{
    // create temporary channel to declare a queue
    AMQP::Channel channel(connection);

    // declare the queue (the channel object is destructed before the
    // instruction reaches the server, but the AMQP-CPP library can deal
    // with this)
    channel.declareQueue("my-new-queue");
}
````


FLAGS AND TABLES
================

Let's take a closer look at one method in the Channel object to explain
two other concepts of this AMQP-CPP library: flags and tables. The method that we
will be looking at is the Channel::declareQueue() method - but we could've
picked a different method too because flags and
tables are used by many methods.

````c++
/**
 *  Declare a queue
 *
 *  If you do not supply a name, a name will be assigned by the server.
 *
 *  The flags can be a combination of the following values:
 *
 *      -   durable     queue survives a broker restart
 *      -   autodelete  queue is automatically removed when all connected consumers are gone
 *      -   passive     only check if the queue exist
 *      -   exclusive   the queue only exists for this connection, and is automatically removed when connection is gone
 *
 *  @param  name        name of the queue
 *  @param  flags       combination of flags
 *  @param  arguments   optional arguments
 */
DeferredQueue &declareQueue(const std::string &name, int flags, const Table &arguments);
DeferredQueue &declareQueue(const std::string &name, const Table &arguments);
DeferredQueue &declareQueue(const std::string &name, int flags = 0);
DeferredQueue &declareQueue(int flags, const Table &arguments);
DeferredQueue &declareQueue(const Table &arguments);
DeferredQueue &declareQueue(int flags = 0);
````

As you can see, the method comes in many forms, and it is up to you to choose
the one that is most appropriate. We now take a look at the most complete
one, the method with three parameters.

All above methods returns a 'DeferredQueue' object. The DeferredQueue class
extends from the AMQP::Deferred class and allows you to install a more powerful
onSuccess() callback function. The 'onSuccess' method for the declareQueue()
function gets three arguments:

````c++
// create a custom callback
auto callback = [](const std::string &name, int msgcount, int consumercount) {

    // @todo add your own implementation

};

// declare the queue, and install the callback that is called on success
channel.declareQueue("myQueue").onSuccess(callback);
````

Just like many others methods in the Channel class, the declareQueue() method
accepts an integer parameter named 'flags'. This is a variable in which you can
set method-specific options, by summing up all the options that are described in
the documentation above the method. If you for example want to create a durable,
auto-deleted queue, you can pass in the value AMQP::durable + AMQP::autodelete.

The declareQueue() method also accepts a parameter named 'arguments', which is of type
Table. This Table object can be used as an associative array to send additional
options to RabbitMQ, that are often custom RabbitMQ extensions to the AMQP
standard. For a list of all supported arguments, take a look at the documentation
on the RabbitMQ website. With every new RabbitMQ release more features, and
supported arguments are added.

The Table class is a very powerful class that enables you to build
complicated, deeply nested structures full of strings, arrays and even other
tables. In reality, you only need strings and integers.

````c++
// custom options that are passed to the declareQueue call
AMQP::Table arguments;
arguments["x-dead-letter-exchange"] = "some-exchange";
arguments["x-message-ttl"] = 3600 * 1000;
arguments["x-expires"] = 7200 * 1000;

// declare the queue
channel.declareQueue("my-queue-name", AMQP::durable + AMQP::autodelete, arguments);
````


PUBLISHING MESSAGES
===================

Publishing messages is easy, and the Channel class has a list of methods that
can all be used for it. The most simple one takes three arguments: the name of the
exchange to publish to, the routing key to use, and the actual message that
you're publishing - all these parameters are standard C++ strings.

More extended versions of the publish() method exist that accept additional
arguments, and that enable you to publish entire Envelope objects. An envelope
is an object that contains the message plus a list of optional meta properties like
the content-type, content-encoding, priority, expire time and more. None of these
meta fields are interpreted by this library, and also the RabbitMQ ignores most
of them, but the AMQP protocol defines them, and they are free for you to use.
For an extensive list of the fields that are supported, take a look at the MetaData.h
header file (MetaData is the base class for Envelope). You should also check the
RabbitMQ documentation to find out if an envelope header is interpreted by the
RabbitMQ server (at the time of this writing, only the expire time is being used).

The following snippet is copied from the Channel.h header file and lists all
available publish() methods. As you can see, you can call the publish() method
in almost any form:

````c++
/**
 *  Publish a message to an exchange
 * 
 *  You have to supply the name of an exchange and a routing key. RabbitMQ will 
 *  then try to send the message to one or more queues. With the optional flags 
 *  parameter you can specify what should happen if the message could not be routed 
 *  to a queue. By default, unroutable message are silently discarded.
 * 
 *  This method returns a reference to a DeferredPublisher object. You can use 
 *  this returned object to install callbacks that are called when an undeliverable 
 *  message is returned, or to set the callback that is called when the server 
 *  confirms that the message was received. 
 * 
 *  To enable handling returned messages, or to enable publisher-confirms, you must 
 *  not only set the callback, but also pass in appropriate flags to enable this 
 *  feature. If you do not pass in these flags, your callbacks will not be called. 
 *  If you are not at all interested in returned messages or publish-confirms, you 
 *  can ignore the flag and the returned object.
 * 
 *  Watch out: the channel returns _the same_ DeferredPublisher object for all 
 *  calls to the publish() method. This means that the callbacks that you install 
 *  for the first published message are also used for subsequent messages _and_ 
 *  it means that if you install a different callback for a later publish 
 *  operation, it overwrites your earlier callbacks 
 * 
 *  The following flags can be supplied:
 * 
 *      -   mandatory   If set, server returns messages that are not sent to a queue
 *      -   immediate   If set, server returns messages that can not immediately be forwarded to a consumer. 
 * 
 *  @param  exchange    the exchange to publish to
 *  @param  routingkey  the routing key
 *  @param  envelope    the full envelope to send
 *  @param  message     the message to send
 *  @param  size        size of the message
 *  @param  flags       optional flags
 */
DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const Envelope &envelope, int flags = 0) { return _implementation->publish(exchange, routingKey, envelope, flags); }
DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const std::string &message, int flags = 0) { return _implementation->publish(exchange, routingKey, Envelope(message.data(), message.size()), flags); }
DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const char *message, size_t size, int flags = 0) { return _implementation->publish(exchange, routingKey, Envelope(message, size), flags); }
DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const char *message, int flags = 0) { return _implementation->publish(exchange, routingKey, Envelope(message, strlen(message)), flags); }
````

Published messages are normally not confirmed by the server, and the RabbitMQ
will not send a report back to inform you whether the message was successfully
published or not. But with the flags you can instruct RabbitMQ to send back
the message if it was undeliverable.

You can also use transactions to ensure that your messages get delivered.
Let's say that you are publishing many messages in a row. If you get
an error halfway through there is no way to know for sure how many messages made
it to the broker and how many should be republished. If this is important, you can
wrap the publish commands inside a transaction. In this case, if an error occurs,
the transaction is automatically rolled back by RabbitMQ and none of the messages
are actually published.

````c++
// start a transaction
channel.startTransaction();

// publish a number of messages
channel.publish("my-exchange", "my-key", "my first message");
channel.publish("my-exchange", "my-key", "another message");

// commit the transactions, and set up callbacks that are called when
// the transaction was successful or not
channel.commitTransaction()
    .onSuccess([]() {
        // all messages were successfully published
    })
    .onError([](const char *message) {
        // none of the messages were published
        // now we have to do it all over again
    });
````

Note that AMQP transactions are not as powerful as transactions that are
knows in the database world. It is not possible to wrap all sort of
operations in a transaction, they are only meaningful for publishing
and consuming.

PUBLISHER CONFIRMS
===================

RabbitMQ supports lightweight method of confirming that broker received and processed
a message. For this method to work, the channel needs to be put in so-called _confirm mode_.
This is done using confirmSelect() method. When channel is successfully put in 
confirm mode, the server and client count messages (starting from 1) and server sends
acknowledgments for every message it processed (it can also acknowledge multiple message at
once). 

If server is unable to process a message, it will send send negative acknowledgments. Both 
positive and negative acknowledgments handling are implemented as callbacks for Channel object.

````c++
// setup confirm mode and ack/nack callbacks
channel.confirmSelect().onSuccess([&]() {
    // from this moment onwards ack/nack confirmations are coming in

    channel.publish("my-exchange", "my-key", "my first message");
    // message counter is now 1, will call onAck/onNack with deliverTag=1

    channel.publish("my-exchange", "my-key", "my second message");
    // message counter is now 2, will call onAck/onNack with deliverTag=2

}).onAck([&](uint64_t deliverTag, bool multiple) {
    // deliverTag is message number
    // multiple is set to true, if all messages UP TO deliverTag have been processed
}).onNack([&](uint64 deliveryTag, bool multiple, bool requeue) {
    // deliverTag is message number
    // multiple is set to true, if all messages UP TO deliverTag have not been processed
    // requeue is to be ignored
});

````

For more information, please see http://www.rabbitmq.com/confirms.html.

CONSUMING MESSAGES
==================

Fetching messages from RabbitMQ is called consuming, and can be started by calling
the method Channel::consume(). After you've called this method, RabbitMQ starts
delivering messages to you.

Just like the publish() method that we just described, the consume() method also
comes in many forms. The first parameter is always the name of the queue you like
to consume from. The subsequent parameters are an optional consumer tag, flags and
a table with custom arguments. The first additional parameter, the consumer tag,
is nothing more than a string identifier that you can use when you want to stop
consuming.

The full documentation from the C++ Channel.h headerfile looks like this:

````c++
/**
 *  Tell the RabbitMQ server that we're ready to consume messages
 *
 *  After this method is called, RabbitMQ starts delivering messages to the client
 *  application. The consume tag is a string identifier that will be passed to
 *  each received message, so that you can associate incoming messages with a
 *  consumer. If you do not specify a consumer tag, the server will assign one
 *  for you.
 *
 *  The following flags are supported:
 *
 *      -   nolocal             if set, messages published on this channel are
 *                              not also consumed
 *
 *      -   noack               if set, consumed messages do not have to be acked,
 *                              this happens automatically
 *
 *      -   exclusive           request exclusive access, only this consumer can
 *                              access the queue
 *
 *  The callback registered with DeferredConsumer::onSuccess() will be called when the
 *  consumer has started.
 *
 *  @param  queue               the queue from which you want to consume
 *  @param  tag                 a consumer tag that will be associated with this consume operation
 *  @param  flags               additional flags
 *  @param  arguments           additional arguments
 *  @return bool
 */
DeferredConsumer &consume(const std::string &queue, const std::string &tag, int flags, const AMQP::Table &arguments);
DeferredConsumer &consume(const std::string &queue, const std::string &tag, int flags = 0);
DeferredConsumer &consume(const std::string &queue, const std::string &tag, const AMQP::Table &arguments);
DeferredConsumer &consume(const std::string &queue, int flags, const AMQP::Table &arguments);
DeferredConsumer &consume(const std::string &queue, int flags = 0);
DeferredConsumer &consume(const std::string &queue, const AMQP::Table &arguments);
````

As you can see, the consume method returns a DeferredConsumer. This object is a
regular Deferred, with  additions. The onSuccess() method of a
DeferredConsumer is slightly different than the onSuccess() method of a regular
Deferred object: one extra parameter will be supplied to your callback function
with the consumer tag.

The onSuccess() callback will be called when the consume operation _has started_,
but not when messages are actually consumed. For this you will have to install
a different callback, using the onReceived() method.

````c++
// callback function that is called when the consume operation starts
auto startCb = [](const std::string &consumertag) {

    std::cout << "consume operation started" << std::endl;
};

// callback function that is called when the consume operation failed
auto errorCb = [](const char *message) {

    std::cout << "consume operation failed" << std::endl;
};

// callback operation when a message was received
auto messageCb = [&channel](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {

    std::cout << "message received" << std::endl;

    // acknowledge the message
    channel.ack(deliveryTag);
};

// start consuming from the queue, and install the callbacks
channel.consume("my-queue")
    .onReceived(messageCb)
    .onSuccess(startCb)
    .onError(errorCb);

````

The Message object holds all information of the delivered message: the actual
content, all meta information from the envelope (in fact, the Message class is
derived from the Envelope class), and even the name of the exchange and the
routing key that were used when the message was originally published. For a full
list of all information in the Message class, you best have a look at the
message.h, envelope.h and metadata.h header files.

Another important parameter to the onReceived() method is the deliveryTag parameter.
This is a unique identifier that you need to acknowledge an incoming message.
RabbitMQ only removes the message after it has been acknowledged, so that if your
application crashes while it was busy processing the message, the message does
not get lost but remains in the queue. But this means that after you've processed
the message, you must inform RabbitMQ about it by calling the Channel:ack() method.
This method is very simple and takes in its simplest form only one parameter: the
deliveryTag of the message.

Consuming messages is a continuous process. RabbitMQ keeps sending messages, until
you stop the consumer, which can be done by calling the Channel::cancel() method.
If you close the channel, or the entire TCP connection, consuming also stops.

RabbitMQ throttles the number of messages that are delivered to you, to prevent
that your application is flooded with messages from the queue, and to spread out
the messages over multiple consumers. This is done with a setting called
quality-of-service (QOS). The QOS setting is a numeric value which holds the number
of unacknowledged messages that you are allowed to have. RabbitMQ stops sending
additional messages when the number of unacknowledges messages has reached this
limit, and only sends additional messages when an earlier message gets acknowledged.
To change the QOS, you can simple call Channel::setQos().


UPGRADING
=========

AMQP-CPP 4.* is not always compatible with previous versions. Especially some 
virtual methods in the ConnectionHandler and TcpHandler classes have been renamed 
or are called during a different stage in the connection lifetime. Check 
out this README file and the comments inside the connectionhandler.h and 
tcphandler.h files to find out if your application has to be changed. You
should especially check the following:

- ConnectionHandler::onConnected has been renamed to ConnectionHandler::onReady
- TcpHandler::onConnected is now called sooner: when the TCP connection is 
  established, instead of when the AMQP connection is ready for instructions.
- The new method TcpHandler::onReady is called when the AMQP connection is 
  ready to be used (this is the old behavior of TcpHandler::onConnected)
- TcpHandler::onError is no longer the last method that is called (TcpHandler::onLost 
  could be called and TcpHandler::onDetached will be called after the error too)
- TcpHandler::onClosed is now called to indicate the graceful end of the 
  AMQP protocol, and not the end of TCP connection.
- TcpHandler::onLost is called when the TCP connection is lost or closed.
- The new method TcpHandler::onDetached is a better alternative for cleanup 
  code instead of TcpHandler::onClosed and/or TcpHandler::onError.


WORK IN PROGRESS
================

Almost all AMQP features have been implemented. But the following things might
need additional attention:

    -   ability to set up secure connections (or is this fully done on the IO level)
    -   login with other protocols than login/password

We also need to add more safety checks so that strange or invalid data from
RabbitMQ does not break the library (although in reality RabbitMQ only sends
valid data). Also, when we now receive an answer from RabbitMQ that does not
match the request that we sent before, we do not report an error (this is also
an issue that only occurs in theory).

It would be nice to have sample implementations for the ConnectionHandler
class that can be directly plugged into libev, libevent and libuv event loops.

For performance reasons, we need to investigate if we can limit the number of times
an incoming or outgoing messages is copied.
