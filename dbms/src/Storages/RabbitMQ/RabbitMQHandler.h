#pragma once
#include <memory>
#include <amqpcpp.h>
#include <Poco/Net/StreamSocket.h>

namespace DB
{
/**
 * The library does not do any IO by itself, and you need to pass an object to the library that the
 * library can use for IO. So, before you start using the library, you first need to create a class that
 * extends from the ConnectionHandler base class. This is a class with a number of methods that are
 * called by the library every time it wants to send out data, or when it needs to inform you that an error occured.
 *
 * Probably will be combined with classes RabbitMQBlockInput(Output)Stream
 */

class RabbitMQHandlerImpl;
class RabbitMQHandler: public AMQP::ConnectionHandler
{
public:

    RabbitMQHandler(const std::string& host, uint16_t port);
    virtual ~RabbitMQHandler();

    void loop();
    void quit();

    bool connected() const;

private:

    RabbitMQHandler(const RabbitMQHandler&) = delete;
    RabbitMQHandler& operator=(const RabbitMQHandler&) = delete;

    void close();

    virtual void onData(AMQP::Connection *connection, const char *data, size_t size);

    virtual void onConnected(AMQP::Connection *connection);
    virtual void onError(AMQP::Connection *connection, const char *message);
    virtual void onClosed(AMQP::Connection *connection);

private:
    std::shared_ptr<RabbitMQHandlerImpl> m_impl;
};


class RabbitMQHandlerImpl
{
public:
    RabbitMQHandlerImpl() :
            connected(false),
            connection(nullptr),
            quit(false)
    {
    }
    Poco::Net::StreamSocket socket;
    bool connected;
    AMQP::Connection* connection;
    bool quit;
    /** Buffer inputBuffer;
    Buffer outBuffer; */
};

}