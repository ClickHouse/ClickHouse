#pragma once
#include <memory>
#include <amqpcpp.h>
#include <Poco/Net/StreamSocket.h>
#include <common/Types.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{
/**
 * The library does not do any IO by itself, and you need to pass an object to the library that the
 * library can use for IO. So, before you start using the library, you first need to create a class that
 * extends from the ConnectionHandler base class. This is a class with a number of methods that are
 * called by the library every time it wants to send out data, or when it needs to inform you that an error occured.
 */

class RabbitMQHandlerImpl;
class RabbitMQHandler: public AMQP::ConnectionHandler
{
public:

    RabbitMQHandler(const std::pair<std::string, UInt16> & parsedAddress_, Poco::Logger * log_);

    ~RabbitMQHandler() override;

    void process();
    void onProduced();
    void onProcessed();
    bool connected() const;

    RabbitMQHandler(const RabbitMQHandler&) = delete;
    RabbitMQHandler& operator=(const RabbitMQHandler&) = delete;

    const String get_user_name() { return user_name; }
    const String get_password() { return password; }

private:
    void onReady(AMQP::Connection * conection) override;
    void onData(AMQP::Connection * connection, const char *data, size_t size) override;
    void onError(AMQP::Connection * connection, const char *message) override;
    void onClosed(AMQP::Connection * connection) override;

    void sendDataToRabbitMQ();

private:
    Poco::Logger * log;
    String user_name;
    String password;

    std::shared_ptr<RabbitMQHandlerImpl> handler_impl;
};

class RabbitMQHandlerImpl
{
public:
    RabbitMQHandlerImpl() :
            connected(false),
            connection(nullptr),
            closed(false),
            readable(false),
            outputBuffer(nullptr, 0),
            inputBuffer(nullptr, 0)
    {
    }
    Poco::Net::StreamSocket socket;
    bool connected = false;
    AMQP::Connection * connection;
    bool closed;
    bool readable;

    WriteBuffer outputBuffer;
    WriteBuffer inputBuffer;
    std::vector<char> tmpBuff;
};

}
