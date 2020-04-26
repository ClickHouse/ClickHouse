#pragma once

#include <memory>
#include <amqpcpp.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/linux_tcp.h>
#include <event2/event.h>
#include <Poco/Net/StreamSocket.h>
#include <common/types.h>


namespace DB
{
/**
 * The library does not do any IO by itself, and you need to pass an object to the library that the
 * library can use for IO. So, before you start using the library, you first need to create a class that
 * extends from the ConnectionHandler base class. This is a class with a number of methods that are
 * called by the library every time it wants to send out data, or when it needs to inform you that an error occured.
 */

class ConnectionImpl;
class RabbitMQHandler: public AMQP::LibEventHandler
{
public:

    RabbitMQHandler(event_base * event, Poco::Logger * log_);
    ~RabbitMQHandler() override;

    bool connected() const;
    //void process();

    RabbitMQHandler(const RabbitMQHandler&) = delete;
    RabbitMQHandler& operator=(const RabbitMQHandler&) = delete;

    const String & get_user_name() { return user_name; }
    const String & get_password() { return password; }

    //void onReady(AMQP::TcpConnection * conection) override;
    //void onError(AMQP::TcpConnection * connection, const char *message) override;
    //void onClosed(AMQP::TcpConnection * connection) override;

private:
    Poco::Logger * log;
    String user_name;
    String password;

    std::shared_ptr<ConnectionImpl> handler_impl;
};


class ConnectionImpl
{
public:
    ConnectionImpl() :
            connected(false),
            connection(nullptr),
            closed(false),
            readable(false)
    {
    }
    bool connected = false;
    AMQP::TcpConnection * connection;
    bool closed;
    bool readable;

    std::vector<char> tmpBuff;
};

}
