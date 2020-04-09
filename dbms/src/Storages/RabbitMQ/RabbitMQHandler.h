#pragma once
#include <memory>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <Poco/Net/StreamSocket.h>
#include <common/types.h>

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

class RabbitMQHandler: public AMQP::TcpHandler
{
public:

    RabbitMQHandler(Poco::Logger * log_);

    ~RabbitMQHandler() override;

    RabbitMQHandler(const RabbitMQHandler&) = delete;
    RabbitMQHandler& operator=(const RabbitMQHandler&) = delete;

private:
    void onAttached(AMQP::TcpConnection * conection) override;
    void onConnected(AMQP::TcpConnection * conection) override;
    void onReady(AMQP::TcpConnection * conection) override;
    void onError(AMQP::TcpConnection * connection, const char *message) override;
    void onClosed(AMQP::TcpConnection * connection) override;
    void onLost(AMQP::TcpConnection * connection) override;
    void onDetached(AMQP::TcpConnection * connection) override;
    void monitor(AMQP::TcpConnection *connection, int fd, int flags) override;

private:
    Poco::Logger * log;
};

}
