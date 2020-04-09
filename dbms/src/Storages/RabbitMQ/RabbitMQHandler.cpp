#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>

#include <stdio.h>
namespace DB
{

/// to connect to rabbitmq server
RabbitMQHandler::RabbitMQHandler(Poco::Logger * log_) :
        log(log_)
{
}

RabbitMQHandler::~RabbitMQHandler()
{
}

void RabbitMQHandler::onAttached(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "A new connection attached to the hadler.");

    /// TODO:init all that is needed further here
}

void RabbitMQHandler::onConnected(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "TCP connection has been established. Setting up started.");
}

void RabbitMQHandler::onReady(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "Login attempt succeeded. TcpConnection is ready to use.");
}

void RabbitMQHandler::onError(AMQP::TcpConnection * /* connection */, const char * message)
{
    LOG_ERROR(log, message);
}

void RabbitMQHandler::onClosed(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "Closing the connection.");

    /// TODO: Close the connection here.
}

void RabbitMQHandler::onLost(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "TcpConnection closed or lost.");
}

void RabbitMQHandler::onDetached(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "TcpConnection is detached.");
}

void RabbitMQHandler::monitor(AMQP::TcpConnection * /* connection */, int /* fd */, int /* flags */ )
{
    LOG_TRACE(log, "monitor() method is called.");
}

}
