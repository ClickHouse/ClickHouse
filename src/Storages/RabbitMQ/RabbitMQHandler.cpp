#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_RABBITMQ;
}

/* The object of this class is shared between concurrent consumers (who share the same connection == share the same
 * event loop and handler).
 */
RabbitMQHandler::RabbitMQHandler(uv_loop_t * loop_, Poco::Logger * log_) :
    AMQP::LibUvHandler(loop_),
    loop(loop_),
    log(log_)
{
}


void RabbitMQHandler::onError(AMQP::TcpConnection * connection, const char * message)
{
    LOG_ERROR(log, "Library error report: {}", message);

    if (!connection->usable() || !connection->ready())
    {
        throw Exception("Connection error", ErrorCodes::CANNOT_CONNECT_RABBITMQ);
    }
}


void RabbitMQHandler::startBackgroundLoop()
{
    /// stop_loop variable is updated in a separate thread
    while (!stop_loop.load())
    {
        uv_run(loop, UV_RUN_NOWAIT);
    }
}


void RabbitMQHandler::startLoop()
{
    if (starting_loop.try_lock())
    {
        uv_run(loop, UV_RUN_NOWAIT);
        starting_loop.unlock();
    }
}

}
