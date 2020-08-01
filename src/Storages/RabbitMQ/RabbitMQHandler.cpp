#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

/* The object of this class is shared between concurrent consumers (who share the same connection == share the same
 * event loop and handler).
 */
RabbitMQHandler::RabbitMQHandler(uv_loop_t * loop_, Poco::Logger * log_) :
    AMQP::LibUvHandler(loop_),
    loop(loop_),
    log(log_)
{
}

///Method that is called when the connection ends up in an error state.
void RabbitMQHandler::onError(AMQP::TcpConnection * connection, const char * message)
{
    connection_running.store(false);
    LOG_ERROR(log, "Library error report: {}", message);

    if (connection)
        connection->close();
}

void RabbitMQHandler::onReady(AMQP::TcpConnection * /* connection */)
{
    connection_running.store(true);
}

void RabbitMQHandler::startLoop()
{
    std::lock_guard lock(startup_mutex);
    /// stop_loop variable is updated in a separate thread
    while (!stop_loop.load() && connection_running.load())
        uv_run(loop, UV_RUN_NOWAIT);
}

void RabbitMQHandler::iterateLoop()
{
    std::unique_lock lock(startup_mutex, std::defer_lock);
    if (lock.try_lock())
        uv_run(loop, UV_RUN_NOWAIT);
}

}
