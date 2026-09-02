#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <thread>

namespace DB
{

/* The object of this class is shared between concurrent consumers (who share the same connection == share the same
 * event loop and handler).
 */
RabbitMQHandler::RabbitMQHandler(uv_loop_t * loop_, LoggerPtr log_) :
    AMQP::LibUvHandler(loop_),
    loop(loop_),
    log(log_),
    connection_running(false),
    loop_running(false),
    loop_state(Loop::STOP)
{
}

///Method that is called when the connection ends up in an error state.
void RabbitMQHandler::onError(AMQP::TcpConnection * /* connection */, const char * message)
{
    LOG_ERROR(log, "Library error report: {}", message);
    connection_running.store(false);
}

void RabbitMQHandler::onReady(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "Connection is ready");
    connection_running.store(true);
    loop_state.store(Loop::RUN);
}

void RabbitMQHandler::startLoop()
{
    std::lock_guard lock(startup_mutex);

    LOG_DEBUG(log, "Background loop started");
    loop_running.store(true);

    while (loop_state.load() == Loop::RUN)
    {
        if (!uv_run(loop, UV_RUN_NOWAIT))
            std::this_thread::yield();
    }

    LOG_DEBUG(log, "Background loop ended");
    loop_running.store(false);
}

int RabbitMQHandler::iterateLoop()
{
    std::unique_lock lock(startup_mutex, std::defer_lock);
    if (lock.try_lock())
        return uv_run(loop, UV_RUN_NOWAIT);
    return 1; /// We cannot know how actual value.
}

/// Do not need synchronization as in iterateLoop(), because this method is used only for
/// initial RabbitMQ setup - at this point there is no background loop thread.
int RabbitMQHandler::startBlockingLoop()
{
    LOG_DEBUG(log, "Started blocking loop.");
    return uv_run(loop, UV_RUN_DEFAULT);
}

bool RabbitMQHandler::startBlockingLoopWithTimeout(uint64_t timeout_ms)
{
    LOG_DEBUG(log, "Started blocking loop with {}ms timeout.", timeout_ms);

    bool timed_out = false;

    uv_timer_t timer;
    uv_timer_init(loop, &timer);
    timer.data = &timed_out;

    uv_timer_start(
        &timer,
        [](uv_timer_t * t)
        {
            *static_cast<bool *>(t->data) = true;
            uv_stop(t->loop);
        },
        timeout_ms,
        /* repeat = */ 0);

    uv_run(loop, UV_RUN_DEFAULT);

    uv_timer_stop(&timer);
    /// Close the timer handle so libuv can release its resources.
    /// The close callback runs on the next NOWAIT tick below.
    uv_close(reinterpret_cast<uv_handle_t *>(&timer), nullptr);
    uv_run(loop, UV_RUN_NOWAIT);

    return !timed_out;
}

void RabbitMQHandler::stopLoop()
{
    LOG_DEBUG(log, "Stopping background loop.");
    loop_state.store(Loop::STOP);
}

void RabbitMQHandler::stopBlockingLoop()
{
    LOG_DEBUG(log, "Stopping blocking loop.");
    uv_stop(loop);
}

}
