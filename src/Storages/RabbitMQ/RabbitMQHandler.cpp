#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Core/Field.h>
#include <base/scope_guard.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
}

void RabbitMQHandler::startLoop()
{
    if (loop_running.exchange(true))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Loop is already running");

    SCOPE_EXIT({
        loop_running.store(false);
        loop_running.notify_all();
    });

    updateLoopState(Loop::RUN);

    LOG_DEBUG(log, "Background loop started");

    while (loop_state.load() == Loop::RUN)
        uv_run(loop, UV_RUN_NOWAIT);

    LOG_DEBUG(log, "Background loop ended");
}

/// Do not need synchronization as in iterateLoop(), because this method is used only for
/// initial RabbitMQ setup - at this point there is no background loop thread.
int RabbitMQHandler::startBlockingLoop()
{
    LOG_DEBUG(log, "Starting blocking loop.");
    updateLoopState(Loop::RUN);
    SCOPE_EXIT({
        LOG_DEBUG(log, "Finished blocking loop.");
        updateLoopState(Loop::STOP);
    });
    return uv_run(loop, UV_RUN_DEFAULT);
}

void RabbitMQHandler::stopLoop(bool background)
{
    if (background && !loop_running.load())
    {
        chassert(loop_state.load() != Loop::RUN);
        return;
    }

    if (background)
        LOG_DEBUG(log, "Background loop stop.");
    else
        LOG_DEBUG(log, "Blocking loop stop.");

    updateLoopState(Loop::STOP);
    uv_stop(loop);

    if (background)
    {
        LOG_TRACE(log, "Waiting for loop to finish ({}).", loop_running.load());

        loop_running.wait(false);

        LOG_TRACE(log, "Loop finished.");
    }
}

int RabbitMQHandler::iterateLoop()
{
    updateLoopState(Loop::RUN);
    SCOPE_EXIT({
        updateLoopState(Loop::STOP);
    });
    return uv_run(loop, UV_RUN_NOWAIT);
}

void RabbitMQHandler::updateLoopState(UInt8 state)
{
    const auto prev_loop_state = loop_state.exchange(state);
    if (state == Loop::RUN && prev_loop_state == Loop::RUN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Loop is already in state RUN");

    LOG_TEST(log, "Updated loop state from {} to {}", toString(prev_loop_state), toString(state));

}

}
