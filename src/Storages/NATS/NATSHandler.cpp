#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Storages/NATS/NATSHandler.h>
#include <adapters/libuv.h>

namespace DB
{

/* The object of this class is shared between concurrent consumers (who share the same connection == share the same
 * event loop and handler).
 */
NATSHandler::NATSHandler(uv_loop_t * loop_, Poco::Logger * log_) :
    loop(loop_),
    log(log_),
    connection_running(false),
    loop_running(false),
    loop_state(Loop::STOP)
{
    natsLibuv_Init();
    natsLibuv_SetThreadLocalLoop(loop);
    natsOptions_Create(&opts);
    status = natsOptions_SetEventLoop(opts, static_cast<void *>(loop),
                                 natsLibuv_Attach,
                                 natsLibuv_Read,
                                 natsLibuv_Write,
                                 natsLibuv_Detach);
}

void NATSHandler::startLoop()
{
    std::lock_guard lock(startup_mutex);
    natsLibuv_SetThreadLocalLoop(loop);

    LOG_DEBUG(log, "Background loop started");
    loop_running.store(true);

    while (loop_state.load() == Loop::RUN)
    {
        uv_run(loop, UV_RUN_NOWAIT);
    }

    LOG_DEBUG(log, "Background loop ended");
    loop_running.store(false);
}

void NATSHandler::changeConnectionStatus(bool is_running) {
    connection_running.store(is_running);
}

void NATSHandler::iterateLoop()
{
    std::unique_lock lock(startup_mutex, std::defer_lock);
    if (lock.try_lock())
    {
        natsLibuv_SetThreadLocalLoop(loop);
        uv_run(loop, UV_RUN_NOWAIT);
    }
}

void NATSHandler::setThreadLocalLoop() {
    std::lock_guard lock(startup_mutex);
    natsLibuv_SetThreadLocalLoop(loop);
}

/// Do not need synchronization as in iterateLoop(), because this method is used only for
/// initial NATS setup - at this point there is no background loop thread.
void NATSHandler::startBlockingLoop()
{
    LOG_DEBUG(log, "Started blocking loop.");
    natsLibuv_SetThreadLocalLoop(loop);
    uv_run(loop, UV_RUN_DEFAULT);
}

void NATSHandler::stopLoop()
{
    LOG_DEBUG(log, "Implicit loop stop.");
    uv_stop(loop);
}

NATSHandler::~NATSHandler() {
    natsOptions_Destroy(opts);
}

}
