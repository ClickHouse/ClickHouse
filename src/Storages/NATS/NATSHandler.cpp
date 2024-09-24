#include <Core/Defines.h>
#include <Storages/NATS/NATSHandler.h>
#include <adapters/libuv.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
}

/* The object of this class is shared between concurrent consumers (who share the same connection == share the same
 * event loop and handler).
 */

static const auto MAX_THREAD_WORK_DURATION_MS = 60000;

NATSHandler::NATSHandler(LoggerPtr log_)
    : log(log_)
    , loop_running(false)
    , loop_state(Loop::STOP)
{
    natsLibuv_Init();
    natsLibuv_SetThreadLocalLoop(loop.getLoop());
}

void NATSHandler::startLoop()
{
    std::lock_guard lock(startup_mutex);
    natsLibuv_SetThreadLocalLoop(loop.getLoop());

    LOG_DEBUG(log, "Background loop started");
    loop_running.store(true);
    auto start_time = std::chrono::steady_clock::now();
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    while (loop_state.load() == Loop::RUN && duration.count() < MAX_THREAD_WORK_DURATION_MS)
    {
        uv_run(loop.getLoop(), UV_RUN_NOWAIT);
        end_time = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    }

    LOG_DEBUG(log, "Background loop ended");
    loop_running.store(false);
}

void NATSHandler::iterateLoop()
{
    std::unique_lock lock(startup_mutex, std::defer_lock);
    if (lock.try_lock())
    {
        natsLibuv_SetThreadLocalLoop(loop.getLoop());
        uv_run(loop.getLoop(), UV_RUN_NOWAIT);
    }
}

LockPtr NATSHandler::setThreadLocalLoop()
{
    auto lock = std::make_unique<std::lock_guard<std::mutex>>(startup_mutex);
    natsLibuv_SetThreadLocalLoop(loop.getLoop());
    return lock;
}

void NATSHandler::stopLoop()
{
    LOG_DEBUG(log, "Implicit loop stop.");
    uv_stop(loop.getLoop());
}

NATSOptionsPtr NATSHandler::createOptions()
{
    natsOptions * options = nullptr;
    auto er = natsOptions_Create(&options);
    if(er){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Can not initialize NATS options. Nats error: {}",
            natsStatus_GetText(er));
    }

    NATSOptionsPtr result(options, &natsOptions_Destroy);
    er = natsOptions_SetEventLoop(result.get(), static_cast<void *>(loop.getLoop()),
                                  natsLibuv_Attach,
                                  natsLibuv_Read,
                                  natsLibuv_Write,
                                  natsLibuv_Detach);
    if(er){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Can not set event loop. Nats error: {}",
            natsStatus_GetText(er));
    }

    natsOptions_SetIOBufSize(result.get(), DBMS_DEFAULT_BUFFER_SIZE);
    natsOptions_SetSendAsap(result.get(), true);

    return result;
}

NATSHandler::~NATSHandler()
{
    auto lock = setThreadLocalLoop();

    LOG_DEBUG(log, "Blocking loop started.");
    uv_run(loop.getLoop(), UV_RUN_DEFAULT);
}

}
