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

NATSHandler::NATSHandler(LoggerPtr log_)
    : log(log_)
    , loop_state(Loop::STOP)
{
}

void NATSHandler::runLoop()
{
    if(loop_state.load() != Loop::STOP)
    {
        return;
    }

    natsLibuv_Init();
    natsLibuv_SetThreadLocalLoop(loop.getLoop());

    loop_state.store(Loop::RUN);

    LOG_DEBUG(log, "Background loop started");

    std::size_t num_pending_tasks = 0;
    {
        std::lock_guard<std::mutex> lock(tasks_mutex);
        num_pending_tasks = tasks.size();
    }

    int num_pending_callbacks = 0;
    while (loop_state.load() == Loop::RUN || num_pending_callbacks != 0 || num_pending_tasks != 0)
    {
        std::queue<Task> executed_tasks;
        {
            std::lock_guard<std::mutex> lock(tasks_mutex);
            std::swap(executed_tasks, tasks);

            num_pending_tasks = tasks.size();
        }

        while(!executed_tasks.empty())
        {
            const auto & task = executed_tasks.front();
            task();
            executed_tasks.pop();
        }

        num_pending_callbacks = uv_run(loop.getLoop(), UV_RUN_NOWAIT);
    }
    loop_state.store(Loop::CLOSED);

    LOG_DEBUG(log, "Background loop ended");
}

void NATSHandler::stopLoop()
{
    if(loop_state.load() != Loop::RUN)
    {
        return;
    }

    LOG_DEBUG(log, "Implicit loop stop.");
    loop_state.store(Loop::STOP);
}

void NATSHandler::post(Task task)
{
    std::lock_guard<std::mutex> lock(tasks_mutex);
    tasks.push(std::move(task));
}

NATSOptionsPtr NATSHandler::createOptions()
{
    natsOptions * options = nullptr;
    auto er = natsOptions_Create(&options);
    if(er)
    {
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
    if(er)
    {
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Can not set event loop. Nats error: {}",
            natsStatus_GetText(er));
    }

    natsOptions_SetIOBufSize(result.get(), DBMS_DEFAULT_BUFFER_SIZE);
    natsOptions_SetSendAsap(result.get(), true);

    return result;
}

}
