#include <Storages/NATS/NATSHandler.h>

#include <Storages/NATS/NATSLibUVAdapter.h>

#include <Core/Defines.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CONNECT_NATS;
}

NATSHandler::NATSHandler(LoggerPtr log_)
    : log(log_)
    , loop_state(Loop::INITIALIZED)
{
}

void NATSHandler::runLoop()
{
    if (loop_state.load() != Loop::INITIALIZED)
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

        while (!executed_tasks.empty())
        {
            auto task = std::move(executed_tasks.front());
            executed_tasks.pop();
            task();
        }

        num_pending_callbacks = uv_run(loop.getLoop(), UV_RUN_NOWAIT);
    }
    loop_state.store(Loop::CLOSED);

    LOG_DEBUG(log, "Background loop ended");

    nats_ReleaseThreadMemory();
}

void NATSHandler::stopLoop()
{
    if (loop_state.load() != Loop::RUN)
    {
        return;
    }

    LOG_DEBUG(log, "Implicit loop stop.");
    loop_state.store(Loop::STOP);
}

void NATSHandler::post(Task task)
{
    if (loop_state.load() != Loop::INITIALIZED && loop_state.load() != Loop::RUN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not post task to event loop: event loop stopped");

    std::lock_guard<std::mutex> lock(tasks_mutex);
    tasks.push(std::move(task));
}

std::future<NATSConnectionPtr> NATSHandler::createConnection(const NATSConfiguration & configuration, std::uint64_t connect_attempts_count)
{
    auto promise = std::make_shared<std::promise<NATSConnectionPtr>>();

    auto connect_future = promise->get_future();
    post(
        [this, &configuration, connect_attempts_count, connect_promise = std::move(promise)]()
        {
            try
            {
                if (loop_state.load() != Loop::RUN)
                    throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to NATS: Event loop stopped");

                for (size_t i = 0; i < connect_attempts_count; ++i)
                {
                    auto connection = std::make_shared<NATSConnection>(configuration, log, createOptions());
                    if (!connection->connect())
                    {
                        LOG_DEBUG(
                            log,
                            "Connect to {} attempt #{} failed, error: {}. Reconnecting...",
                            connection->connectionInfoForLog(), i + 1, nats_GetLastError(nullptr));
                        continue;
                    }
                    connect_promise->set_value(connection);

                    return;
                }

                throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to Nats last error: {}", nats_GetLastError(nullptr));
            }
            catch (...)
            {
                tryLogCurrentException(log);
                try
                {
                    connect_promise->set_exception(std::current_exception());
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                }
            }
        });

    return connect_future;
}

NATSOptionsPtr NATSHandler::createOptions()
{
    natsOptions * options = nullptr;
    auto er = natsOptions_Create(&options);
    if (er)
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
    if (er)
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
