#include <Storages/NATS/NATSHandler.h>

#include <Core/Defines.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <adapters/libuv.h>
#include <base/scope_guard.h>

namespace DB
{

namespace Loop
{
    static const UInt8 CREATED = 0;
    static const UInt8 RUN = 1;
    static const UInt8 STOP = 2;
    static const UInt8 CLOSED = 3;
}

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int INVALID_STATE;
}

NATSHandler::NATSHandler(LoggerPtr log_)
    : log(log_)
    , loop_state(Loop::CREATED)
{
    auto error = uv_async_init(loop.getLoop(), &execute_tasks_scheduler, processTasks);
    if (error)
    {
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot create scheduler, received error {}", error);
    }

    execute_tasks_scheduler.data = this;
}

void NATSHandler::runLoop()
{
    {
        std::lock_guard lock(loop_state_mutex);
        if (loop_state != Loop::CREATED)
        {
            return;
        }

        natsLibuv_Init();
        natsLibuv_SetThreadLocalLoop(loop.getLoop());

        loop_state = Loop::RUN;
    }

    SCOPE_EXIT({
        nats_ReleaseThreadMemory();
        resetThreadLocalLoop();
    });

    LOG_DEBUG(log, "Background loop started");

    uv_run(loop.getLoop(), UV_RUN_DEFAULT);

    {
        std::lock_guard lock(loop_state_mutex);
        loop_state = Loop::CLOSED;
    }

    LOG_DEBUG(log, "Background loop ended");
}

void NATSHandler::processTasks(uv_async_t* scheduler)
{
    auto * self_ptr = static_cast<NATSHandler *>(scheduler->data);
    if (!self_ptr)
    {
        return;
    }
    self_ptr->executeTasks();

    std::lock_guard lock(self_ptr->loop_state_mutex);
    if (self_ptr->loop_state == Loop::STOP)
    {
        uv_stop(self_ptr->loop.getLoop());
    }
}

void NATSHandler::stopLoop()
{
    std::lock_guard lock(loop_state_mutex);

    if (loop_state != Loop::RUN)
    {
        return;
    }

    LOG_DEBUG(log, "Implicit loop stop");
    loop_state = Loop::STOP;

    uv_async_send(&execute_tasks_scheduler);
}

void NATSHandler::post(Task task)
{
    std::scoped_lock lock(loop_state_mutex, tasks_mutex);

    if (loop_state != Loop::CREATED && loop_state != Loop::RUN)
        throw Exception(ErrorCodes::INVALID_STATE, "Can not post task to event loop: event loop stopped");

    tasks.push(std::move(task));

    uv_async_send(&execute_tasks_scheduler);
}
void NATSHandler::executeTasks()
{
    std::queue<Task> executed_tasks;
    {
        std::lock_guard<std::mutex> lock(tasks_mutex);
        std::swap(executed_tasks, tasks);
    }

    while (!executed_tasks.empty())
    {
        auto task = std::move(executed_tasks.front());
        executed_tasks.pop();
        task();
    }
}

std::future<NATSConnectionPtr> NATSHandler::createConnection(const NATSConfiguration & configuration)
{
    auto promise = std::make_shared<std::promise<NATSConnectionPtr>>();

    auto connect_future = promise->get_future();
    post(
        [this, &configuration, connect_promise = std::move(promise)]()
        {
            try
            {
                for (UInt64 i = 0; i < configuration.max_connect_tries; ++i)
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

void NATSHandler::resetThreadLocalLoop()
{
    uv_key_set(&uvLoopThreadKey, nullptr);
}

}
