#include <Storages/NATS/NATSProducer.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>


namespace DB
{

static const auto BATCH = 1000;
static const auto MAX_BUFFERED = 131072;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

NATSProducer::NATSProducer(
    const NATSConfiguration & configuration_,
    BackgroundSchedulePool & broker_schedule_pool_,
    const String & subject_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , configuration(configuration_)
    , event_handler(log)
    , subject(subject_)
    , shutdown_called(shutdown_called_)
    , payloads(BATCH)
{
    looping_task = broker_schedule_pool_.createTask("NATSProducerLoopingTask", [this] { event_handler.runLoop(); });
    looping_task->deactivate();
}

void NATSProducer::initialize()
{
    looping_task->activateAndSchedule();

    try
    {
        auto connect_future = event_handler.createConnection(configuration);
        connection = connect_future.get();
    }
    catch (...)
    {
        tryLogCurrentException(log);

        event_handler.stopLoop();
        looping_task->deactivate();

        throw;
    }
}

void NATSProducer::finishImpl()
{
    try
    {
        if (connection)
        {
            if (connection->isConnected())
                natsConnection_Flush(connection->getConnection());

            connection->disconnect();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    event_handler.stopLoop();
    looping_task->deactivate();
}


void NATSProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    if (!payloads.push(message))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
}

void NATSProducer::publish()
{
    String payload;

    natsStatus status;
    while (!payloads.empty())
    {
        if (!connection->isConnected() || natsConnection_Buffered(connection->getConnection()) > MAX_BUFFERED)
            break;
        bool pop_result = payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");

        status = natsConnection_Publish(connection->getConnection(), subject.c_str(), payload.c_str(), static_cast<int>(payload.size()));

        if (status != NATS_OK)
        {
            LOG_DEBUG(log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), nats_GetLastError(nullptr));
            if (!payloads.pushFront(payload))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
            break;
        }
    }
}

void NATSProducer::stopProducingTask()
{
    payloads.finish();
}

void NATSProducer::startProducingTaskLoop()
{
    try
    {
        while (!payloads.isFinishedAndEmpty())
        {
            if (!connection->isConnected())
                std::this_thread::sleep_for(std::chrono::milliseconds(configuration.reconnect_wait));
            else
                publish();
        }

        while (!connection->isConnected() && !shutdown_called)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(configuration.reconnect_wait));
        }

        if (connection->isConnected() && natsConnection_Buffered(connection->getConnection()) > 0)
            natsConnection_Flush(connection->getConnection());
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);

    nats_ReleaseThreadMemory();
}

}
