#include <Storages/NATS/NATSProducer.h>

#include <atomic>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>


namespace DB
{

static const auto BATCH = 1000;
static const auto MAX_BUFFERED = 131072;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_STATE;
}

NATSProducer::NATSProducer(NATSConnectionPtr connection_, const String & subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , connection(std::move(connection_))
    , subject(subject_)
    , shutdown_called(shutdown_called_)
    , payloads(BATCH)
{
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
}

void NATSProducer::cancel() noexcept
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void NATSProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    if (!payloads.push(message))
        throw Exception(ErrorCodes::INVALID_STATE, "Could not push to payloads queue");
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
                throw Exception(ErrorCodes::INVALID_STATE, "Could not push to payloads queue");
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
    SCOPE_EXIT(nats_ReleaseThreadMemory());

    try
    {
        while (!payloads.isFinishedAndEmpty())
        {
            if (!connection->isConnected())
                std::this_thread::sleep_for(std::chrono::milliseconds(connection->getReconnectWait()));
            else
                publish();
        }

        while (!connection->isConnected() && !shutdown_called)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(connection->getReconnectWait()));
        }

        if (connection->isConnected() && natsConnection_Buffered(connection->getConnection()) > 0)
            natsConnection_Flush(connection->getConnection());
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);
}

}
