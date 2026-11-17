#include <Storages/NATS/INATSProducer.h>

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
    extern const int CANNOT_CONNECT_NATS;
}

INATSProducer::INATSProducer(NATSConnectionPtr connection_, String subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , connection(std::move(connection_))
    , subject(std::move(subject_))
    , shutdown_called(shutdown_called_)
    , payloads(BATCH)
{
}

void INATSProducer::finishImpl()
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

    if (!publish_error.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot publish to NATS subject {}: {}", subject, publish_error);
}

void INATSProducer::cancel() noexcept
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

void INATSProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    if (!payloads.push(message))
    {
        /// The producing task has stopped on a publish error and closed the queue: `finish` throws it.
        finish();
        throw Exception(ErrorCodes::INVALID_STATE, "Could not push to payloads queue");
    }
}

void INATSProducer::publish()
{
    String payload;

    natsStatus status = {};
    while (!payloads.empty())
    {
        if (!connection->isConnected() || natsConnection_Buffered(connection->getConnection()) > MAX_BUFFERED)
            break;
        bool pop_result = payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");

        status = publishMessage(payload);
        if (status != NATS_OK)
        {
            LOG_DEBUG(log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), getNATSLastError());
            if (status == NATS_NO_RESPONDERS)
                publish_error = "there is no JetStream stream for this subject";
            else if (!payloads.pushFront(payload))
                publish_error = fmt::format("{}. Last error message: {}", natsStatus_GetText(status), getNATSLastError());
            break;
        }
    }
}

void INATSProducer::stopProducingTask()
{
    payloads.finish();
}

void INATSProducer::startProducingTaskLoop()
{
    SCOPE_EXIT(nats_ReleaseThreadMemory());

    try
    {
        while (publish_error.empty() && !payloads.isFinishedAndEmpty())
        {
            if (!connection->isConnected())
                std::this_thread::sleep_for(std::chrono::milliseconds(connection->getReconnectWait()));
            else
                publish();
        }

        if (!publish_error.empty())
        {
            /// Nothing pops `payloads` anymore, so `produce` must not wait for free space in it.
            payloads.finish();
            return;
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
