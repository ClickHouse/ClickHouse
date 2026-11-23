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

/// A publish failing with any other status (e.g. a timeout during a leader change of the stream) is retried with backoff.
static constexpr size_t MAX_PUBLISH_RETRIES = 5;
static constexpr auto PUBLISH_RETRY_INITIAL_WAIT = std::chrono::milliseconds(100);

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
    cancelled = true;
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
    while (pending_payload || !payloads.empty())
    {
        if (!connection->isConnected() || natsConnection_Buffered(connection->getConnection()) > MAX_BUFFERED)
            break;

        if (!pending_payload)
        {
            String payload;
            if (!payloads.pop(payload))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");
            pending_payload = std::move(payload);
        }

        natsStatus status = publishMessage(*pending_payload);
        if (status != NATS_OK)
        {
            LOG_DEBUG(log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), getNATSLastError());
            /// A retry cannot succeed: the client rejects a message over max_payload before sending it, and NATS_ERR is
            /// the stream rejecting the message.
            if (status == NATS_NO_RESPONDERS)
                publish_error = "there is no JetStream stream for this subject";
            else if (status == NATS_MAX_PAYLOAD || status == NATS_ERR || pending_payload_retries == MAX_PUBLISH_RETRIES || cancelled)
                publish_error = fmt::format("{}. Last error message: {}", natsStatus_GetText(status), getNATSLastError());
            else
            {
                std::this_thread::sleep_for(PUBLISH_RETRY_INITIAL_WAIT * (1 << pending_payload_retries));
                ++pending_payload_retries;
            }
            break;
        }
        pending_payload.reset();
        pending_payload_retries = 0;
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
        while (publish_error.empty() && (pending_payload || !payloads.isFinishedAndEmpty()))
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
        publish_error = getCurrentExceptionMessage(false);
        payloads.finish();
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);
}

}
