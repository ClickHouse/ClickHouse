#include <Storages/NATS/INATSProducer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <thread>


namespace DB
{

static const auto BATCH = 1000;

/// How long the producing task waits for the next message before it checks the connection and the shutdown again.
static constexpr UInt64 POP_TIMEOUT_MS = 100;

/// A publish failing with a retryable status (e.g. a timeout during a leader change of the stream) is retried with backoff.
static constexpr size_t MAX_PUBLISH_RETRIES = 5;
static constexpr auto PUBLISH_RETRY_INITIAL_WAIT = std::chrono::milliseconds(100);

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int ACCESS_DENIED;
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
    connection->disconnect();

    if (shutdown_called)
        throw Exception(ErrorCodes::ABORTED, "Table is detached");

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
        /// The producing task has stopped on a publish error or on table shutdown and closed the queue: `finish` throws the error.
        finish();
        throw Exception(ErrorCodes::INVALID_STATE, "Could not push to payloads queue");
    }
}

void INATSProducer::publish()
{
    for (size_t i = 0; i < BATCH; ++i)
    {
        if (!pending_payload)
        {
            String payload;
            /// Wait only for the first message, the rest of the batch is what is already there.
            const bool popped = i == 0 ? payloads.tryPop(payload, POP_TIMEOUT_MS) : payloads.tryPop(payload);
            if (!popped)
                break;
            pending_payload = std::move(payload);
        }

        String error;
        const auto status = publishMessage(*pending_payload, error);
        if (status == PublishStatus::Ok)
        {
            pending_payload.reset();
            pending_payload_retries = 0;
            continue;
        }

        LOG_DEBUG(log, "Something went wrong during publishing to NATS subject: {}", error);
        if (status == PublishStatus::Fail || pending_payload_retries == MAX_PUBLISH_RETRIES || cancelled)
        {
            publish_error = error;
        }
        else
        {
            std::this_thread::sleep_for(PUBLISH_RETRY_INITIAL_WAIT * (1 << pending_payload_retries));
            ++pending_payload_retries;
        }
        break;
    }

    /// Answers the `PING` of the server and notices an error it reported.
    getConnection().processIncoming();
}

void INATSProducer::stopProducingTask()
{
    payloads.finish();
}

void INATSProducer::startProducingTaskLoop()
{
    try
    {
        while (publish_error.empty() && !shutdown_called && (pending_payload || !payloads.isFinishedAndEmpty()))
        {
            if (!connection->isConnected())
            {
                if (cancelled)
                {
                    publish_error = fmt::format("the connection is lost: {}", connection->lastErrorForLog());
                    break;
                }

                LOG_DEBUG(log, "The connection to NATS is lost ({}), connecting again", connection->lastErrorForLog());
                try
                {
                    connection->connect();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Cannot connect to NATS");
                    std::this_thread::sleep_for(std::chrono::milliseconds(connection->getReconnectWait()));
                }
                continue;
            }

            try
            {
                publish();
            }
            catch (const Exception & e)
            {
                /// The server refused the publish: a retry cannot succeed.
                if (e.code() == ErrorCodes::ACCESS_DENIED)
                {
                    publish_error = e.message();
                    break;
                }
                /// The connection failed: the pending message is published again on a new connection.
                tryLogCurrentException(log, "Cannot publish to NATS");
            }
        }

        if (!publish_error.empty() || shutdown_called)
        {
            /// Nothing pops `payloads` anymore, so `produce` must not wait for free space in it.
            payloads.finish();
            return;
        }

        /// Wait until the server has received everything.
        connection->get().flush();
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
