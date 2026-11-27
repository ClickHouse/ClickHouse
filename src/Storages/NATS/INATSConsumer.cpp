#include <Storages/NATS/INATSConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
}

/// Waiting for messages is done in slices of this length, to notice that the consumer is stopped.
static constexpr UInt64 WAIT_SLICE_MS = 100;

INATSConsumer::INATSConsumer(
    const NATSConfiguration & configuration,
    const std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    const std::atomic<bool> & stopped_)
    : connection(configuration, log_)
    , subjects(subjects_)
    , log(log_)
    , stopped(stopped_)
    , queue_name(subscribe_queue_name)
{
}

void INATSConsumer::connect()
{
    if (connection.isConnected())
        return;

    /// Nothing that was received on the previous connection can be acknowledged anymore.
    dropBuffered();
    connection.connect();
}

bool INATSConsumer::maintainConnection(String & error)
{
    if (connection.isConnected())
        return true;

    const bool was_subscribed = subscribed;
    subscribed = false;
    try
    {
        LOG_INFO(log, "No connection to NATS {} (last error: {}), connecting", connection.connectionInfoForLog(), connection.lastErrorForLog());
        connect();
        if (was_subscribed)
            subscribe();
        return true;
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(false);
        LOG_WARNING(log, "Cannot connect to NATS {}: {}", connection.connectionInfoForLog(), error);
        return false;
    }
}

void INATSConsumer::subscribe()
{
    if (subscribed)
        return;

    connect();
    subscribeImpl();
    subscribed = true;
}

void INATSConsumer::unsubscribe()
{
    if (!subscribed)
        return;
    subscribed = false;

    if (!connection.isConnected())
        return;

    try
    {
        unsubscribeImpl();
        LOG_DEBUG(log, "Consumer unsubscribed");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot unsubscribe a NATS consumer");
    }
}

void INATSConsumer::disconnect()
{
    subscribed = false;
    connection.disconnect();
}

bool INATSConsumer::receive(UInt64 timeout_ms)
{
    if (pending)
        return true;
    if (!subscribed || connection.hasFailed())
        return false;

    try
    {
        while (auto message = receiveImpl(timeout_ms))
        {
            if (!message->data.empty())
            {
                pending = MessageData{.message = std::move(message->data), .subject = std::move(message->subject), .reply = std::move(message->reply)};
                return true;
            }

            /// An empty JetStream message is acknowledged, so that it is not redelivered.
            if (needsAck() && !message->reply.empty())
                getConnection().publish(message->reply, "", "+ACK");
            timeout_ms = 0;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot receive messages from NATS");
    }
    return false;
}

bool INATSConsumer::queueEmpty()
{
    return !receive(0);
}

ReadBufferPtr INATSConsumer::consume(std::optional<UInt64> timeout_ms)
{
    if (stopped)
        return nullptr;

    if (!timeout_ms)
    {
        if (!receive(0))
            return nullptr;
    }
    else
    {
        Stopwatch watch;
        while (!receive(std::min(WAIT_SLICE_MS, *timeout_ms - std::min(*timeout_ms, watch.elapsedMilliseconds()))))
        {
            if (stopped || watch.elapsedMilliseconds() >= *timeout_ms || !subscribed || connection.hasFailed())
                return nullptr;
        }
    }

    current = std::move(*pending);
    pending.reset();

    if (needsAck())
        consumed_messages.push_back(current.reply);

    return std::make_shared<ReadBufferFromMemory>(current.message);
}

void INATSConsumer::respond(std::vector<String> & messages, std::string_view payload)
{
    if (messages.empty())
        return;

    try
    {
        if (!connection.isConnected())
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "The connection is lost");
        for (const auto & reply : messages)
            getConnection().publish(reply, "", payload);
        getConnection().flush();
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to send {} for {} messages (the server may redeliver them): {}",
            payload, messages.size(), getCurrentExceptionMessage(false));
    }
    messages.clear();
}

void INATSConsumer::finishAndReturnUnprocessed(SkippedMessages skipped_messages_action)
{
    /// What was received but not consumed goes back too.
    std::vector<String> buffered;
    if (pending && !pending->reply.empty())
        buffered.push_back(pending->reply);
    pending.reset();
    if (subscribed && !connection.hasFailed())
    {
        try
        {
            auto drained = drainBeforeUnsubscribe();
            buffered.insert(buffered.end(), drained.begin(), drained.end());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot receive the remaining messages from NATS");
        }
    }

    /// Unsubscribe before the messages are returned: otherwise the server delivers them again at once to a pull request
    /// of this consumer that is still waiting, and they stay unacknowledged until the ACK deadline.
    unsubscribe();

    respond(consumed_messages, "-NAK");
    respond(skipped_messages, skipped_messages_action == SkippedMessages::Acknowledge ? "+ACK" : "-NAK");
    respond(buffered, "-NAK");
}

void INATSConsumer::dropBuffered()
{
    consumed_messages.clear();
    skipped_messages.clear();
    pending.reset();
}

void INATSConsumer::ackConsumed()
{
    std::vector<String> all = std::move(consumed_messages);
    all.insert(all.end(), std::make_move_iterator(skipped_messages.begin()), std::make_move_iterator(skipped_messages.end()));
    consumed_messages.clear();
    skipped_messages.clear();
    respond(all, "+ACK");
}

void INATSConsumer::markLastConsumedSkipped()
{
    /// Core NATS messages are not recorded.
    if (consumed_messages.empty())
        return;

    skipped_messages.push_back(std::move(consumed_messages.back()));
    consumed_messages.pop_back();
}

void INATSConsumer::dropConsumed()
{
    consumed_messages.clear();
    skipped_messages.clear();
}

void INATSConsumer::returnConsumed()
{
    respond(consumed_messages, "-NAK");
    respond(skipped_messages, "-NAK");
}

}
