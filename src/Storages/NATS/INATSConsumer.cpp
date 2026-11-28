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

    salvageReceived();
    connection.connect();
}

void INATSConsumer::addReceived(NATSClient::Message message)
{
    if (!message.data.empty())
    {
        buffered.push_back(MessageData{.message = std::move(message.data), .subject = std::move(message.subject), .reply = std::move(message.reply)});
        return;
    }

    /// An empty JetStream message is acknowledged, so that it is not redelivered.
    if (needsAck() && !message.reply.empty())
        skipped_messages.push_back(std::move(message.reply));
}

void INATSConsumer::salvageReceived()
{
    if (!subscribed)
        return;

    size_t num_salvaged = 0;
    for (auto & message : connection.takeReceived())
    {
        if (auto accepted = acceptMessage(std::move(message)))
        {
            addReceived(std::move(*accepted));
            ++num_salvaged;
        }
    }
    if (num_salvaged)
        LOG_DEBUG(log, "Kept {} messages received on the lost connection", num_salvaged);
}

bool INATSConsumer::maintainConnection(String & error)
{
    if (connection.isConnected())
        return true;

    const bool was_subscribed = subscribed;
    try
    {
        LOG_INFO(log, "No connection to NATS {} (last error: {}), connecting", connection.connectionInfoForLog(), connection.lastErrorForLog());
        connect();
        subscribed = false;
        if (was_subscribed)
            subscribe();
        return true;
    }
    catch (...)
    {
        subscribed = false;
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

    if (!connection.isConnected())
    {
        /// Nothing can be returned to the server, so what was received is kept, and inserted after connecting again.
        salvageReceived();
        subscribed = false;
        return;
    }
    subscribed = false;

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
    if (!buffered.empty())
        return true;
    if (!subscribed || connection.hasFailed())
        return false;

    try
    {
        while (auto message = receiveImpl(timeout_ms))
        {
            addReceived(std::move(*message));
            if (!buffered.empty())
                return true;
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

    current = std::move(buffered.front());
    buffered.pop_front();

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
    if (subscribed && !connection.isConnected())
    {
        /// Nothing can be returned over a lost connection. What was received but not consumed is kept and inserted after
        /// connecting again: the acknowledgements of JetStream messages work over any connection. The consumed messages
        /// belong to the owner that is finishing, and are not committed: they are redelivered after the ACK deadline,
        /// rather than acknowledged by the next owner of the consumer. Skipped messages are final only for `Acknowledge`.
        unsubscribe();
        consumed_messages.clear();
        if (skipped_messages_action != SkippedMessages::Acknowledge)
            skipped_messages.clear();
        return;
    }

    /// What was received but not consumed goes back too.
    std::vector<String> unprocessed;
    for (auto & message : buffered)
        if (!message.reply.empty())
            unprocessed.push_back(std::move(message.reply));
    buffered.clear();
    if (subscribed && !connection.hasFailed())
    {
        try
        {
            auto drained = drainBeforeUnsubscribe();
            unprocessed.insert(unprocessed.end(), drained.begin(), drained.end());
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
    respond(unprocessed, "-NAK");
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
