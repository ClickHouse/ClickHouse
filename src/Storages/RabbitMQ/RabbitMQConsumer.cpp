#include <Storages/RabbitMQ/RabbitMQConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <base/EnumReflection.h>


namespace DB
{

/// Waiting for messages is done in slices of this length, to notice that the consumer is stopped or the read is cancelled.
static constexpr UInt64 WAIT_SLICE_MS = 100;

/// Bounds the deliveries buffered in memory by `checkConnection`.
static constexpr size_t MAX_READ_AHEAD_DELIVERIES = 100000;

RabbitMQConsumer::RabbitMQConsumer(
        const RabbitMQConfiguration & configuration_,
        const std::vector<String> & queues_,
        size_t channel_id_base_,
        const String & channel_base_,
        LoggerPtr log_)
        : connection(configuration_, log_)
        , queues(queues_)
        , channel_base(channel_base_)
        , channel_id_base(channel_id_base_)
        , log(log_)
{
}

void RabbitMQConsumer::stop()
{
    stopped = true;
}

void RabbitMQConsumer::setError(const String & what)
{
    LOG_ERROR(log, "Consumer failed on channel {} while {}: {}", channel_id, what, getCurrentExceptionMessage(false));
    state = State::ERROR;
}

void RabbitMQConsumer::updateChannel()
{
    const bool is_reconnect = channel_id_counter > 0;
    state = State::ERROR;
    last_commited_delivery_tag = 0;
    /// A delivery of the old channel can no longer be acked and the broker redelivers it.
    pending.reset();

    try
    {
        connection.connect();
        for (const auto & queue_name : queues)
        {
            connection.get().consume(queue_name);
            LOG_TRACE(log, "Consumer {} is subscribed to queue {}", channel_id_base, queue_name);
        }
    }
    catch (...) /// Ok: setError logs the exception
    {
        setError("subscribing to the queues");
        return;
    }

    /// 1. channel_id_base - indicates current consumer buffer.
    /// 2. channel_id_counter - indicates serial number of created channel for current buffer
    ///    (incremented on each channel update).
    /// 3. channel_base is to guarantee that channel_id is unique for each table.
    channel_id = fmt::format("{}_{}_{}", channel_id_base, channel_id_counter++, channel_base);
    state = State::OK;

    if (is_reconnect)
        LOG_DEBUG(log, "Connection restored, consumer channel {} is ready", channel_id);
    else
        LOG_TRACE(log, "Channel {} is successfully created", channel_id);
}

bool RabbitMQConsumer::receive(UInt64 timeout_ms)
{
    if (pending)
        return true;
    if (state != State::OK)
        return false;

    try
    {
        pending = connection.get().receiveDelivery(Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(timeout_ms) * 1000));
    }
    catch (...) /// Ok: setError logs the exception
    {
        setError("receiving messages");
        return false;
    }

    /// Empty messages are skipped, as before.
    if (pending && pending->body.empty())
        pending.reset();

    return pending.has_value();
}

bool RabbitMQConsumer::hasPendingMessages()
{
    return !stopped && receive(0);
}

void RabbitMQConsumer::waitForMessages(std::optional<uint64_t> timeout_ms, std::function<bool()> is_cancelled)
{
    const UInt64 timeout = timeout_ms.value_or(SANITY_TIMEOUT);
    Stopwatch watch;
    while (!stopped && !(is_cancelled && is_cancelled()) && state == State::OK)
    {
        const UInt64 elapsed = watch.elapsedMilliseconds();
        if (elapsed >= timeout)
            return;
        if (receive(std::min(WAIT_SLICE_MS, timeout - elapsed)))
            return;
    }
}

bool RabbitMQConsumer::checkConnection()
{
    if (state != State::OK)
        return false;

    try
    {
        connection.get().receiveAvailable(MAX_READ_AHEAD_DELIVERIES);
    }
    catch (...) /// Ok: setError logs the exception
    {
        setError("checking the connection");
    }
    return state == State::OK;
}

void RabbitMQConsumer::heartbeat()
{
    if (state != State::OK)
        return;

    try
    {
        connection.get().sendHeartbeatIfNeeded();
    }
    catch (...) /// Ok: setError logs the exception
    {
        setError("sending a heartbeat");
    }
}

ReadBufferPtr RabbitMQConsumer::consume()
{
    if (stopped || !receive(0))
        return nullptr;

    current = MessageData{
        .message = std::move(pending->body),
        .message_id = std::move(pending->message_id),
        .timestamp = pending->timestamp,
        .redelivered = pending->redelivered,
        .delivery_tag = pending->delivery_tag,
        .channel_id = channel_id};
    pending.reset();

    return std::make_shared<ReadBufferFromMemory>(current.message);
}

bool RabbitMQConsumer::ackMessages(const CommitInfo & commit_info)
{
    if (state != State::OK)
    {
        LOG_TEST(log, "State is {}, will not ack messages", magic_enum::enum_name(state));
        return false;
    }

    /// Do not send ack to server if message's channel is not the same as
    /// current running channel because delivery tags are scoped per channel,
    /// so if channel fails, all previous delivery tags become invalid.
    if (commit_info.channel_id != channel_id)
    {
        LOG_TEST(log, "Channel ID changed {} -> {}, will not ack messages", commit_info.channel_id, channel_id);
        return false;
    }

    try
    {
        for (const auto & delivery_tag : commit_info.failed_delivery_tags)
        {
            connection.get().reject(delivery_tag, /* requeue */ false);
            LOG_TRACE(log, "Consumer rejected message with deliveryTag {} on channel {}", delivery_tag, channel_id);
        }

        /// Duplicate ack?
        if (commit_info.delivery_tag > last_commited_delivery_tag)
        {
            connection.get().ack(commit_info.delivery_tag, /* multiple */ true);
            last_commited_delivery_tag = commit_info.delivery_tag;

            LOG_TRACE(
                log, "Consumer committed messages with deliveryTags up to {} on channel {}",
                last_commited_delivery_tag, channel_id);

            return true;
        }
    }
    catch (...) /// Ok: setError logs the exception
    {
        setError("acknowledging messages");
        return false;
    }

    if (commit_info.delivery_tag)
    {
        LOG_ERROR(
            log,
            "Did not commit messages for {}:{}, (current commit point {}:{})",
            commit_info.channel_id, commit_info.delivery_tag,
            channel_id, last_commited_delivery_tag);
    }

    return false;
}

bool RabbitMQConsumer::nackMessages(const CommitInfo & commit_info, bool requeue)
{
    const char * verb = requeue ? "requeue" : "nack";

    if (state != State::OK)
    {
        LOG_TEST(log, "State is {}, will not {} messages", magic_enum::enum_name(state), verb);
        return false;
    }

    if (commit_info.channel_id != channel_id)
    {
        LOG_TEST(log, "Channel ID changed {} -> {}, will not {} messages", commit_info.channel_id, channel_id, verb);
        return false;
    }

    try
    {
        for (const auto & delivery_tag : commit_info.failed_delivery_tags)
        {
            connection.get().reject(delivery_tag, requeue);
            LOG_TRACE(log, "Consumer did {} message with deliveryTag {} on channel {}", verb, delivery_tag, channel_id);
        }

        /// Nothing to reject.
        if (!commit_info.delivery_tag || commit_info.delivery_tag <= last_commited_delivery_tag)
        {
            LOG_TEST(log, "Delivery tag is {}, last committed delivery tag: {}, will not {} messages",
                     commit_info.delivery_tag, last_commited_delivery_tag, verb);
            return false;
        }

        connection.get().nack(commit_info.delivery_tag, /* multiple */ true, requeue);
    }
    catch (...) /// Ok: setError logs the exception
    {
        setError(fmt::format("trying to {} messages", verb));
        return false;
    }

    LOG_TRACE(
        log, "Consumer did {} messages with deliveryTags from {} to {} on channel {}",
        verb, last_commited_delivery_tag, commit_info.delivery_tag, channel_id);

    return true;
}

void RabbitMQConsumer::closeConnections()
{
    connection.disconnect();
    pending.reset();
    state = State::NONE;
    LOG_TRACE(log, "Consumer channel {} is closed", channel_id);
}

}
