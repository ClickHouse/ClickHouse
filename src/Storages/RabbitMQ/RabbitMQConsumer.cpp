#include <utility>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/logger_useful.h>
#include <Poco/Timer.h>
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RabbitMQConsumer::RabbitMQConsumer(
        RabbitMQHandler & event_handler_,
        std::vector<String> & queues_,
        size_t channel_id_base_,
        const String & channel_base_,
        LoggerPtr log_,
        uint32_t queue_size_)
        : event_handler(event_handler_)
        , queues(queues_)
        , channel_base(channel_base_)
        , channel_id_base(channel_id_base_)
        , log(log_)
        , received(queue_size_)
{
}

void RabbitMQConsumer::stop()
{
    stopped = true;
    cv.notify_one();
    notifyReceivedSpace();
}

void RabbitMQConsumer::notifyReceivedSpace()
{
    /// Lock and unlock before notifying, so that a callback that has just seen a full queue under the mutex is already waiting.
    {
        std::lock_guard lock(channel_mutex);
    }
    received_space_cv.notify_all();
}

void RabbitMQConsumer::wakeUp()
{
    std::lock_guard lock(mutex);
    cv.notify_all();
}

void RabbitMQConsumer::closeConnections()
{
    if (!consumer_channel)
        return;

    auto closing = std::make_shared<ChannelCloseState>();
    close_state = closing;

    consumer_channel->close()
    .onSuccess([this, closing]()
    {
        if (closing->abandoned)
            return;
        LOG_TRACE(log, "Consumer channel {} is closed", channel_id);
        closing->completed = true;
        event_handler.stopBlockingLoop();
    })
    .onError([this, closing](const char * message)
    {
        if (closing->abandoned)
            return;
        LOG_ERROR(log, "Failed to close consumer channel {}: {}", channel_id, message);
        /// An error also completes the wait: a channel that could not be closed is not going to
        /// keep holding the queue, and treating it as outstanding would burn the whole timeout.
        closing->completed = true;
        event_handler.stopBlockingLoop();
    });
}

void RabbitMQConsumer::subscribe(UInt64 generation)
{
    for (const auto & queue_name : queues)
    {
        consumer_channel->consume(queue_name)
        .onSuccess([this, generation, &queue_name](const std::string & /* consumer_tag */)
        {
            std::lock_guard lock(channel_mutex);
            if (!isCurrentChannel(generation))
                return;
            LOG_TRACE(
                log, "Consumer on channel {} ({}/{}) is subscribed to queue {}",
                channel_id, subscriptions_num, queues.size(), queue_name);
        })
        .onReceived([this, generation](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
        {
            if (!message.bodySize())
                return;

            /// The generation is checked and the delivery is pushed under `channel_mutex`, so `updateChannel` cannot
            /// replace the channel and clear the queue in between. The mutex is released while waiting for space:
            /// the queue is drained only by the streaming thread, which may be about to call `updateChannel`.
            std::unique_lock lock(channel_mutex);
            received_space_cv.wait(lock, [&]
            {
                return !isCurrentChannel(generation) || stopped || received.size() < received.maxFill();
            });

            /// A late delivery of a replaced channel can no longer be acked, the broker redelivers it.
            /// The same holds for a delivery to a stopped consumer: it is not going to be read.
            if (stopped || !isCurrentChannel(generation) || received.size() >= received.maxFill())
                return;

            MessageData result{
                .message = std::string(message.body(), message.body() + message.bodySize()),
                .message_id = message.hasMessageID() ? message.messageID() : "",
                .timestamp = message.hasTimestamp() ? message.timestamp() : 0,
                .redelivered = redelivered,
                .delivery_tag = delivery_tag,
                .channel_id = channel_id};

            /// Only this callback pushes and it holds the mutex, so there is space.
            if (!received.tryPush(std::move(result)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

            lock.unlock();
            cv.notify_one();
        })
        .onError([this, generation](const char * message)
        {
            std::lock_guard lock(channel_mutex);
            if (!isCurrentChannel(generation))
                return;

            /* End up here either if channel ends up in an error state (then there will be resubscription)
             * or consume call error, which arises from queue settings mismatch or queue level error,
             * which should not happen as no one else is supposed to touch them
             */
            LOG_ERROR(log, "Consumer failed on channel {}. Reason: {}", channel_id, message);
            state = State::ERROR;
        });
    }
}


bool RabbitMQConsumer::ackMessages(const CommitInfo & commit_info)
{
    if (state != State::OK)
    {
        LOG_TEST(log, "State is {}, will not ack messages", magic_enum::enum_name(state.load(std::memory_order_relaxed)));
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

    for (const auto & delivery_tag : commit_info.failed_delivery_tags)
    {
        if (consumer_channel->reject(delivery_tag))
            LOG_TRACE(
                log, "Consumer rejected message with deliveryTag {} on channel {}",
                delivery_tag, channel_id);
        else
            LOG_WARNING(
                log, "Failed to reject message with deliveryTag {} on channel {}",
                delivery_tag, channel_id);
    }

    /// Duplicate ack?
    if (commit_info.delivery_tag > last_commited_delivery_tag
        && consumer_channel->ack(commit_info.delivery_tag, AMQP::multiple))
    {
        last_commited_delivery_tag = commit_info.delivery_tag;

        LOG_TRACE(
            log, "Consumer committed messages with deliveryTags up to {} on channel {}",
            last_commited_delivery_tag, channel_id);

        return true;
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
    const int flags = requeue ? (AMQP::multiple | AMQP::requeue) : AMQP::multiple;
    const char * verb = requeue ? "requeue" : "nack";

    if (state != State::OK)
    {
        LOG_TEST(log, "State is {}, will not {} messages", magic_enum::enum_name(state.load(std::memory_order_relaxed)), verb);
        return false;
    }

    if (commit_info.channel_id != channel_id)
    {
        LOG_TEST(log, "Channel ID changed {} -> {}, will not {} messages", commit_info.channel_id, channel_id, verb);
        return false;
    }

    const int failed_flags = requeue ? AMQP::requeue : 0;
    for (const auto & delivery_tag : commit_info.failed_delivery_tags)
    {
        if (consumer_channel->reject(delivery_tag, failed_flags))
            LOG_TRACE(
                log, "Consumer did {} message with deliveryTag {} on channel {}",
                verb, delivery_tag, channel_id);
        else
            LOG_WARNING(
                log, "Failed to {} message with deliveryTag {} on channel {}",
                verb, delivery_tag, channel_id);
    }

    /// Nothing to reject.
    if (!commit_info.delivery_tag || commit_info.delivery_tag <= last_commited_delivery_tag)
    {
        LOG_TEST(log, "Delivery tag is {}, last committed delivery tag: {}, will not {} messages",
                 commit_info.delivery_tag, last_commited_delivery_tag, verb);
        return false;
    }

    if (consumer_channel->reject(commit_info.delivery_tag, flags))
    {
        LOG_TRACE(
            log, "Consumer did {} messages with deliveryTags from {} to {} on channel {}",
            verb, last_commited_delivery_tag, commit_info.delivery_tag, channel_id);

        return true;
    }

    LOG_ERROR(
        log,
        "Failed to {} messages for {}:{}, (current commit point {}:{})",
        verb, commit_info.channel_id, commit_info.delivery_tag,
        channel_id, last_commited_delivery_tag);

    return false;
}

void RabbitMQConsumer::updateChannel(RabbitMQConnection & connection)
{
    UInt64 generation = 0;
    {
        /// A callback of the old channel either completes before this or sees the new generation and does nothing,
        /// also if it is still running in the background loop, which is not waited for.
        std::lock_guard lock(channel_mutex);
        state = State::INITIALIZING;
        last_commited_delivery_tag = 0;
        /// Abandon the callbacks of the old channel first, so that a late delivery cannot refill the queue after it is cleared.
        generation = ++channel_generation;
        /// Deliveries of the old channel can no longer be acked and the broker redelivers them.
        /// Keeping them would insert duplicates and mix delivery tags of two channels in one commit.
        received.clear();
    }
    /// Wake a callback of the old channel that waits for space in the queue.
    received_space_cv.notify_all();

    consumer_channel = connection.createChannel();
    consumer_channel->onReady([this, generation]()
    {
        std::lock_guard lock(channel_mutex);
        if (!isCurrentChannel(generation))
            return;

        try
        {
            /// 1. channel_id_base - indicates current consumer buffer.
            /// 2. channel_id_couner - indicates serial number of created channel for current buffer
            ///    (incremented on each channel update).
            /// 3. channel_base is to guarantee that channel_id is unique for each table.
            channel_id = fmt::format("{}_{}_{}", channel_id_base, channel_id_counter++, channel_base);

            LOG_TRACE(log, "Channel {} is successfully created", channel_id);

            subscriptions_num = 0;
            subscribe(generation);

            state = State::OK;
        }
        catch (...)
        {
            state = State::ERROR;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    });

    consumer_channel->onError([this, generation](const char * message)
    {
        std::lock_guard lock(channel_mutex);
        if (!isCurrentChannel(generation))
            return;

        LOG_ERROR(
            log, "Channel {} received an error: {} (usable: {}, connected: {})",
            channel_id, message, consumer_channel->usable(), consumer_channel->connected());

        if (!consumer_channel->usable() || !consumer_channel->connected())
        {
            state = State::ERROR;
        }
    });
}


bool RabbitMQConsumer::needChannelUpdate()
{
    chassert(consumer_channel);
    return state == State::ERROR;
}


ReadBufferPtr RabbitMQConsumer::consume()
{
    if (stopped || !received.tryPop(current))
        return nullptr;

    notifyReceivedSpace();

    return std::make_shared<ReadBufferFromMemory>(current.message);
}

}
