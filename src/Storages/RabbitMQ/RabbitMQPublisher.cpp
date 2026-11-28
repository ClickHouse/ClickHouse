#include <Storages/RabbitMQ/RabbitMQPublisher.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <boost/algorithm/string/split.hpp>


namespace DB
{

static const auto RETURNED_LIMIT = 50000;

/// How long a step waits for new messages to publish or for confirms, when there is nothing else to do.
static constexpr UInt64 WAIT_MS = 100;

/// The delay before connecting again after a failed attempt.
static constexpr UInt64 RECONNECT_DELAY_MS = 200;

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_RABBITMQ;
    extern const int LOGICAL_ERROR;
}

RabbitMQPublisher::RabbitMQPublisher(
    const RabbitMQConfiguration & configuration_,
    const Names & routing_keys_,
    const String & exchange_name_,
    AMQP::ExchangeType exchange_type_,
    size_t channel_id_base_,
    bool persistent_,
    LoggerPtr log_)
    : connection(configuration_, log_)
    , routing_keys(routing_keys_)
    , exchange_name(exchange_name_)
    , exchange_type(exchange_type_)
    , channel_id_base(std::to_string(channel_id_base_))
    , persistent(persistent_)
    , log(log_)
    , returned(RETURNED_LIMIT)
{
    if (exchange_type == AMQP::ExchangeType::Headers)
    {
        for (const auto & header : routing_keys)
        {
            std::vector<String> matching;
            boost::split(matching, header, [](char c){ return c == '='; });
            key_arguments.addString(matching[0], matching[1]);
        }
    }
}

void RabbitMQPublisher::connect()
{
    try
    {
        setupChannel();
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot open a producer channel to RabbitMQ {}: {}",
            connection.connectionInfoForLog(), getCurrentExceptionMessage(false));
    }
}

void RabbitMQPublisher::setupChannel()
{
    channel_ready = false;
    connection.connect();

    /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails,
     * onNack() is received. If persistent == false, message is confirmed the moment it is enqueued. First option is two times
     * slower than the second, so default is second and the first is turned on in table setting.
     *
     * "Publisher confirms" are implemented similar to strategy#3 here https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
     */
    connection.get().confirmSelect();

    /// Delivery tags are scoped per channel.
    delivery_tag = 0;
    channel_id = channel_id_base + "_" + std::to_string(channel_id_counter++);
    channel_ready = true;
    LOG_DEBUG(log, "Producer's channel {} is ready", channel_id);
}

void RabbitMQPublisher::onChannelError()
{
    LOG_ERROR(log, "Producer's channel {} error: {}", channel_id, getCurrentExceptionMessage(false));
    channel_ready = false;

    /* Save records that have not received ack/nack from server before channel closure. They are removed and pushed back again once
     * they are republished because after channel recovery they will acquire new delivery tags, so all previous records become invalid
     */
    for (const auto & record : delivery_record)
        if (!returned.push(record.second))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

    LOG_DEBUG(log, "Producer on channel {} hasn't confirmed {} messages", channel_id, delivery_record.size());

    delivery_record.clear();
    delivery_tag = 0;
}

void RabbitMQPublisher::removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish)
{
    /// The delivery tag comes from the broker, so it may refer to a message that is not (or no longer) in `delivery_record`.
    if (multiple)
    {
        /// If multiple is true, then all delivery tags up to and including current are confirmed (with ack or nack).
        auto record_iter = delivery_record.upper_bound(received_delivery_tag);

        if (republish)
            for (auto record = delivery_record.begin(); record != record_iter; ++record)
                if (!returned.push(record->second))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

        /// Delete the records even in case when republished because new delivery tags will be assigned by the server.
        delivery_record.erase(delivery_record.begin(), record_iter);
    }
    else
    {
        auto record_iter = delivery_record.find(received_delivery_tag);
        if (record_iter == delivery_record.end())
        {
            LOG_WARNING(log, "Received {} for unknown delivery tag {}, ignoring it", republish ? "nack" : "ack", received_delivery_tag);
            return;
        }

        if (republish)
            if (!returned.push(record_iter->second))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

        delivery_record.erase(record_iter);
    }
}

void RabbitMQPublisher::publish(Payloads & messages, bool republishing, bool wait_for_messages)
{
    auto & channel = connection.get();
    Payload payload;
    size_t num_published = 0;

    /* It is important to make sure that delivery_record.size() is never bigger than returned.size(), i.e. number if unacknowledged
     * messages cannot exceed returned.size(), because they all might end up there
     */
    while (num_published < BATCH && delivery_record.size() < RETURNED_LIMIT)
    {
        /// Wait only for the first message, the rest of the batch is what is already there.
        const bool popped = (wait_for_messages && num_published == 0) ? messages.tryPop(payload, WAIT_MS) : messages.tryPop(payload);
        if (!popped)
            break;

        /// The message is recorded right away, so that it is published again if anything below fails.
        ++delivery_tag;
        delivery_record.insert(delivery_record.end(), {delivery_tag, payload});
        ++num_published;

        AMQP::MessageProperties properties;

        /// if headers exchange is used, routing keys are added here via headers, if not - it is just empty
        properties.headers = key_arguments;

        /* There is the case when connection is lost in the period after some messages were published and before ack/nack was sent by the
         * server, then it means that publisher will never know whether those messages were delivered or not, and therefore those records
         * that received no ack/nack before connection loss will be republished, so there might be duplicates. To
         * let consumer know that received message might be a possible duplicate - a "republished" field is added to message metadata
         */
        properties.headers.addString("republished", std::to_string(republishing));

        /* Adding here a messageID property to message metadata. Since RabbitMQ does not guarantee exactly-once delivery, then on the
         * consumer side "republished" field of message metadata can be checked and, if it set to 1, consumer might also check "messageID"
         * property. This way detection of duplicates is guaranteed
         */
        properties.message_id = std::to_string(payload.id);

        /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse
        if (persistent)
            properties.delivery_mode = 2;

        String routing_key;
        if (exchange_type == AMQP::ExchangeType::ConsistentHash)
            routing_key = std::to_string(delivery_tag - 1);
        else if (exchange_type != AMQP::ExchangeType::Headers)
            routing_key = routing_keys[0];

        channel.publish(exchange_name, routing_key, payload.message, properties);
    }

    if (num_published)
        channel.flush();
}

void RabbitMQPublisher::processConfirms(UInt64 timeout_ms)
{
    for (const auto & confirm : connection.get().receiveConfirms(Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(timeout_ms) * 1000)))
        removeRecord(confirm.delivery_tag, confirm.multiple, /* republish */ !confirm.ack);
}

String RabbitMQPublisher::publishStep(Payloads & payloads)
{
    if (!channel_ready)
    {
        try
        {
            setupChannel();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot restore the producer connection");
            sleepForMilliseconds(RECONNECT_DELAY_MS);
            return {};
        }
    }

    try
    {
        const size_t pending_before = delivery_record.size();

        /* Publish main payloads only when there are no returned messages. This way it is ensured that returned messages are republished
         * as fast as possible and no new publishes are made before returned messages are handled
         */
        if (!returned.empty())
            publish(returned, /* republishing */ true, /* wait_for_messages */ false);
        else
            publish(payloads, /* republishing */ false, /* wait_for_messages */ delivery_record.empty());

        /// Wait for confirms only if nothing was published: otherwise there are probably more messages to publish.
        const bool published = delivery_record.size() != pending_before;
        processConfirms(!published && !delivery_record.empty() ? WAIT_MS : 0);
    }
    catch (...) /// Ok: onChannelError logs the exception
    {
        /// The broker closes only the channel when it rejects a publish, e.g. NOT_FOUND for a missing exchange.
        /// A new channel is rejected the same way.
        const String channel_close_reason = connection.get().getChannelCloseReason();
        onChannelError();
        if (!channel_close_reason.empty())
            return fmt::format("Cannot publish to RabbitMQ exchange '{}': {}", exchange_name, channel_close_reason);
    }

    return {};
}

bool RabbitMQPublisher::isReusable()
{
    return channel_ready && !hasPendingDeliveries() && connection.isConnected();
}

void RabbitMQPublisher::disconnect()
{
    connection.disconnect();
}

}
