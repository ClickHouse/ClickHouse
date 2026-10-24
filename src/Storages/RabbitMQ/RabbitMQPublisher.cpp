#include <Storages/RabbitMQ/RabbitMQPublisher.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <amqpcpp.h>
#include <boost/algorithm/string/split.hpp>


namespace DB
{

static const auto RETURNED_LIMIT = 50000;

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
    if (exchange_type == AMQP::ExchangeType::headers)
    {
        for (const auto & header : routing_keys)
        {
            std::vector<String> matching;
            boost::split(matching, header, [](char c){ return c == '='; });
            key_arguments[matching[0]] = matching[1];
        }
    }
}

void RabbitMQPublisher::connect()
{
    if (!connection.connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to RabbitMQ {}", connection.connectionInfoForLog());

    setupChannel();

    /// The channel `onReady` and `onError` callbacks stop the loop. Wait here rather than in the producing task,
    /// so that the channel is ready even if the insert has no rows and the publisher can be reused.
    if (!connection.getHandler().startBlockingLoopWithTimeout(BLOCKING_LOOP_TIMEOUT_MS) || !channel_ready)
    {
        /// Close the connection while the channel callbacks can still safely refer to this object.
        disconnect(/* immediately */ true);
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot open a producer channel to RabbitMQ {}", connection.connectionInfoForLog());
    }
}

void RabbitMQPublisher::setupChannel()
{
    channel = connection.createChannel();
    LOG_TRACE(log, "Created a producer channel");

    channel->onError([&](const char * message)
    {
        LOG_ERROR(log, "Producer's channel {} error: {}", channel_id, message);

        /// Channel is not usable anymore. (https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/36#issuecomment-125112236)
        channel->close();

        /* Save records that have not received ack/nack from server before channel closure. They are removed and pushed back again once
         * they are republished because after channel recovery they will acquire new delivery tags, so all previous records become invalid
         */
        for (const auto & record : delivery_record)
            if (!returned.push(record.second))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

        LOG_DEBUG(log, "Producer on channel {} hasn't confirmed {} messages", channel_id, delivery_record.size());

        /// Delivery tags are scoped per channel.
        delivery_record.clear();
        delivery_tag = 0;
        channel_ready = false;
        connection.getHandler().stopBlockingLoop();
    });

    channel->onReady([&]()
    {
        channel_id = channel_id_base + "_" + std::to_string(channel_id_counter++);
        LOG_DEBUG(log, "Producer's channel {} is ready", channel_id);

        /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails,
         * onNack() is received. If persistent == false, message is confirmed the moment it is enqueued. First option is two times
         * slower than the second, so default is second and the first is turned on in table setting.
         *
         * "Publisher confirms" are implemented similar to strategy#3 here https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
         */
        channel->confirmSelect()
        .onAck([&](uint64_t acked_delivery_tag, bool multiple)
        {
            removeRecord(acked_delivery_tag, multiple, false);
        })
        .onNack([&](uint64_t nacked_delivery_tag, bool multiple, bool /* requeue */)
        {
            removeRecord(nacked_delivery_tag, multiple, true);
        });
        channel_ready = true;
        connection.getHandler().stopBlockingLoop();
    });
}

void RabbitMQPublisher::removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish)
{
    auto record_iter = delivery_record.find(received_delivery_tag);
    chassert(record_iter != delivery_record.end());

    if (multiple)
    {
        /// If multiple is true, then all delivery tags up to and including current are confirmed (with ack or nack).
        ++record_iter;

        if (republish)
            for (auto record = delivery_record.begin(); record != record_iter; ++record)
                if (!returned.push(record->second))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

        /// Delete the records even in case when republished because new delivery tags will be assigned by the server.
        delivery_record.erase(delivery_record.begin(), record_iter);
    }
    else
    {
        if (republish)
            if (!returned.push(record_iter->second))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

        delivery_record.erase(record_iter);
    }
}

void RabbitMQPublisher::publish(Payloads & messages, bool republishing)
{
    Payload payload;

    /* It is important to make sure that delivery_record.size() is never bigger than returned.size(), i.e. number if unacknowledged
     * messages cannot exceed returned.size(), because they all might end up there
     */
    while (!messages.empty() && channel->usable() && delivery_record.size() < RETURNED_LIMIT)
    {
        bool pop_result = messages.pop(payload);

        if (!pop_result)
            return;

        AMQP::Envelope envelope(payload.message.data(), payload.message.size());

        /// if headers exchange is used, routing keys are added here via headers, if not - it is just empty
        AMQP::Table message_settings = key_arguments;

        /* There is the case when connection is lost in the period after some messages were published and before ack/nack was sent by the
         * server, then it means that publisher will never know whether those messages were delivered or not, and therefore those records
         * that received no ack/nack before connection loss will be republished (see onError() callback), so there might be duplicates. To
         * let consumer know that received message might be a possible duplicate - a "republished" field is added to message metadata
         */
        message_settings["republished"] = std::to_string(republishing);
        envelope.setHeaders(message_settings);

        /* Adding here a messageID property to message metadata. Since RabbitMQ does not guarantee exactly-once delivery, then on the
         * consumer side "republished" field of message metadata can be checked and, if it set to 1, consumer might also check "messageID"
         * property. This way detection of duplicates is guaranteed
         */
        envelope.setMessageID(std::to_string(payload.id));

        /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse
        if (persistent)
            envelope.setDeliveryMode(2);

        if (exchange_type == AMQP::ExchangeType::consistent_hash)
        {
            channel->publish(exchange_name, std::to_string(delivery_tag), envelope);
        }
        else if (exchange_type == AMQP::ExchangeType::headers)
        {
            channel->publish(exchange_name, "", envelope);
        }
        else
        {
            channel->publish(exchange_name, routing_keys[0], envelope);
        }

        /// This is needed for "publisher confirms", which guarantees at-least-once delivery
        ++delivery_tag;
        delivery_record.insert(delivery_record.end(), {delivery_tag, payload});

        /// Need to break at some point to let event loop run, because no publishing actually happens before looping
        if (delivery_tag % BATCH == 0)
            break;
    }

    iterateEventLoop();
}

void RabbitMQPublisher::publishStep(Payloads & payloads)
{
    /// If onReady callback is not received, channel->usable() will anyway return true,
    /// but must publish only after onReady callback.
    if (channel_ready)
    {
        /* Publish main paylods only when there are no returned messages. This way it is ensured that returned messages are republished
         * as fast as possible and no new publishes are made before returned messages are handled
         */
        if (!returned.empty() && channel->usable())
            publish(returned, true);
        else if (!payloads.empty() && channel->usable())
            publish(payloads, false);
    }

    iterateEventLoop();

    if (!channel->usable())
    {
        if (connection.reconnect())
            setupChannel();
    }
}

bool RabbitMQPublisher::isReusable()
{
    iterateEventLoop();
    return connection.isConnected() && channel->usable() && channel_ready && !hasPendingDeliveries();
}

void RabbitMQPublisher::closeChannel()
{
    /// Shared flag so that a late-dispatched close() callback becomes a no-op if we stop waiting
    /// on timeout, mirroring the setup/cleanup paths in StorageRabbitMQ.
    auto abandoned = std::make_shared<bool>(false);
    channel->close()
    .onSuccess([this, abandoned]()
    {
        if (*abandoned)
            return;
        LOG_TRACE(log, "Successfully closed producer channel");
        connection.getHandler().stopBlockingLoop();
    })
    .onError([this, abandoned](const char * message)
    {
        if (*abandoned)
            return;
        LOG_ERROR(log, "Failed to close producer channel: {}", message);
        connection.getHandler().stopBlockingLoop();
    });

    /// If the connection is dead the close() callbacks never fire. Bound the wait so that INSERT
    /// producer shutdown (and therefore server shutdown) cannot block indefinitely.
    if (!connection.getHandler().startBlockingLoopWithTimeout(BLOCKING_LOOP_TIMEOUT_MS))
    {
        /// Make any late-dispatched callback above a no-op before we return.
        *abandoned = true;
        LOG_WARNING(log, "Timed out waiting for producer channel to close - connection may be dead");
    }
    else
        LOG_DEBUG(log, "Producer on channel completed");
}

void RabbitMQPublisher::disconnect(bool immediately)
{
    connection.disconnect(immediately);
}

int RabbitMQPublisher::iterateEventLoop()
{
    return connection.getHandler().iterateLoop();
}

}
