#include <Storages/RabbitMQ/RabbitMQProducer.h>

#include <Core/Block.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <amqpcpp.h>
#include <boost/algorithm/string/split.hpp>
#include <chrono>
#include <thread>
#include <atomic>


namespace DB
{

static const auto BATCH = 1000;
static const auto RETURNED_LIMIT = 50000;
static const auto FINISH_PRODUCER_NUM_TRIES = 50;

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_RABBITMQ;
    extern const int LOGICAL_ERROR;
}

RabbitMQProducer::RabbitMQProducer(
    const RabbitMQConfiguration & configuration_,
    const Names & routing_keys_,
    const String & exchange_name_,
    AMQP::ExchangeType exchange_type_,
    size_t channel_id_base_,
    bool persistent_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , connection(configuration_, log_)
    , routing_keys(routing_keys_)
    , exchange_name(exchange_name_)
    , exchange_type(exchange_type_)
    , channel_id_base(std::to_string(channel_id_base_))
    , persistent(persistent_)
    , shutdown_called(shutdown_called_)
    , payloads(BATCH)
    , returned(RETURNED_LIMIT)
{
}

void RabbitMQProducer::initialize()
{
    LOG_TRACE(log, "Initializing producer");

    if (connection.connect())
        setupChannel();
    else
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to RabbitMQ {}", connection.connectionInfoForLog());

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

void RabbitMQProducer::stopProducingTask()
{
    payloads.finish();
}

void RabbitMQProducer::finishImpl()
{
    connection.disconnect();
}

void RabbitMQProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    Payload payload;
    payload.message = message;
    payload.id = ++payload_counter;
    LOG_TEST(log, "Pushing message with id {}", payload.id);
    if (!payloads.push(std::move(payload)))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
}

void RabbitMQProducer::setupChannel()
{
    producer_channel = connection.createChannel();
    LOG_TRACE(log, "Created a producer channel");

    producer_channel->onError([&](const char * message)
    {
        LOG_ERROR(log, "Producer's channel {} error: {}", channel_id, message);

        /// Channel is not usable anymore. (https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/36#issuecomment-125112236)
        producer_channel->close();

        /* Save records that have not received ack/nack from server before channel closure. They are removed and pushed back again once
         * they are republished because after channel recovery they will acquire new delivery tags, so all previous records become invalid
         */
        for (const auto & record : delivery_record)
            if (!returned.push(record.second))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to returned queue");

        LOG_DEBUG(log, "Producer on channel {} hasn't confirmed {} messages, {} waiting to be published",
                channel_id, delivery_record.size(), payloads.size());

        /// Delivery tags are scoped per channel.
        delivery_record.clear();
        delivery_tag = 0;
        producer_ready = false;
    });

    producer_channel->onReady([&]()
    {
        channel_id = channel_id_base + "_" + std::to_string(channel_id_counter++);
        LOG_DEBUG(log, "Producer's channel {} is ready", channel_id);

        /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails,
         * onNack() is received. If persistent == false, message is confirmed the moment it is enqueued. First option is two times
         * slower than the second, so default is second and the first is turned on in table setting.
         *
         * "Publisher confirms" are implemented similar to strategy#3 here https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
         */
        producer_channel->confirmSelect()
        .onAck([&](uint64_t acked_delivery_tag, bool multiple)
        {
            removeRecord(acked_delivery_tag, multiple, false);
        })
        .onNack([&](uint64_t nacked_delivery_tag, bool multiple, bool /* requeue */)
        {
            removeRecord(nacked_delivery_tag, multiple, true);
        });
        producer_ready = true;
    });
}

void RabbitMQProducer::removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish)
{
    auto record_iter = delivery_record.find(received_delivery_tag);
    assert(record_iter != delivery_record.end());

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

void RabbitMQProducer::publish(Payloads & messages, bool republishing)
{
    Payload payload;

    /* It is important to make sure that delivery_record.size() is never bigger than returned.size(), i.e. number if unacknowledged
     * messages cannot exceed returned.size(), because they all might end up there
     */
    while (!messages.empty() && producer_channel->usable() && delivery_record.size() < RETURNED_LIMIT)
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
            producer_channel->publish(exchange_name, std::to_string(delivery_tag), envelope);
        }
        else if (exchange_type == AMQP::ExchangeType::headers)
        {
            producer_channel->publish(exchange_name, "", envelope);
        }
        else
        {
            producer_channel->publish(exchange_name, routing_keys[0], envelope);
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

void RabbitMQProducer::startProducingTaskLoop()
{
    LOG_TRACE(log, "Starting producer loop");

    while ((!payloads.isFinishedAndEmpty() || !returned.empty() || !delivery_record.empty()) && !shutdown_called.load())
    {
        /// If onReady callback is not received, producer->usable() will anyway return true,
        /// but must publish only after onReady callback.
        if (producer_ready)
        {
            /* Publish main paylods only when there are no returned messages. This way it is ensured that returned messages are republished
             * as fast as possible and no new publishes are made before returned messages are handled
             */
            if (!returned.empty() && producer_channel->usable())
                publish(returned, true);
            else if (!payloads.empty() && producer_channel->usable())
                publish(payloads, false);
        }

        iterateEventLoop();

        if (!producer_channel->usable())
        {
            if (connection.reconnect())
                setupChannel();
        }
    }

    int res = 0;
    size_t try_num = 0;
    while (++try_num <= FINISH_PRODUCER_NUM_TRIES && (res = iterateEventLoop()))
    {
        LOG_TEST(log, "Waiting for pending callbacks to finish (count: {}, try: {})", res, try_num);
    }

    producer_channel->close()
    .onSuccess([&]()
    {
        LOG_TRACE(log, "Successfully closed producer channel");
        connection.getHandler().stopLoop();
    })
    .onError([&](const char * message)
    {
        LOG_ERROR(log, "Failed to close producer channel: {}", message);
        connection.getHandler().stopLoop();
    });

    int active = connection.getHandler().startBlockingLoop();
    LOG_DEBUG(log, "Producer on channel completed (not finished events: {})", active);
}


int RabbitMQProducer::iterateEventLoop()
{
    return connection.getHandler().iterateLoop();
}

}
