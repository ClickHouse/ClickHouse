#include <utility>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <boost/algorithm/string/split.hpp>
#include <common/logger_useful.h>
#include "Poco/Timer.h"
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static const auto QUEUE_SIZE = 50000;

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        ChannelPtr setup_channel_,
        HandlerPtr event_handler_,
        const String & exchange_name_,
        size_t channel_id_base_,
        const String & channel_base_,
        const String & queue_base_,
        Poco::Logger * log_,
        char row_delimiter_,
        bool hash_exchange_,
        size_t num_queues_,
        const String & deadletter_exchange_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , setup_channel(setup_channel_)
        , event_handler(event_handler_)
        , exchange_name(exchange_name_)
        , channel_base(channel_base_)
        , channel_id_base(channel_id_base_)
        , queue_base(queue_base_)
        , hash_exchange(hash_exchange_)
        , num_queues(num_queues_)
        , deadletter_exchange(deadletter_exchange_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
        , received(QUEUE_SIZE * num_queues)
{
    for (size_t queue_id = 0; queue_id < num_queues; ++queue_id)
        bindQueue(queue_id);

    consumer_channel->onReady([&]()
    {
        channel_id = std::to_string(channel_id_base) + "_" + std::to_string(channel_id_counter++) + "_" + channel_base;
        LOG_TRACE(log, "Channel {} is created", channel_id);

        consumer_channel->onError([&](const char * message)
        {
            LOG_ERROR(log, "Channel {} error: {}", channel_id, message);
            channel_error.store(true);
        });

        updateAckTracker(AckTracker());
        subscribe();

        channel_error.store(false);
    });
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::bindQueue(size_t queue_id)
{
    std::atomic<bool> binding_created = false;

    auto success_callback = [&](const std::string &  queue_name, int msgcount, int /* consumercount */)
    {
        queues.emplace_back(queue_name);
        LOG_DEBUG(log, "Queue {} is declared", queue_name);

        if (msgcount)
            LOG_TRACE(log, "Queue {} is non-empty. Non-consumed messaged will also be delivered", queue_name);

       /* Here we bind either to sharding exchange (consistent-hash) or to bridge exchange (fanout). All bindings to routing keys are
        * done between client's exchange and local bridge exchange. Binding key must be a string integer in case of hash exchange, for
        * fanout exchange it can be arbitrary.
        */
        setup_channel->bindQueue(exchange_name, queue_name, std::to_string(channel_id_base))
        .onSuccess([&]
        {
            binding_created = true;
        })
        .onError([&](const char * message)
        {
            throw Exception("Failed to create queue binding. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
        });
    };

    auto error_callback([&](const char * message)
    {
        throw Exception("Failed to declare queue. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
    });

    AMQP::Table queue_settings;
    if (!deadletter_exchange.empty())
    {
        queue_settings["x-dead-letter-exchange"] = deadletter_exchange;
    }

    /* The first option not just simplifies queue_name, but also implements the possibility to be able to resume reading from one
     * specific queue when its name is specified in queue_base setting.
     */
    const String queue_name = !hash_exchange ? queue_base : std::to_string(channel_id_base) + "_" + std::to_string(queue_id) + "_" + queue_base;
    setup_channel->declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);

    while (!binding_created)
    {
        iterateEventLoop();
    }
}


void ReadBufferFromRabbitMQConsumer::subscribe()
{
    for (const auto & queue_name : queues)
    {
        consumer_channel->consume(queue_name)
        .onSuccess([&](const std::string & /* consumer_tag */)
        {
            LOG_TRACE(log, "Consumer on channel {} is subscribed to queue {}", channel_id, queue_name);
        })
        .onReceived([&](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
        {
            if (message.bodySize())
            {
                String message_received = std::string(message.body(), message.body() + message.bodySize());
                if (row_delimiter != '\0')
                    message_received += row_delimiter;

                if (message.hasMessageID())
                    received.push({message_received, message.messageID(), redelivered, AckTracker(delivery_tag, channel_id)});
                else
                    received.push({message_received, "", redelivered, AckTracker(delivery_tag, channel_id)});
            }
        })
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Consumer failed on channel {}. Reason: {}", channel_id, message);
        });
    }
}


void ReadBufferFromRabbitMQConsumer::ackMessages()
{
    /* Delivery tags are scoped per channel, so if channel fails, then all previous delivery tags become invalid. Also this check ensures
     * that there is no data race with onReady callback in restoreChannel() (they can be called at the same time from different threads).
     * And there is no need to synchronize this method with updateAckTracker() as they are not supposed to be called at the same time.
     */
    if (channel_error.load())
        return;

    AckTracker record = last_inserted_record;

    /// Do not send ack to server if message's channel is not the same as current running channel.
    if (record.channel_id == channel_id && record.delivery_tag && record.delivery_tag > prev_tag && event_handler->connectionRunning())
    {
        consumer_channel->ack(record.delivery_tag, AMQP::multiple); /// Will ack all up to last tag starting from last acked.
        prev_tag = record.delivery_tag;

        LOG_TRACE(log, "Consumer acknowledged messages with deliveryTags up to {} on the channel {}", record.delivery_tag, channel_id);
    }
}


void ReadBufferFromRabbitMQConsumer::updateAckTracker(AckTracker record)
{
    /* This method can be called from readImpl and from channel->onError() callback, but channel_error check ensures that it is not done
     * at the same time, so no synchronization needed.
     */
    if (record.delivery_tag && channel_error.load())
        return;

    if (!record.delivery_tag)
        prev_tag = 0;

    last_inserted_record = record;
}


void ReadBufferFromRabbitMQConsumer::restoreChannel(ChannelPtr new_channel)
{
    consumer_channel = std::move(new_channel);
    consumer_channel->onReady([&]()
    {
        /* First number indicates current consumer buffer; second number indicates serial number of created channel for current buffer,
         * i.e. if channel fails - another one is created and its serial number is incremented; channel_base is to guarantee that
         * channel_id is unique for each table.
         */
        channel_id = std::to_string(channel_id_base) + "_" + std::to_string(channel_id_counter++) + "_" + channel_base;
        LOG_TRACE(log, "Channel {} is created", channel_id);

        consumer_channel->onError([&](const char * message)
        {
            LOG_ERROR(log, "Channel {} error: {}", channel_id, message);
            channel_error.store(true);
        });

        updateAckTracker(AckTracker());
        subscribe();

        channel_error.store(false);
    });
}


void ReadBufferFromRabbitMQConsumer::iterateEventLoop()
{
    event_handler->iterateLoop();
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (stopped || !allowed)
        return false;

    if (received.tryPop(current))
    {
        auto * new_position = const_cast<char *>(current.message.data());
        BufferBase::set(new_position, current.message.size(), 0);
        allowed = false;

        return true;
    }

    return false;
}

}
