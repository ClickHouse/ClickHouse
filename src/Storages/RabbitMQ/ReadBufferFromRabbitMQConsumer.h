#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <event2/event.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class ReadBufferFromRabbitMQConsumer : public ReadBuffer
{

public:
    ReadBufferFromRabbitMQConsumer(
            ChannelPtr consumer_channel_,
            RabbitMQHandler & eventHandler_,
            const String & exchange_name_,
            const String & routing_key_,
            const size_t channel_id_,
            Poco::Logger * log_,
            char row_delimiter_,
            const bool bind_by_id_,
            const bool hash_exchange_,
            const size_t num_queues_,
            const std::atomic<bool> & stopped_);

    ~ReadBufferFromRabbitMQConsumer() override;

    void allowNext() { allowed = true; } // Allow to read next message.
    void subscribeConsumer();

private:
    using Messages = std::vector<String>;

    ChannelPtr consumer_channel;
    RabbitMQHandler & eventHandler;

    const String & exchange_name;
    const String & routing_key;
    const size_t channel_id;
    const bool bind_by_id;
    const bool hash_exchange;
    const size_t num_queues;

    Poco::Logger * log;
    char row_delimiter;
    bool stalled = false;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    String current_exchange_name;

    /* Note: as all concurrent consumers share the same connection => they also share the same
     * event loop, which can be started by any consumer and the loop is blocking only to the thread that
     * started it, and the loop executes ALL active callbacks on the connection => in case num_consumers > 1,
     * at most two threads will be present: main thread and the one that executes callbacks (1 thread if
     * main thread is the one that started the loop). Both reference these variables.
     */
    std::atomic<bool> exchange_declared = false, subscribed = false, loop_started = false, false_param = false;
    std::atomic<bool> consumer_created = false, consumer_failed = false;
    std::atomic<size_t> count_subscribed = 0;

    std::vector<String> queues;
    Messages received;
    Messages messages;
    Messages::iterator current;
    std::unordered_map<String, bool> subscribed_queue;

    std::mutex mutex;

    bool nextImpl() override;

    void initExchange();
    void initQueueBindings(const size_t queue_id);
    void subscribe(const String & queue_name);
    void startEventLoop(std::atomic<bool> & check_param, std::atomic<bool> & loop_started);
    void stopEventLoopWithTimeout();
    void stopEventLoop();

};
}
