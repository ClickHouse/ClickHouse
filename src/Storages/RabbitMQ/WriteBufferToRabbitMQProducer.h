#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <list>
#include <mutex>
#include <atomic>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>

namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            std::pair<String, UInt16> & parsed_address,
            Context & global_context,
            const std::pair<String, String> & login_password_,
            const String & routing_key_,
            const String & exchange_,
            Poco::Logger * log_,
            size_t num_queues_,
            bool bind_by_id_,
            bool use_transactional_channel_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );

    ~WriteBufferToRabbitMQProducer() override;

    void countRow();
    void activateWriting() { writing_task->activateAndSchedule(); }

private:
    void nextImpl() override;
    void checkExchange();
    void iterateEventLoop();
    void writingFunc();
    void finilizeProducer();

    const std::pair<String, String> login_password;
    const String routing_key;
    const String exchange_name;
    const bool bind_by_id;
    const size_t num_queues;
    const bool use_transactional_channel;

    BackgroundSchedulePool::TaskHolder writing_task;
    std::atomic<bool> stop_loop = false;

    std::unique_ptr<uv_loop_t> loop;
    std::unique_ptr<RabbitMQHandler> event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
    ChannelPtr producer_channel;

    ConcurrentBoundedQueue<String> payloads;
    size_t next_queue = 0;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t rows = 0;
    std::list<std::string> chunks;
};

}
