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

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            std::pair<String, UInt16> & parsed_address_,
            Context & global_context,
            const std::pair<String, String> & login_password_,
            const Names & routing_keys_,
            const String & exchange_name_,
            const AMQP::ExchangeType exchange_type_,
            Poco::Logger * log_,
            const bool use_transactional_channel_,
            const bool persistent_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );

    ~WriteBufferToRabbitMQProducer() override;

    void countRow();
    void activateWriting() { writing_task->activateAndSchedule(); }
    void commit();
    void updateMaxWait() { wait_num.store(delivery_tag); }

private:
    void nextImpl() override;
    void iterateEventLoop();
    void writingFunc();
    bool setupConnection();
    void setupChannel();
    void removeConfirmed(UInt64 received_delivery_tag, bool multiple);

    std::pair<String, UInt16> parsed_address;
    const std::pair<String, String> login_password;
    const Names routing_keys;
    const String exchange_name;
    AMQP::ExchangeType exchange_type;
    const bool use_transactional_channel;
    const bool persistent;

    AMQP::Table key_arguments;
    BackgroundSchedulePool::TaskHolder writing_task;

    std::unique_ptr<uv_loop_t> loop;
    std::unique_ptr<RabbitMQHandler> event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
    std::unique_ptr<AMQP::TcpChannel> producer_channel;

    ConcurrentBoundedQueue<String> payloads;
    UInt64 delivery_tag = 0;
    std::atomic<bool> wait_all = true;
    std::atomic<UInt64> wait_num = 0;
    std::set<UInt64> delivery_tags_record;
    std::mutex mutex;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t rows = 0;
    std::list<std::string> chunks;
};

}
