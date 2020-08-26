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
            const size_t channel_id_,
            const String channel_base_,
            const bool persistent_,
            std::atomic<bool> & wait_confirm_,
            Poco::Logger * log_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );

    ~WriteBufferToRabbitMQProducer() override;

    void countRow();
    void activateWriting() { writing_task->activateAndSchedule(); }
    void updateMaxWait() { wait_num.store(payload_counter); }

private:
    void nextImpl() override;
    void iterateEventLoop();
    void writingFunc();
    bool setupConnection(bool reconnecting);
    void setupChannel();
    void removeConfirmed(UInt64 received_delivery_tag, bool multiple, bool republish);
    void publish(ConcurrentBoundedQueue<std::pair<UInt64, String>> & message, bool republishing);

    std::pair<String, UInt16> parsed_address;
    const std::pair<String, String> login_password;
    const Names routing_keys;
    const String exchange_name;
    AMQP::ExchangeType exchange_type;
    const String channel_id_base;
    const String channel_base;
    const bool persistent;
    std::atomic<bool> & wait_confirm;

    AMQP::Table key_arguments;
    BackgroundSchedulePool::TaskHolder writing_task;

    std::unique_ptr<uv_loop_t> loop;
    std::unique_ptr<RabbitMQHandler> event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
    std::unique_ptr<AMQP::TcpChannel> producer_channel;

    String channel_id;
    ConcurrentBoundedQueue<std::pair<UInt64, String>> payloads, returned;
    UInt64 delivery_tag = 0;
    std::atomic<bool> wait_all = true;
    std::atomic<UInt64> wait_num = 0;
    UInt64 payload_counter = 0;
    std::map<UInt64, std::pair<UInt64, String>> delivery_record;
    UInt64 channel_id_counter = 0;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t rows = 0;
    std::list<std::string> chunks;
};

}
