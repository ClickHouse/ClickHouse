#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <list>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

using ProducerPtr = std::shared_ptr<AMQP::TcpChannel>;
using Messages = std::vector<String>;

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            std::pair<std::string, UInt16> & parsed_address,
            const String & routing_key_,
            const String & exchange_,
            Poco::Logger * log_,
            const size_t num_queues_,
            const bool bind_by_id_,
            const bool hash_exchange_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );

    ~WriteBufferToRabbitMQProducer() override;

    void count_row();
    void flush();

private:
    void nextImpl() override;
    void checkExchange();
    void startEventLoop(std::atomic<bool> & check_param);

    const String routing_key;
    const String exchange_name;
    const bool bind_by_id;
    const bool hash_exchange;
    const size_t num_queues;

    event_base * producerEvbase;
    RabbitMQHandler eventHandler;
    AMQP::TcpConnection connection;
    ProducerPtr producer_channel;

    size_t next_queue = 0;
    UInt64 message_counter = 0;
    String channel_id;

    Messages messages;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t count_mes = 0;
    size_t rows = 0;
    std::list<std::string> chunks;
};

}
