#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <list>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{
using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;
using Channels = std::vector<ChannelPtr>;

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            ChannelPtr producer_channel_,
            RabbitMQHandler & eventHandler_,
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

private:
    void nextImpl() override;
    void checkExchange();
    void startNonBlockEventLoop();

    ChannelPtr producer_channel;
    RabbitMQHandler & eventHandler;

    std::atomic<bool> exchange_declared = false, exchange_error = false;
    const String routing_key;
    const String exchange_name;
    const bool bind_by_id;
    const bool hash_exchange;
    const size_t num_queues;
    size_t next_queue = 0;
    String channel_id;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t count_mes = 0;
    size_t rows = 0;
    std::list<std::string> chunks;

};
}
