#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <list>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{
using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            ChannelPtr producer_channel_,
            RabbitMQHandler & eventHandler_,
            const String & routing_key_,
            const String & exchange_,
            Poco::Logger * log_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );

    ~WriteBufferToRabbitMQProducer() override;

    void startNonBlockEventLoop();
    void count_row();

    bool exchange_declared = false, exchange_error = false;

private:
    void nextImpl() override;
    void initExchange();

    ChannelPtr producer_channel;
    RabbitMQHandler & eventHandler;
    const String routing_key;
    const String exchange_name;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;

    size_t rows = 0;
    std::list<std::string> chunks;
};
}
