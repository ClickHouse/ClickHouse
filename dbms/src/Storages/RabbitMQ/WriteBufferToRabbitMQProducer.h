#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>

#include <list>

#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

/* It is recommended to have a channel per thread, and a channel per consumer.
But for publishing it is totally ok to use the same channel. */

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            ChannelPtr channel_,
            const std::string & routing_key_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );
    ~WriteBufferToRabbitMQProducer() override;

    void count_row();

private:
    void nextImpl() override;

    ChannelPtr channel;
    const std::string routing_key;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;

    size_t rows = 0;
    std::list<std::string> chunks;
};
}
