#pragma once

#include <IO/WriteBuffer.h>

#include <list>

#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

    using ProducerPtr = std::shared_ptr<AMQP::Channel>;

    class WriteBufferToRabbitMQProducer : public WriteBuffer
    {
    public:
        WriteBufferToRabbitMQProducer(
                ProducerPtr producer_,
                RabbitMQHandler * handler_,
                const std::string & routing_key_,
                std::optional<char> delimiter,
                size_t rows_per_message,
                size_t chunk_size_
        );
        ~WriteBufferToRabbitMQProducer() override;

        void count_row();

    private:
        void nextImpl() override;

        ProducerPtr producer;
        RabbitMQHandler * handler;
        const std::string routing_key;
        const std::optional<char> delim;
        const size_t max_rows;
        const size_t chunk_size;

        size_t rows = 0;
        std::list<std::string> chunks;
    };
}