#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <common/logger_useful.h>
#include <amqpcpp.h>
#include <chrono>
#include <thread>

namespace DB
{

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        ChannelPtr producer_channel_,
        RabbitMQHandler & eventHandler_,
        const String & routing_key_,
        const String & exchange_,
        Poco::Logger * log_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , producer_channel(std::move(producer_channel_))
        , eventHandler(eventHandler_)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{

    initExchange();
}


WriteBufferToRabbitMQProducer::~WriteBufferToRabbitMQProducer()
{
    assert(rows == 0 && chunks.empty());
}


void WriteBufferToRabbitMQProducer::initExchange()
{
    producer_channel->declareExchange(exchange_name, AMQP::direct)
        .onSuccess([&]()
        {
            exchange_declared = true;
        })
        .onError([&](const char * /* message */)
        {
            exchange_error = true;
            LOG_ERROR(log, "Echange was not declared - messages might be lost.");
        });
}


void WriteBufferToRabbitMQProducer::count_row()
{
    /// it is important to make sure exchange is declared before we proceed
    while (!exchange_declared && !exchange_error)
    {
        startNonBlockEventLoop();
    }

    if (++rows % max_rows == 0)
    {
        const std::string & last_chunk = chunks.back();
        size_t last_chunk_size = offset();

        if (delim && last_chunk[last_chunk_size - 1] == delim)
            --last_chunk_size;

        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + last_chunk_size);

        for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
            payload.append(*i);

        payload.append(last_chunk, 0, last_chunk_size);

        producer_channel->publish(exchange_name, routing_key, payload);

        rows = 0;
        chunks.clear();
        set(nullptr, 0);
    }
}


void WriteBufferToRabbitMQProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}


void WriteBufferToRabbitMQProducer::startNonBlockEventLoop()
{
    eventHandler.startNonBlock();
}


}
