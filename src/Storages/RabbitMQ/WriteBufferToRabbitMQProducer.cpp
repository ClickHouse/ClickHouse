#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <common/logger_useful.h>
#include <amqpcpp.h>

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
        size_t chunk_size_
)
        : WriteBuffer(nullptr, 0)
        , producer_channel(producer_channel_)
        , eventHandler(eventHandler_)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{
}

WriteBufferToRabbitMQProducer::~WriteBufferToRabbitMQProducer()
{
    assert(rows == 0 && chunks.empty());
}

void WriteBufferToRabbitMQProducer::count_row()
{
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

        producer_channel->declareExchange(exchange_name, AMQP::direct)
        .onSuccess([&]() 
        {
           producer_channel->publish(exchange_name, routing_key, payload)
           .onError([&](const char * message)
           {
               LOG_DEBUG(log, "Publish error: " << message);
               stopEventLoop();
           }); 

           stopEventLoop();
       });

        rows = 0;
        chunks.clear();
        set(nullptr, 0);

        startEventLoop();
    }
}


void WriteBufferToRabbitMQProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}


void WriteBufferToRabbitMQProducer::startEventLoop()
{
    eventHandler.start();
}


void WriteBufferToRabbitMQProducer::startNonBlockEventLoop()
{
    eventHandler.startNonBlock();
}


void WriteBufferToRabbitMQProducer::stopEventLoop()
{
    eventHandler.stop();
}

}
