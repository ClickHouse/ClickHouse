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
        , producer_channel(std::move(producer_channel_))
        , eventHandler(eventHandler_)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{
    producer_channel->confirmSelect()
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Error with the producer channel: " << message);
        });
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

        bool exchange_declared = false, exchange_error = false;
        producer_channel->declareExchange(exchange_name, AMQP::direct)
            .onSuccess([&]() 
            {
                exchange_declared = true;
                stopEventLoop();
            })
            .onError([&](const char * /* message */)
            {
                exchange_error = true;
                stopEventLoop();
            });

        /* This is important here to start making event loops in a loop because when INSERT query is called in a
         * complex context of other queries, then it is not guaranteed that exactly declareExchange.callback will stop
         * current event loop, because this can be done by other pending events. As a result, this callback
         * will remain pending and might be invoked after the call to this class destructor,
         * which will lead to heap use after free.  */

        while (!exchange_declared && !exchange_error)
        {
            startNonBlockEventLoop();
        }

        if (!exchange_error)
        {
            producer_channel->publish(exchange_name, routing_key, payload);
        }
        else
        {
            LOG_ERROR(log, "Echange was not declared so no publishing happened.");
        }

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

void WriteBufferToRabbitMQProducer::free()
{
    eventHandler.free();
}

}
