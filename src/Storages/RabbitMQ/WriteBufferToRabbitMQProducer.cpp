#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <amqpcpp.h>

namespace DB
{
WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        ChannelPtr channel_,
        const String & routing_key_,
        const String & exchange_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_
)
        : WriteBuffer(nullptr, 0)
        , channel(channel_)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
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

        channel->declareExchange(exchange_name, AMQP::direct).onSuccess([&]()
           {
               channel->publish(exchange_name, routing_key, payload).onError(
                       [](const char * /* messsage */)
                       {
                       });
           });

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
}
