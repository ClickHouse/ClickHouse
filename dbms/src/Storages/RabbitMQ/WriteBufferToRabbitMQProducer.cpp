#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <cassert>
#include <amqpcpp.h>

namespace DB
{
    WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
            ProducerPtr producer_,
            RabbitMQHandler * handler_,
            const std::string & routing_key_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    )
            : WriteBuffer(nullptr, 0)
            , producer(producer_)
            , handler(handler_)
            , routing_key(routing_key_)
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
            std::string payload;
            payload.reserve((chunks.size() - 1) * chunk_size + offset());

            for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
                payload.append(*i);

            int trunk_delim = delim && chunks.back()[offset() - 1] == delim ? 1 : 0;

            payload.append(chunks.back(), 0, offset() - trunk_delim);

            while (true)
            {
                try
                {
                    producer->onReady([&]()
                                    {
                                        if(handler->connected())
                                        {
                                            producer->publish("", routing_key, payload);
                                            handler->quit();
                                        }
                                    });
                    handler->loop();
                }
                catch (AMQP::Exception & e)
                {
                    // TODO: catch here queue overflow
                    throw e;
                }

                break;
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

}