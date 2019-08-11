#include "WriteBufferToKafkaProducer.h"

namespace DB
{
WriteBufferToKafkaProducer::WriteBufferToKafkaProducer(
    ProducerPtr producer_, const std::string & topic_, size_t rows_per_message, size_t chunk_size_, std::chrono::milliseconds poll_timeout)
    : WriteBuffer(nullptr, 0)
    , producer(producer_)
    , topic(topic_)
    , max_rows(rows_per_message)
    , chunk_size(chunk_size_)
    , timeout(poll_timeout)
{
}

WriteBufferToKafkaProducer::~WriteBufferToKafkaProducer()
{
    assert(rows == 0 && chunks.empty());
}

void WriteBufferToKafkaProducer::count_row()
{
    if (++rows % max_rows == 0)
    {
        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + offset());
        for (const auto & chunk : chunks)
            payload.append(chunk);
        payload.append(chunks.back(), 0, offset());

        while (true)
        {
            try
            {
                producer->produce(cppkafka::MessageBuilder(topic).payload(payload));
            }
            catch (cppkafka::HandleException & e)
            {
                if (e.get_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                {
                    producer->poll(timeout);
                    continue;
                }
                throw e;
            }

            break;
        }

        rows = 0;
        chunks.clear();
        set(nullptr, 0);
    }
}

void WriteBufferToKafkaProducer::flush()
{
    producer->flush();
}

void WriteBufferToKafkaProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().reserve(chunk_size);
    set(chunks.back().data(), chunk_size);
}

}
