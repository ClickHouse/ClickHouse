#include "WriteBufferToKafkaProducer.h"

namespace DB
{
WriteBufferToKafkaProducer::WriteBufferToKafkaProducer(
    ProducerPtr producer_,
    const std::string & topic_,
    std::optional<char> delimiter,
    size_t rows_per_message,
    size_t chunk_size_,
    std::chrono::milliseconds poll_timeout)
    : WriteBuffer(nullptr, 0)
    , producer(producer_)
    , topic(topic_)
    , delim(delimiter)
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
        for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
            payload.append(*i);
        int trunk_delim = delim && chunks.back()[offset() - 1] == delim ? 1 : 0;
        payload.append(chunks.back(), 0, offset() - trunk_delim);

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
    // For unknown reason we may hit some internal timeout when inserting for the first time.
    while (true)
    {
        try
        {
            producer->flush(timeout);
        }
        catch (cppkafka::HandleException & e)
        {
            if (e.get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT)
                continue;
            throw e;
        }

        break;
    }
}

void WriteBufferToKafkaProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}

}
