#pragma once

#include <IO/ReadBuffer.h>
#include <common/logger_useful.h>

#include <cppkafka/cppkafka.h>

namespace DB
{
using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
public:
    ReadBufferFromKafkaConsumer(ConsumerPtr consumer_, Poco::Logger * log_, char row_delimiter_)
        : ReadBuffer(nullptr, 0), consumer(consumer_), log(log_), row_delimiter(row_delimiter_)
    {
        if (row_delimiter != '\0')
            LOG_TRACE(log, "Row delimiter is: " << row_delimiter);
    }

    /// Commit messages read with this consumer
    void commit()
    {
        LOG_TRACE(log, "Committing " << read_messages << " messages");
        if (read_messages == 0)
            return;

        consumer->async_commit();
        read_messages = 0;
    }

private:
    ConsumerPtr consumer;
    cppkafka::Message current;
    bool current_pending = false; /// We've fetched "current" message and need to process it on the next iteration.
    Poco::Logger * log;
    size_t read_messages = 0;
    char row_delimiter;

    bool nextImpl() override;
};

} // namespace DB
