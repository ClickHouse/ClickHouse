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
    ReadBufferFromKafkaConsumer(ConsumerPtr consumer_, Poco::Logger * log_)
        : ReadBuffer(nullptr, 0), consumer(consumer_), log(log_)
    {
    }

    /// Commit messages read with this consumer
    auto commit()
    {
        if (read_messages)
        {
            LOG_TRACE(log, "Committing " << read_messages << " messages");
            consumer->async_commit();
        }

        auto result = read_messages;
        read_messages = 0;

        return result;
    }

private:
    ConsumerPtr consumer;
    cppkafka::Message message;
    Poco::Logger * log;
    size_t read_messages = 0;

    bool nextImpl() override;
};

} // namespace DB
