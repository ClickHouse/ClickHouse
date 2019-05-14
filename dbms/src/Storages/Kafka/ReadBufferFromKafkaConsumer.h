#pragma once

#include <Core/Names.h>
#include <IO/DelimitedReadBuffer.h>
#include <common/logger_useful.h>

#include <cppkafka/cppkafka.h>

namespace DB
{

using BufferPtr = std::shared_ptr<DelimitedReadBuffer>;
using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;


class ReadBufferFromKafkaConsumer : public ReadBuffer
{
public:
    ReadBufferFromKafkaConsumer(ConsumerPtr consumer_, Poco::Logger * log_, size_t max_batch_size)
        : ReadBuffer(nullptr, 0), consumer(consumer_), log(log_), batch_size(max_batch_size), current(messages.begin())
    {
    }

    void commit(); // Commit all processed messages.
    void subscribe(const Names & topics); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

private:
    using Messages = std::vector<cppkafka::Message>;

    ConsumerPtr consumer;
    Poco::Logger * log;
    const size_t batch_size = 1;

    Messages messages;
    Messages::const_iterator current;

    bool nextImpl() override;
};

}
