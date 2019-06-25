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
    ReadBufferFromKafkaConsumer(
        ConsumerPtr consumer_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_, bool intermediate_commit_);
    ~ReadBufferFromKafkaConsumer() override;

    void commit(); // Commit all processed messages.
    void subscribe(const Names & topics); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    auto pollTimeout() { return poll_timeout; }

private:
    using Messages = std::vector<cppkafka::Message>;

    ConsumerPtr consumer;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    bool stalled = false;
    bool intermediate_commit = true;

    Messages messages;
    Messages::const_iterator current;

    bool nextImpl() override;
};

}
