#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>

#include <cppkafka/cppkafka.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
public:
    ReadBufferFromKafkaConsumer(
        ConsumerPtr consumer_,
        Poco::Logger * log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        bool intermediate_commit_,
        char delimiter_,
        const std::atomic<bool> & stopped_);
    ~ReadBufferFromKafkaConsumer() override;

    void commit(); // Commit all processed messages.
    void subscribe(const Names & topics); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    auto pollTimeout() const { return poll_timeout; }

    // Return values for the message that's being read.
    String currentTopic() const { return current[-1].get_topic(); }
    String currentKey() const { return current[-1].get_key(); }
    auto currentOffset() const { return current[-1].get_offset(); }
    auto currentPartition() const { return current[-1].get_partition(); }
    auto currentTimestamp() const { return current[-1].get_timestamp(); }

private:
    using Messages = std::vector<cppkafka::Message>;

    ConsumerPtr consumer;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    bool stalled = false;
    bool intermediate_commit = true;

    char delimiter;
    bool put_delimiter = false;

    const std::atomic<bool> & stopped;

    Messages messages;
    Messages::const_iterator current;

    bool nextImpl() override;
};

using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromKafkaConsumer>;

}
