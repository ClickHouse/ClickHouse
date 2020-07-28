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
        const std::atomic<bool> & stopped_,
        const Names & _topics
    );
    ~ReadBufferFromKafkaConsumer() override;
    void allowNext() { allowed = true; } // Allow to read next message.
    void commit(); // Commit all processed messages.
    void subscribe(); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    auto pollTimeout() const { return poll_timeout; }

    bool hasMorePolledMessages() const;
    bool polledDataUnusable() const { return (was_stopped || rebalance_happened); }
    bool isStalled() const { return stalled; }

    void storeLastReadMessageOffset();
    void resetToLastCommitted(const char * msg);

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
    size_t offsets_stored = 0;
    bool stalled = false;
    bool intermediate_commit = true;
    bool allowed = true;

    const std::atomic<bool> & stopped;

    // order is important, need to be destructed before consumer
    Messages messages;
    Messages::const_iterator current;

    bool rebalance_happened = false;

    bool was_stopped = false;

    // order is important, need to be destructed before consumer
    cppkafka::TopicPartitionList assignment;
    const Names topics;

    void drain();

    bool nextImpl() override;
};

}
