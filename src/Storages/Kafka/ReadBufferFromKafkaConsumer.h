#pragma once

#include <Core/Names.h>
#include <common/types.h>
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
    void commit(); // Commit all processed messages.
    void subscribe(); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    auto pollTimeout() const { return poll_timeout; }

    inline bool hasMorePolledMessages() const
    {
        return (stalled_status == NOT_STALLED) && (current != messages.end());
    }

    inline bool polledDataUnusable() const
    {
        return  (stalled_status != NOT_STALLED) && (stalled_status != NO_MESSAGES_RETURNED);
    }

    inline bool isStalled() const { return stalled_status != NOT_STALLED; }

    void storeLastReadMessageOffset();
    void resetToLastCommitted(const char * msg);

    // Polls batch of messages from Kafka or allows to read consecutive message by nextImpl
    // returns true if there are some messages to process
    // return false and sets stalled to false if there are no messages to process.
    // additionally sets
    bool poll();

    // Return values for the message that's being read.
    String currentTopic() const { return current[-1].get_topic(); }
    String currentKey() const { return current[-1].get_key(); }
    auto currentOffset() const { return current[-1].get_offset(); }
    auto currentPartition() const { return current[-1].get_partition(); }
    auto currentTimestamp() const { return current[-1].get_timestamp(); }
    const auto & currentHeaderList() const { return current[-1].get_header_list(); }
    String currentPayload() const { return current[-1].get_payload(); }

private:
    using Messages = std::vector<cppkafka::Message>;

    enum StalledStatus
    {
        NOT_STALLED,
        NO_MESSAGES_RETURNED,
        REBALANCE_HAPPENED,
        CONSUMER_STOPPED,
        NO_ASSIGNMENT,
        ERRORS_RETURNED
    };

    ConsumerPtr consumer;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    size_t offsets_stored = 0;

    StalledStatus stalled_status = NO_MESSAGES_RETURNED;

    bool intermediate_commit = true;
    bool allowed = true;
    size_t waited_for_assignment = 0;

    const std::atomic<bool> & stopped;

    // order is important, need to be destructed before consumer
    Messages messages;
    Messages::const_iterator current;

    // order is important, need to be destructed before consumer
    std::optional<cppkafka::TopicPartitionList> assignment;
    const Names topics;

    void drain();
    void cleanUnprocessed();
    void resetIfStopped();
    /// Return number of messages with an error.
    size_t filterMessageErrors();

    bool nextImpl() override;
};

}
