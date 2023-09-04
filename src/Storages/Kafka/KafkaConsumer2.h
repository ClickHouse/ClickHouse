#pragma once

#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>

#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>

#include <compare>

namespace CurrentMetrics
{
extern const Metric KafkaConsumers;
}

namespace Poco
{
class Logger;
}

namespace DB
{

using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class KafkaConsumer2
{
public:
    static inline constexpr int INVALID_OFFSET = RD_KAFKA_OFFSET_INVALID;
    static inline constexpr int BEGINNING_OFFSET = RD_KAFKA_OFFSET_BEGINNING;
    static inline constexpr int END_OFFSET = RD_KAFKA_OFFSET_END;

    struct TopicPartition
    {
        String topic;
        int32_t partition_id;
        int64_t offset{INVALID_OFFSET};

        bool operator==(const TopicPartition &) const = default;
        bool operator<(const TopicPartition & other) const;
    };

    using TopicPartitions = std::vector<TopicPartition>;

    struct OnlyTopicNameAndPartitionIdHash
    {
        std::size_t operator()(const TopicPartition & tp) const
        {
            SipHash s;
            s.update(tp.topic);
            s.update(tp.partition_id);
            return s.get64();
        }
    };

    struct OnlyTopicNameAndPartitionIdEquality
    {
        bool operator()(const TopicPartition & lhs, const TopicPartition & rhs) const
        {
            return lhs.topic == rhs.topic && lhs.partition_id == rhs.partition_id;
        }
    };

    struct TopicPartitionCount
    {
        String topic;
        size_t partition_count;
    };

    using TopicPartitionCounts = std::vector<KafkaConsumer2::TopicPartitionCount>;

    // struct AssignmentChanges {
    //     TopicPartitions revoked_partitions;
    //     TopicPartitions new_partitions;
    // };

    KafkaConsumer2(
        ConsumerPtr consumer_,
        Poco::Logger * log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        bool intermediate_commit_,
        const std::atomic<bool> & stopped_,
        const Names & _topics);

    ~KafkaConsumer2();

    void pollEvents();

    TopicPartitionCounts getPartitionCounts() const;

    auto pollTimeout() const { return poll_timeout; }

    inline bool hasMorePolledMessages() const { return (stalled_status == StalledStatus::NOT_STALLED) && (current != messages.end()); }

    inline bool isStalled() const { return stalled_status != StalledStatus::NOT_STALLED; }

    bool polledDataUnusable(const TopicPartition & topic_partition) const;

    // Returns the topic partitions that the consumer got from rebalancing the consumer group. If the consumer received
    // no topic partitions or all of them were revoked, it returns a null pointer.
    TopicPartitions const * getKafkaAssignment() const;

    // As the main source of offsets is not Kafka, the offsets needs to pushed to the consumer from outside
    bool needsOffsetUpdate() const { return needs_offset_update; }

    // Returns true if it received new assignment and could update the internal state accordingly, false otherwise
    void updateOffsets(const TopicPartitions & topic_partitions);

    /// Polls batch of messages from Kafka and returns read buffer containing the next message or
    /// nullptr when there are no messages to process.
    /// TODO(antaljanosbenjamin): add batch size param
    ReadBufferPtr consume(const TopicPartition & topic_partition);

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
    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaConsumers};

    enum class StalledStatus
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

    StalledStatus stalled_status = StalledStatus::NO_MESSAGES_RETURNED;

    bool intermediate_commit = true;
    size_t waited_for_assignment = 0;

    const std::atomic<bool> & stopped;

    // order is important, need to be destructed before consumer
    Messages messages;
    Messages::const_iterator current;

    // order is important, need to be destructed before consumer
    std::optional<TopicPartitions> assignment;
    bool needs_offset_update{false};
    std::unordered_map<TopicPartition, cppkafka::Queue, OnlyTopicNameAndPartitionIdHash, OnlyTopicNameAndPartitionIdEquality> queues;
    const Names topics;

    void drain();
    void cleanUnprocessed();
    void resetIfStopped();
    /// Return number of messages with an error.
    size_t filterMessageErrors();
    ReadBufferPtr getNextMessage();

    void initializeQueues(const cppkafka::TopicPartitionList & topic_partitions);
};

}
