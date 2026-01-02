#pragma once

#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>
#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <Common/CurrentMetrics.h>

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

using CppKafkaConsumerPtr = std::shared_ptr<cppkafka::Consumer>;
using LoggerPtr = std::shared_ptr<Poco::Logger>;

class KafkaConsumer2;
using KafkaConsumer2Ptr = std::shared_ptr<KafkaConsumer2>;

class KafkaConsumer2 : public IKafkaExceptionInfoSink, public std::enable_shared_from_this<KafkaConsumer2>
{
public:
    static inline constexpr int INVALID_OFFSET = RD_KAFKA_OFFSET_INVALID;
    static inline constexpr int BEGINNING_OFFSET = RD_KAFKA_OFFSET_BEGINNING;
    static inline constexpr int END_OFFSET = RD_KAFKA_OFFSET_END;

    struct TopicPartition
    {
        String topic;
        int32_t partition_id;

        bool operator==(const TopicPartition &) const = default;
        bool operator<(const TopicPartition & other) const;
    };

    using TopicPartitions = std::vector<TopicPartition>;

    struct TopicPartitionHash
    {
        std::size_t operator()(const TopicPartition & tp) const;
    };

    struct TopicPartitionEquality
    {
        bool operator()(const TopicPartition & lhs, const TopicPartition & rhs) const
        {
            return lhs.topic == rhs.topic && lhs.partition_id == rhs.partition_id;
        }
    };

    struct TopicPartitionOffset : public TopicPartition
    {
        int64_t offset{INVALID_OFFSET};
    };

    using CurrentOffsetsMap = std::unordered_map<TopicPartition, int64_t, TopicPartitionHash, TopicPartitionEquality>;
    using ExceptionsBuffer = StorageKafkaUtils::ConsumerStatistics::ExceptionsBuffer;
    struct Stat
    {
        CurrentOffsetsMap current_offsets;
        std::string consumer_id;
        std::string rdkafka_stat;
        UInt64 num_messages_read;
        ExceptionsBuffer exceptions_buffer;
    };

    using TopicPartitionOffsets = std::vector<TopicPartitionOffset>;

    struct TopicPartitionCount
    {
        String topic;
        size_t partition_count;
    };

    KafkaConsumer2(
        LoggerPtr log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        const std::atomic<bool> & stopped_,
        const Names & topics_,
        size_t skip_bytes_ = 0);

    ~KafkaConsumer2() override;

    void createConsumer(cppkafka::Configuration consumer_config);
    bool hasConsumer() const { return consumer.get() != nullptr; }
    CppKafkaConsumerPtr && moveConsumer();

    // Poll only the main consumer queue without any topic-partition queues. This is useful to get notified about events, such as rebalance,
    // new assignment, etc..
    void pollEvents();

    auto pollTimeout() const { return poll_timeout; }

    inline bool hasMorePolledMessages() const { return (stalled_status == StalledStatus::NOT_STALLED) && (current != messages.end()); }

    inline bool isStalled() const { return stalled_status != StalledStatus::NOT_STALLED; }

    void updateOffsets(TopicPartitionOffsets && topic_partition_offsets);

    /// Polls batch of messages from the given topic-partition and returns read buffer containing the next message or
    /// nullptr when there are no messages to process.
    ReadBufferPtr consume(const TopicPartition & topic_partition, const std::optional<uint64_t> & message_count);

    void commit(const TopicPartitionOffset & topic_partition_offset);

    // Return values for the message that's being read.
    String currentTopic() const { return current[-1].get_topic(); }
    String currentKey() const { return current[-1].get_key(); }
    auto currentOffset() const { return current[-1].get_offset(); }
    auto currentPartition() const { return current[-1].get_partition(); }
    auto currentTimestamp() const { return current[-1].get_timestamp(); }
    const auto & currentHeaderList() const { return current[-1].get_header_list(); }
    String currentPayload() const { return current[-1].get_payload(); }

    // Build the full list of partitions for our subscribed topics.
    TopicPartitionOffsets getAllTopicPartitionOffsets() const;

    Stat getStat() const;

    void setExceptionInfo(const std::string & text, bool with_stacktrace) override;

private:
    using Messages = std::vector<cppkafka::Message>;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaConsumers};

    enum class StalledStatus
    {
        NOT_STALLED,
        NO_MESSAGES_RETURNED,
        CONSUMER_STOPPED,
        NO_ASSIGNMENT,
        ERRORS_RETURNED
    };

    /// system.kafka_consumers data is retrieved asynchronously
    /// so we have to protect exceptions_buffer
    mutable std::mutex exception_mutex;
    const size_t EXCEPTIONS_DEPTH = 10;
    ExceptionsBuffer exceptions_buffer TSA_GUARDED_BY(exception_mutex);

    // order is important, need to be destructed *after* consumer
    mutable std::mutex rdkafka_stat_mutex;
    std::string rdkafka_stat TSA_GUARDED_BY(rdkafka_stat_mutex);
    std::atomic<UInt64> num_messages_read = 0;

    CppKafkaConsumerPtr consumer{nullptr};
    LoggerPtr log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    const size_t skip_bytes = 0;

    StalledStatus stalled_status = StalledStatus::NO_MESSAGES_RETURNED;

    const std::atomic<bool> & stopped;

    // order is important, need to be destructed before consumer
    Messages messages;
    Messages::const_iterator current;

    // order is important, need to be destructed before consumer
    std::unordered_map<TopicPartition, cppkafka::Queue, TopicPartitionHash, TopicPartitionEquality> queues;
    const Names topics;

    bool polledDataUnusable(const TopicPartition & topic_partition) const;
    void resetIfStopped();
    void filterMessageErrors();
    ReadBufferPtr getNextMessage();

    void cleanQueuesAndMessages();
    void initializeQueues(const cppkafka::TopicPartitionList & topic_partitions);
};

}
