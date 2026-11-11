#pragma once

#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Storages/Kafka/Clients/Client.h>
#include <Storages/Kafka/Clients/PartitionReader.h>
#include <base/types.h>
#include <Common/CurrentMetrics.h>

#include <unordered_map>

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

using LoggerPtr = std::shared_ptr<Poco::Logger>;

class KafkaConsumer2;
using KafkaConsumer2Ptr = std::shared_ptr<KafkaConsumer2>;

/// Consumer of the Kafka2 engine, built on the in-tree Kafka client
/// (Storages/Kafka/Clients): a synchronous, zero-thread client where every
/// assigned topic-partition is a PartitionReader positioned at an explicit
/// offset. There are no consumer groups, no rebalancing and no background
/// callbacks — KeeperHandlingConsumer owns the assignment through Keeper
/// locks, and the Kafka-side commit is a group-less best-effort mirror.
class KafkaConsumer2 : public IKafkaExceptionInfoSink, public std::enable_shared_from_this<KafkaConsumer2>
{
public:
    /// Sentinel for "no offset stored in Keeper". The consumer resolves it to
    /// the broker-committed offset of the consumer group, falling back to the
    /// earliest offset (the `auto.offset.reset=earliest` behavior of the
    /// librdkafka-based implementation).
    static inline constexpr int64_t INVALID_OFFSET = -1001;

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
        UInt64 num_messages_read{};
        ExceptionsBuffer exceptions_buffer;
    };

    using TopicPartitionOffsets = std::vector<TopicPartitionOffset>;

    KafkaConsumer2(
        LoggerPtr log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        const std::atomic<bool> & stopped_,
        const Names & topics_,
        const String & group_,
        size_t skip_bytes_ = 0);

    ~KafkaConsumer2() override;

    /// Creating the client is cheap (it owns no threads and connects lazily),
    /// but it is still done on first use only, like before.
    void createConsumer(Kafka::ClientConfig consumer_config);
    bool hasConsumer() const { return client != nullptr; }

    auto pollTimeout() const { return poll_timeout; }

    inline bool hasMorePolledMessages() const { return (stalled_status == StalledStatus::NOT_STALLED) && (current != messages.end()); }

    inline bool isStalled() const { return stalled_status != StalledStatus::NOT_STALLED; }

    /// True when the last `consume` was cut short by a stop, leaving the batch incomplete.
    inline bool wasStoppedWhileConsuming() const { return stalled_status == StalledStatus::CONSUMER_STOPPED; }

    /// Positions the readers of the given topic-partitions at the given
    /// offsets, resolving INVALID_OFFSET sentinels through the broker.
    /// Partitions missing from the list are dropped.
    void updateOffsets(TopicPartitionOffsets && topic_partition_offsets);

    /// Polls batch of messages from the given topic-partition and returns read buffer containing the next message or
    /// nullptr when there are no messages to process. A single poll pulls at most
    /// `min(message_count, batch_size)` messages into the consumer.
    ReadBufferPtr consume(const TopicPartition & topic_partition, const std::optional<uint64_t> & message_count);

    /// Best-effort group-less commit to Kafka; Keeper is the source of truth.
    void commit(const TopicPartitionOffset & topic_partition_offset);

    // Return values for the message that's being read.
    const String & currentTopic() const { return current_topic_partition.topic; }
    String currentKey() const { return current[-1].key.value_or(""); }
    auto currentOffset() const { return current[-1].offset; }
    auto currentPartition() const { return current_topic_partition.partition_id; }
    /// Milliseconds since epoch, nullopt when the broker sent no timestamp.
    std::optional<int64_t> currentTimestampMs() const
    {
        const auto timestamp_ms = current[-1].timestamp_ms;
        return timestamp_ms >= 0 ? std::optional{timestamp_ms} : std::nullopt;
    }
    const std::vector<Kafka::RecordHeader> & currentHeaderList() const { return current[-1].headers; }
    String currentPayload() const { return current[-1].value.value_or(""); }

    // Build the full list of partitions for our subscribed topics.
    TopicPartitionOffsets getAllTopicPartitionOffsets() const;

    Stat getStat() const;

    void setExceptionInfo(const std::string & text, bool with_stacktrace) override;

private:
    using Messages = std::vector<Kafka::Record>;

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

    std::atomic<UInt64> num_messages_read = 0;

    std::unique_ptr<Kafka::Client> client;
    /// Reported as the consumer id of system.kafka_consumers (there is no
    /// group member id: the client never joins a consumer group).
    String client_id;
    LoggerPtr log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    const size_t skip_bytes = 0;

    StalledStatus stalled_status = StalledStatus::NO_MESSAGES_RETURNED;

    const std::atomic<bool> & stopped;

    Messages messages;
    Messages::const_iterator current;
    /// The records do not carry their topic-partition; it is the one of the last poll.
    TopicPartition current_topic_partition;

    /// One positioned reader per assigned topic-partition.
    std::unordered_map<TopicPartition, Kafka::PartitionReader, TopicPartitionHash, TopicPartitionEquality> readers;

    /// Reader positions for `getStat`, which runs concurrently with `consume`
    /// (PartitionReader itself is single-threaded).
    mutable std::mutex offsets_snapshot_mutex;
    CurrentOffsetsMap offsets_snapshot TSA_GUARDED_BY(offsets_snapshot_mutex);

    const Names topics;
    const String group;

    void resetIfStopped();
    ReadBufferPtr getNextMessage();
};

}
