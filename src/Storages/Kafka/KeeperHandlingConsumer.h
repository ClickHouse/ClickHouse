#pragma once

#include <expected>
#include <filesystem>
#include <Core/Block_fwd.h>
#include <IO/ReadBuffer.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <cppkafka/topic.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
class KeeperHandlingConsumer
{
public:
    enum class CannotPollReason : uint8_t
    {
        NoMetadata,
        KeeperSessionEnded,
        NoPartitions,
    };

    class MessageInfo
    {
    public:
        explicit MessageInfo(const KafkaConsumer2 & kafka_consumer_)
            : kafka_consumer(kafka_consumer_)
        {
        }

        String currentTopic() const { return kafka_consumer.currentTopic(); }
        int32_t currentPartition() const { return kafka_consumer.currentPartition(); }
        int64_t currentOffset() const { return kafka_consumer.currentOffset(); }
        String currentKey() const { return kafka_consumer.currentKey(); }
        String currentPayload() const { return kafka_consumer.currentPayload(); }
        boost::optional<cppkafka::MessageTimestamp> currentTimestamp() const { return kafka_consumer.currentTimestamp(); }
        const cppkafka::Message::HeaderListType & currentHeaderList() const { return kafka_consumer.currentHeaderList(); }

    private:
        const KafkaConsumer2 & kafka_consumer;
    };

    class OffsetGuard
    {
    public:
        OffsetGuard(KeeperHandlingConsumer & consumer_, int64_t new_offset_);
        OffsetGuard(OffsetGuard && other) noexcept;
        ~OffsetGuard();

        void commit();
    private:
        bool needs_rollback{true};
        KeeperHandlingConsumer * consumer;
        int64_t new_offset;
    };

    using MessageSinkFunction
        = std::function<bool(ReadBufferPtr, const MessageInfo &, bool /*has_more_polled_messages*/, bool /*stalled*/)>;

    KeeperHandlingConsumer(
        const KafkaConsumer2Ptr & kafka_consumer_,
        const std::shared_ptr<zkutil::ZooKeeper> & keeper_,
        const std::filesystem::path & keeper_path_,
        const String & replica_name_,
        size_t idx_,
        const LoggerPtr & log_);

    /// It is important that the consumer is using the same Keeper session as the storage to make sure all ephemeral
    /// nodes are handled at the same time, e.g.: if a replica is deactivated because of session loss, all of its
    /// consumers should lose connection to keeper.
    bool needsNewKeeper() const;
    void setKeeper(const std::shared_ptr<zkutil::ZooKeeper> & keeper_);

    /// The concept of `prepareToPoll` and `poll` is quite a bit quirky, but I didn't find a better way to make it:
    /// 1. Separate the logic of converting messages to rows with virtual columns and everything else
    /// 2. Do not expose the KafkaConsumer2, so KeeperHandlingConsumer has a strict control over the offsets
    /// 3. Only do the heavy initialization once the consumer can actually poll messages
    /// As a result we have the complex return value and the MessageSink callback. In the end I think the extra
    /// complexity does worth it.

    std::optional<CannotPollReason> prepareToPoll();

    /// Returns empty optional if polling it not possible or no messages were polled.
    /// Returns an OffsetGuard that rollbacks the offsets unless it is committed or deactivated.
    std::optional<OffsetGuard> poll(MessageSinkFunction & message_sink);

private:
    using TopicPartition = KafkaConsumer2::TopicPartition;
    using TopicPartitions = KafkaConsumer2::TopicPartitions;
    using TopicPartitionOffset = KafkaConsumer2::TopicPartitionOffset;
    using TopicPartitionOffsets = KafkaConsumer2::TopicPartitionOffsets;

    struct LockedTopicPartitionInfo
    {
        zkutil::EphemeralNodeHolderPtr lock;
        std::optional<int64_t> committed_offset;
        std::optional<int64_t> intent_size;
    };

    using TopicPartitionLocks = std::
        unordered_map<TopicPartition, LockedTopicPartitionInfo, KafkaConsumer2::TopicPartitionHash, KafkaConsumer2::TopicPartitionEquality>;

    using TopicPartitionSet
        = std::unordered_set<TopicPartition, KafkaConsumer2::TopicPartitionHash, KafkaConsumer2::TopicPartitionEquality>;

    struct ActiveReplicasInfo
    {
        UInt64 active_replica_count{0};
        bool has_replica_without_locks{false};
    };

    std::filesystem::path keeper_path;
    const String replica_name;
    size_t idx;

    KafkaConsumer2Ptr kafka_consumer;
    zkutil::ZooKeeperPtr keeper;
    size_t topic_partition_index_to_consume_from{0};
    TopicPartitions assigned_topic_partitions{};
    size_t poll_count = 0;
    TopicPartitionLocks permanent_locks{};
    TopicPartitionLocks tmp_locks{};
    LoggerPtr log;

    // Quota, how many temporary locks can be taken in current round
    size_t tmp_locks_quota{};

    // Searches first in permanent_locks, then in tmp_locks.
    LockedTopicPartitionInfo & getTopicPartitionLock(const TopicPartition & topic_partition);

    std::pair<TopicPartitionSet, ActiveReplicasInfo> getLockedTopicPartitions();
    std::pair<TopicPartitions, ActiveReplicasInfo> getAvailableTopicPartitions(const TopicPartitionOffsets & all_topic_partitions);
    std::optional<LockedTopicPartitionInfo> createLocksInfoIfFree(const TopicPartition & partition_to_lock);

    void lockTemporaryLocks(const TopicPartitions & available_topic_partitions, bool has_replica_without_locks);

    void updatePermanentLocks(const TopicPartitions & available_topic_partitions, size_t topic_partitions_count, size_t active_replica_count);

    void rollbackToCommittedOffsets();

    void saveCommittedOffset(int64_t new_offset);
    // To save commit and intent nodes
    void writeTopicPartitionInfoToKeeper(const std::filesystem::path & keeper_path_to_data, const String & data);
    void writeIntentToKeeper(const KafkaConsumer2::TopicPartition & topic_partition, int64_t intent);


    std::filesystem::path getTopicPartitionPath(const TopicPartition & topic_partition);
    std::filesystem::path getTopicPartitionLockPath(const TopicPartition & topic_partition);

    void appendToAssignedTopicPartitions(const TopicPartitionLocks & locks);
};
}
