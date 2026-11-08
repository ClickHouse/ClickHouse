#pragma once

#include <Storages/Kafka/Clients/Config.h>
#include <Storages/Kafka/Clients/Error.h>
#include <Storages/Kafka/Clients/Types.h>

#include <chrono>
#include <memory>
#include <optional>
#include <vector>

namespace Kafka
{

class Cluster;

class PartitionProducer;

struct BrokerMetadata
{
    Int32 node_id = -1;
    String host;
    Int32 port = 0;
    std::optional<String> rack;
};

struct PartitionMetadata
{
    Int32 partition = 0;
    Int32 leader_id = -1;
    Int32 leader_epoch = -1;
    std::vector<Int32> replicas;
    std::vector<Int32> isr;
};

struct TopicMetadata
{
    String name;
    Uuid topic_id;
    bool is_internal = false;
    std::vector<PartitionMetadata> partitions;
};

struct ClusterMetadata
{
    std::vector<BrokerMetadata> brokers;
    std::optional<String> cluster_id;
    Int32 controller_id = -1;
    std::vector<TopicMetadata> topics;
};

/// One stateless "give me messages from this topic-partition" call.
struct FetchSpec
{
    String topic;
    Int32 partition = 0;
    Int64 offset = 0;
    /// Broker-side long poll: how long the broker may hold the request while
    /// it waits for min_bytes to accumulate. Clamped to the call deadline.
    Int32 max_wait_ms = 500;
    Int32 min_bytes = 1;
    /// Per-partition byte cap for one response.
    Int32 max_bytes = 1 << 20;
};

struct FetchResult
{
    /// Absolute offsets/timestamps; control batches and pre-offset records
    /// (batch compaction artifacts) are already filtered out.
    std::vector<Record> records;
    Int64 high_watermark = -1;
    Int64 log_start_offset = -1;
    /// Where the next fetch should continue. Advances past control batches
    /// and compacted gaps even when `records` is empty.
    Int64 next_offset = 0;
    /// The response ended in a partial batch (cut at max_bytes) that was
    /// discarded; the next fetch returns it whole.
    bool truncated_tail = false;
    /// Offsets below this are decided (committed or aborted); under
    /// read_committed consumption effectively ends here. -1 when unknown.
    Int64 last_stable_offset = -1;
};

/// Synchronous Kafka client. Zero background threads: every operation runs on
/// the calling thread, bounded by a deadline. Thread-safe — operations check
/// out connections from small per-broker pools, so calls from different
/// threads proceed in parallel.
class Client
{
public:
    explicit Client(ClientConfig config);
    ~Client();
    Client(const Client &) = delete;
    Client & operator=(const Client &) = delete;

    /// Topic metadata for the named topics (cheap: pooled connection + cache
    /// bounded by metadata_max_age). Empty list = brokers only.
    ClusterMetadata getMetadata(const std::vector<String> & topics, std::chrono::milliseconds timeout);

    /// Metadata with allow_auto_topic_creation=true, polled until the topic
    /// has partitions (broker-side creation is asynchronous).
    void ensureTopic(const String & topic, std::chrono::milliseconds timeout);

    /// Consume one batch from one topic-partition. No groups, no subscription,
    /// no rebalance — requirement 3.
    FetchResult fetch(const FetchSpec & spec, std::chrono::milliseconds timeout);

    /// Resolve earliest/latest/by-timestamp to a concrete offset.
    Int64 listOffset(const TopicPartition & tp, OffsetSpec spec, std::chrono::milliseconds timeout);

    /// Group-less offset commit (generation -1, empty member id): the group is
    /// a named storage slot, membership does not exist. ClickHouse owns the
    /// assignment.
    void commitOffsets(
        const String & group, const std::vector<TopicPartitionOffset> & offsets, std::chrono::milliseconds timeout);

    /// Committed offsets for the given partitions; offset -1 = nothing stored.
    std::vector<TopicPartitionOffset> fetchCommittedOffsets(
        const String & group, const std::vector<TopicPartition> & partitions, std::chrono::milliseconds timeout);

    struct ProduceSpec
    {
        String topic;
        Int32 partition = 0;
        /// Record offsets/deltas are assigned by the library; set key/value/
        /// headers/timestamp_ms only. timestamp_ms -1 = now.
        std::vector<Record> records;
        /// -1 = all ISR, 1 = leader only, 0 = fire-and-forget.
        Int16 acks = -1;
        Compression compression = Compression::None;
        /// -1 = the codec's default level.
        int compression_level = -1;
    };

    struct ProduceResult
    {
        /// Offset assigned to the first record (-1 with acks=0).
        Int64 base_offset = -1;
        Int64 log_append_time_ms = -1;
    };

    /// Synchronous produce of one batch to one partition.
    ProduceResult produce(const ProduceSpec & spec, std::chrono::milliseconds timeout);

    /// Interrupt every blocked call on any thread with ErrorKind::Wakeup.
    /// One-shot: calls started afterwards proceed normally.
    void wakeup();

private:
    friend class PartitionProducer;
    friend class TopicProducer;
    /// Internal: shared cluster state (metadata cache, pools, wakeup).
    Cluster & getCluster();

    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
