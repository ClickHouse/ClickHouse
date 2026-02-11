#pragma once

#include <string>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Interpreters/InsertDeduplication.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Storages/MergeTree/InsertBlockInfo.h>


namespace Poco { class Logger; }

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{
enum class InsertDeduplicationVersions : uint8_t;


class StorageReplicatedMergeTree;
struct BlockWithPartition;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

struct MergeTreeTemporaryPart;
using TemporaryPartPtr = std::unique_ptr<MergeTreeTemporaryPart>;


struct DelayedPartInPartition
{
    LoggerPtr log;
    BlockWithPartition block_with_partition;

    DeduplicationInfo::Ptr deduplication_info;
    TemporaryPartPtr temp_part;
    UInt64 elapsed_ns;
    ProfileEvents::Counters part_counters;
};


/// ReplicatedMergeTreeSink will sink data to replicated merge tree with deduplication.
/// The template argument "async_insert" indicates whether this sink serves for async inserts.
/// Async inserts will have different deduplication policy. We use a vector of "block ids" to
/// identify different async inserts inside the same part. It will remove the duplicate inserts
/// when it encounters lock and retries.
class ReplicatedMergeTreeSink : public SinkToStorage
{
public:
    ReplicatedMergeTreeSink(
        bool async_insert_,
        StorageReplicatedMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        size_t quorum_,
        size_t quorum_timeout_ms_,
        size_t max_parts_per_block_,
        bool quorum_parallel_,
        bool majority_quorum_,
        ContextPtr context_,
        // special flag to determine the ALTER TABLE ATTACH PART without the query context,
        // needed to set the special LogEntryType::ATTACH_PART
        bool is_attach_ = false,
        bool allow_attach_while_readonly_ = false);

    ~ReplicatedMergeTreeSink() override;

    void onStart() override;
    void consume(Chunk & chunk) override;
    void onFinish() override;

    String getName() const override { return "ReplicatedMergeTreeSink"; }

    /// For ATTACHing existing data on filesystem.
    bool writeExistingPart(MergeTreeData::MutableDataPartPtr & part);

protected:
    virtual void finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper);
    virtual TemporaryPartPtr writeNewTempPart(BlockWithPartition & block);

    ZooKeeperWithFaultInjectionPtr createKeeper(String name);

    std::vector<DeduplicationHash> detectConflictsInAsyncBlockIDs(const std::vector<DeduplicationHash> & deduplication_hashes);

    /// We can delay processing for previous chunk and start writing a new one.
    std::vector<DelayedPartInPartition> delayed_parts;


    /// Rename temporary part and commit to ZooKeeper.
    /// Returns a map of conflicting blocks and its actual part names if block has to be deduplicated.
    std::vector<DeduplicationHash> commitPart(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        MergeTreeData::MutableDataPartPtr & part,
        const std::vector<DeduplicationHash> & deduplication_hashes,
        const std::vector<String> & deduplication_block_ids);

    StorageReplicatedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;

    /// Checks active replicas.
    /// Returns total number of replicas.
    size_t checkQuorumPrecondition(const ZooKeeperWithFaultInjectionPtr & zookeeper);

    size_t getQuorumSize() const;
    bool isQuorumEnabled() const;
    String quorumLogMessage() const; /// Used in logs for debug purposes
    void resolveQuorum(const ZooKeeperWithFaultInjectionPtr & zookeeper, std::string actual_part_name);
    /// Wait for quorum to be satisfied on path (quorum_path) form part (part_name)
    /// Also checks that replica still alive.
    void waitForQuorum(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        const std::string & part_name,
        const std::string & quorum_path,
        int is_active_node_version,
        int host_node_version) const;

    struct QuorumInfo
    {
        String status_path;
        int is_active_node_version = -1;
        int host_node_version = -1;
    };

    QuorumInfo quorum_info;
    /// std::nullopt means use majority quorum.
    /// 0 or 1 means no quorum, larger than 1 means quorum size.
    std::optional<size_t> required_quorum_size;
    size_t quorum_replicas_num = 0;
    size_t quorum_timeout_ms;
    size_t max_parts_per_block;

    UInt64 deduplication_cache_version = 0;
    UInt64 deduplication_async_inserts_cache_version = 0;

    bool is_attach = false;
    bool allow_attach_while_readonly = false;
    bool quorum_parallel = false;
    bool deduplicate = true;
    UInt64 num_blocks_processed = 0;

    LoggerPtr log;

    ContextPtr context;
    StorageSnapshotPtr storage_snapshot;

    bool is_async_insert = true;
    InsertDeduplicationVersions insert_deduplication_version = InsertDeduplicationVersions::NEW_UNIFIED_HASHES;
};

}
