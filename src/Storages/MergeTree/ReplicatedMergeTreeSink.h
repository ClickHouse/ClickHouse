#pragma once

#include <string>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Interpreters/InsertDeduplication.h>
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

class StorageReplicatedMergeTree;
struct BlockWithPartition;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

struct MergeTreeTemporaryPart;
using TemporaryPartPtr = std::unique_ptr<MergeTreeTemporaryPart>;

struct ReplicatedMergeTreeDelayedChunk
{
    struct Partition
    {
        LoggerPtr log;
        BlockWithPartition block_with_partition;

        DeduplicationInfo::Ptr deduplication_info;
        TemporaryPartPtr temp_part;
        UInt64 elapsed_ns;
        ProfileEvents::Counters part_counters;
    };

    ReplicatedMergeTreeDelayedChunk() = default;
    explicit ReplicatedMergeTreeDelayedChunk(size_t replicas_num_);

    size_t replicas_num = 0;
    std::vector<Partition> partitions;
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
    virtual void finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper);
    virtual TemporaryPartPtr writeNewTempPart(BlockWithPartition & block);

    std::vector<std::string> detectConflictsInAsyncBlockIDs(const std::vector<std::string> & ids);

    /// We can delay processing for previous chunk and start writing a new one.
    std::unique_ptr<ReplicatedMergeTreeDelayedChunk> delayed_chunk;

    struct QuorumInfo
    {
        String status_path;
        int is_active_node_version = -1;
        int host_node_version = -1;
    };

    QuorumInfo quorum_info;

    /// Checks active replicas.
    /// Returns total number of replicas.
    size_t checkQuorumPrecondition(const ZooKeeperWithFaultInjectionPtr & zookeeper);

    /// Rename temporary part and commit to ZooKeeper.
    /// Returns a map of conflicting blocks and its actual part names if block has to be deduplicated.
    std::map<std::string, std::string> commitPart(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        MergeTreeData::MutableDataPartPtr & part,
        const std::vector<std::string> & block_ids,
        size_t replicas_num);


    /// Wait for quorum to be satisfied on path (quorum_path) form part (part_name)
    /// Also checks that replica still alive.
    void waitForQuorum(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        const std::string & part_name,
        const std::string & quorum_path,
        int is_active_node_version,
        int host_node_version,
        size_t replicas_num) const;

    StorageReplicatedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;

    /// Empty means use majority quorum.
    std::optional<size_t> required_quorum_size;

    size_t getQuorumSize(size_t replicas_num) const;
    bool isQuorumEnabled() const;
    String quorumLogMessage(size_t replicas_num) const; /// Used in logs for debug purposes
    void resolveQuorum(const ZooKeeperWithFaultInjectionPtr & zookeeper, size_t replicas_num, std::string actual_part_name);

    size_t quorum_timeout_ms;
    size_t max_parts_per_block;

    UInt64 cache_version = 0;

    bool is_attach = false;
    bool allow_attach_while_readonly = false;
    bool quorum_parallel = false;
    bool deduplicate = true;
    UInt64 num_blocks_processed = 0;

    std::set<std::string> parts_to_wait_for_quorum;

    LoggerPtr log;

    ContextPtr context;
    StorageSnapshotPtr storage_snapshot;

    bool is_async_insert = true;
};

}
