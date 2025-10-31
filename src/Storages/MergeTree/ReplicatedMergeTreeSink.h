#pragma once

#include <string>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
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

        TemporaryPartPtr temp_part;
        UInt64 elapsed_ns;
        ProfileEvents::Counters part_counters;


        Partition() = default;
        Partition(
            LoggerPtr log_,
            TemporaryPartPtr && temp_part_,
            UInt64 elapsed_ns_,
            BlockWithPartition && block_,
            ProfileEvents::Counters && part_counters_);
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
        bool deduplicate_,
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
    /// Returns a list of conflicting async blocks and true if the whole parts was deduplicated
    std::pair<std::set<String>, bool> commitPart(
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

    size_t quorum_timeout_ms;
    size_t max_parts_per_block;

    UInt64 cache_version = 0;

    bool is_attach = false;
    bool allow_attach_while_readonly = false;
    bool quorum_parallel = false;
    const bool deduplicate = true;
    UInt64 num_blocks_processed = 0;

    LoggerPtr log;

    ContextPtr context;
    StorageSnapshotPtr storage_snapshot;

    bool is_async_insert = true;
};

}
