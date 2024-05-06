#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>


namespace Poco { class Logger; }

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageReplicatedMergeTree;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;


/// ReplicatedMergeTreeSink will sink data to replicated merge tree with deduplication.
/// The template argument "async_insert" indicates whether this sink serves for async inserts.
/// Async inserts will have different deduplication policy. We use a vector of "block ids" to
/// identify different async inserts inside the same part. It will remove the duplicate inserts
/// when it encounters lock and retries.
template<bool async_insert>
class ReplicatedMergeTreeSinkImpl : public SinkToStorage
{
public:
    ReplicatedMergeTreeSinkImpl(
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

    ~ReplicatedMergeTreeSinkImpl() override;

    void onStart() override;
    void consume(Chunk & chunk) override;
    void onFinish() override;

    String getName() const override { return "ReplicatedMergeTreeSink"; }

    /// For ATTACHing existing data on filesystem.
    bool writeExistingPart(MergeTreeData::MutableDataPartPtr & part);

    struct DelayedChunk;
private:
    std::vector<String> detectConflictsInAsyncBlockIDs(const std::vector<String> & ids);

    using BlockIDsType = std::conditional_t<async_insert, std::vector<String>, String>;

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
    std::pair<std::vector<String>, bool> commitPart(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        MergeTreeData::MutableDataPartPtr & part,
        const BlockIDsType & block_id,
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

    UInt64 chunk_dedup_seqnum = 0; /// input chunk ordinal number in case of dedup token

    /// We can delay processing for previous chunk and start writing a new one.
    std::unique_ptr<DelayedChunk> delayed_chunk;

    void finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper);
};

using ReplicatedMergeTreeSinkWithAsyncDeduplicate = ReplicatedMergeTreeSinkImpl<true>;
using ReplicatedMergeTreeSink = ReplicatedMergeTreeSinkImpl<false>;

}
