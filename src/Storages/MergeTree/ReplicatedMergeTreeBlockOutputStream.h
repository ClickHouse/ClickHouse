#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <common/types.h>


namespace Poco { class Logger; }

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageReplicatedMergeTree;


class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    ReplicatedMergeTreeBlockOutputStream(
        StorageReplicatedMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        size_t quorum_,
        size_t quorum_timeout_ms_,
        size_t max_parts_per_block_,
        bool quorum_parallel_,
        bool deduplicate_,
        ContextPtr context_,
        // special flag to determine the ALTER TABLE ATTACH PART without the query context,
        // needed to set the special LogEntryType::ATTACH_PART
        bool is_attach_ = false);

    Block getHeader() const override;
    void writePrefix() override;
    void write(const Block & block) override;

    /// For ATTACHing existing data on filesystem.
    void writeExistingPart(MergeTreeData::MutableDataPartPtr & part);

    /// For proper deduplication in MaterializedViews
    bool lastBlockIsDuplicate() const
    {
        return last_block_is_duplicate;
    }

private:
    struct QuorumInfo
    {
        String status_path;
        String is_active_node_value;
        int is_active_node_version = -1;
        int host_node_version = -1;
    };

    QuorumInfo quorum_info;
    void checkQuorumPrecondition(zkutil::ZooKeeperPtr & zookeeper);

    /// Rename temporary part and commit to ZooKeeper.
    void commitPart(zkutil::ZooKeeperPtr & zookeeper, MergeTreeData::MutableDataPartPtr & part, const String & block_id);

    /// Wait for quorum to be satisfied on path (quorum_path) form part (part_name)
    /// Also checks that replica still alive.
    void waitForQuorum(
        zkutil::ZooKeeperPtr & zookeeper, const std::string & part_name,
        const std::string & quorum_path, const std::string & is_active_node_value) const;

    StorageReplicatedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t quorum;
    size_t quorum_timeout_ms;
    size_t max_parts_per_block;

    bool is_attach = false;
    bool quorum_parallel = false;
    bool deduplicate = true;
    bool last_block_is_duplicate = false;

    using Logger = Poco::Logger;
    Poco::Logger * log;

    ContextPtr context;
};

}
