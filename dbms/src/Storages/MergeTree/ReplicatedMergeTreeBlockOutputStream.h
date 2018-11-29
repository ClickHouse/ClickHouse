#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Core/Types.h>


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
    ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_, size_t quorum_, size_t quorum_timeout_ms_,
                                         bool deduplicate_);

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

    StorageReplicatedMergeTree & storage;
    size_t quorum;
    size_t quorum_timeout_ms;

    bool deduplicate = true;
    bool last_block_is_duplicate = false;

    using Logger = Poco::Logger;
    Logger * log;
};

}
