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
    ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_,
        size_t quorum_, size_t quorum_timeout_ms_);

    void write(const Block & block) override;

    /// For ATTACHing existing data on filesystem.
    void writeExistingPart(MergeTreeData::MutableDataPartPtr & part);

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

    using Logger = Poco::Logger;
    Logger * log;
};

}
