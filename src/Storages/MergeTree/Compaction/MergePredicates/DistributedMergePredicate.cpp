#include <Storages/MergeTree/Compaction/MergePredicates/DistributedMergePredicate.h>

#include <base/defines.h>

namespace DB
{

CommittingBlocks getCommittingBlocks(zkutil::ZooKeeperPtr & zookeeper, const std::string & zookeeper_path, const std::optional<PartitionIdsHint> & partition_ids_hint)
{
    CommittingBlocks committing_blocks;

    /// Load current inserts
    /// Hint avoids listing partitions that we don't really need.
    /// Dropped (or cleaned up by TTL) partitions are never removed from ZK,
    /// so without hint it can do a few thousands requests (if not using MultiRead).
    Strings partitions;
    if (!partition_ids_hint)
        partitions = zookeeper->getChildren(fs::path(zookeeper_path) / "block_numbers");
    else
        std::copy(partition_ids_hint->begin(), partition_ids_hint->end(), std::back_inserter(partitions));

    std::vector<std::string> paths;
    paths.reserve(partitions.size());
    for (const String & partition : partitions)
        paths.push_back(fs::path(zookeeper_path) / "block_numbers" / partition);

    auto locks_children = zookeeper->tryGetChildren(paths);

    for (size_t i = 0; i < partitions.size(); ++i)
    {
        auto & response = locks_children[i];
        if (response.error == Coordination::Error::ZNONODE)
        {
            if (!partition_ids_hint.has_value())
                throw Coordination::Exception::fromPath(response.error, paths[i]);

            continue;
        }

        chassert(response.error == Coordination::Error::ZOK);

        Strings partition_block_numbers = locks_children[i].names;
        for (const String & entry : partition_block_numbers)
        {
            if (!startsWith(entry, "block-"))
                continue;

            Int64 block_number = parse<Int64>(entry.substr(strlen("block-")));
            committing_blocks[partitions[i]].insert(block_number);
        }
    }

    return committing_blocks;
}

}
