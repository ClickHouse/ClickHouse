#include <Storages/MergeTree/Compaction/MergePredicates/DistributedMergePredicate.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <IO/ReadHelpers.h>
#include <base/defines.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

CommittingBlocks getCommittingBlocks(zkutil::ZooKeeperPtr & zookeeper, const std::string & zookeeper_path, std::optional<PartitionIdsHint> & partition_ids_hint, bool with_data)
{
    CommittingBlocks committing_blocks;

    /// Load current inserts
    /// Hint avoids listing partitions that we don't really need.
    /// Dropped (or cleaned up by TTL) partitions are never removed from ZK,
    /// so without hint it can do a few thousands requests (if not using MultiRead).
    Strings partitions;
    if (!partition_ids_hint)
    {
        partitions = zookeeper->getChildren(fs::path(zookeeper_path) / "block_numbers");
    }
    else
    {
        NameSet partitions_set = *partition_ids_hint;

        /// We need to get committing blocks for original partitions of patch parts
        /// because the corretness of merge of patch parts depends on them.
        for (const auto & partition_id : *partition_ids_hint)
        {
            if (isPatchPartitionId(partition_id))
                partitions_set.insert(getOriginalPartitionIdOfPatch(partition_id));
        }

        partitions.reserve(partitions_set.size());
        std::move(partitions_set.begin(), partitions_set.end(), std::back_inserter(partitions));
    }

    std::vector<std::string> paths;
    paths.reserve(partitions.size());
    for (const String & partition : partitions)
        paths.push_back(fs::path(zookeeper_path) / "block_numbers" / partition);

    auto locks_children = zookeeper->tryGetChildren(paths);

    std::vector<String> block_partitions;
    std::vector<Int64> block_numbers;
    std::vector<String> block_data_paths;

    for (size_t i = 0; i < partitions.size(); ++i)
    {
        auto & response = locks_children[i];
        if (response.error != Coordination::Error::ZOK && !partition_ids_hint)
            throw Coordination::Exception::fromPath(response.error, paths[i]);

        if (response.error != Coordination::Error::ZOK)
        {
            /// Probably a wrong hint was provided (it's ok if a user passed non-existing partition to OPTIMIZE)
            partition_ids_hint->erase(partitions[i]);
            continue;
        }

        Strings partition_block_numbers = locks_children[i].names;
        for (const String & entry : partition_block_numbers)
        {
            if (!startsWith(entry, "block-"))
                continue;

            Int64 block_number = parse<Int64>(entry.substr(strlen("block-")));

            block_partitions.push_back(partitions[i]);
            block_numbers.push_back(block_number);
            block_data_paths.push_back(fs::path(paths[i]) / entry);
        }
    }

    if (with_data)
    {
        auto blocks_data = zookeeper->tryGet(block_data_paths);

        for (size_t i = 0; i < blocks_data.size(); ++i)
        {
            auto & response = blocks_data[i];
            if (response.error == Coordination::Error::ZNONODE)
            {
                committing_blocks[block_partitions[i]].insert(CommittingBlock(CommittingBlock::Op::Unknown, block_numbers[i]));
                continue;
            }

            if (response.error != Coordination::Error::ZOK)
                throw Coordination::Exception::fromPath(response.error, block_data_paths[i]);

            auto block_data = deserializeCommittingBlockOpFromString(response.data);
            committing_blocks[block_partitions[i]].insert(CommittingBlock(block_data, block_numbers[i]));
        }
    }
    else
    {
        for (size_t i = 0; i < block_partitions.size(); ++i)
            committing_blocks[block_partitions[i]].insert(CommittingBlock(CommittingBlock::Op::Unknown, block_numbers[i]));
    }

    return committing_blocks;
}

}
