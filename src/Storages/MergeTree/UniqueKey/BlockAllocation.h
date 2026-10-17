#pragma once

#include <Interpreters/InsertDeduplication.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Logger.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
class MergeTreeDeduplicationLog;

/// An INSERT's block number and its entry in the dedup log, from allocation until the part commits.
class BlockAllocation
{
public:
    BlockAllocation(
        LoggerPtr log,
        std::unique_ptr<PlainCommittingBlockHolder> block_holder_,
        MergeTreeDeduplicationLog * deduplication_log_,
        IMergeTreeDataPart & part,
        const std::vector<DeduplicationHash> & deduplication_hashes);

    ~BlockAllocation();

    /// Block ids already in the dedup log: the block is a replay, and nothing is published.
    const std::vector<std::string> & dedupConflicts() const { return dedup_conflicts; }
    /// The parts that already hold those block ids, in the same order.
    const std::vector<MergeTreePartInfo> & dedupConflictParts() const { return dedup_conflict_parts; }

    void commit() { committed = true; }

private:
    std::unique_ptr<PlainCommittingBlockHolder> block_holder;
    MergeTreeDeduplicationLog * deduplication_log;
    MergeTreePartInfo part_info;
    std::vector<std::string> dedup_conflicts;
    std::vector<MergeTreePartInfo> dedup_conflict_parts;
    bool registered = false;
    bool committed = false;
};

}
