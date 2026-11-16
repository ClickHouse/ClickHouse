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

    /// Names `part` after the allocated block number
    void assignTo(IMergeTreeDataPart & part) const;

    void commit() { committed = true; }

private:
    std::unique_ptr<PlainCommittingBlockHolder> block_holder;
    MergeTreeDeduplicationLog * deduplication_log;
    MergeTreePartInfo part_info;
    std::vector<std::string> dedup_conflicts;
    bool registered = false;
    bool committed = false;
};

}
