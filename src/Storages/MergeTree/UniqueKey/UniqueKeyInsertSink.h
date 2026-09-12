#pragma once

#include <Interpreters/InsertDeduplication.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

struct BlockWithPartition;

/// What a unique-key INSERT calls back into the sink that wrote its part. Three calls, so the
/// commit protocol depends on this and not on `MergeTreeSink`: the sink is the only implementation
/// today, and the insert path itself is not part of the protocol.
class IUniqueKeyInsertSink
{
public:
    virtual ~IUniqueKeyInsertSink() = default;

    /// Allocate the part's block number and check it against the dedup log. Returns the
    /// conflicting block ids -- empty means proceed. The holder comes back through `block_holder`
    /// so the rename can run later under the same allocation.
    virtual std::vector<std::string> allocateAndCheckBlockDedup(
        MergeTreeMutableDataPartPtr & part,
        const std::vector<DeduplicationHash> & deduplication_hashes,
        std::unique_ptr<PlainCommittingBlockHolder> & block_holder) = 0;

    /// Publish a part whose block number is already allocated, under the parts lock.
    virtual void addAllocatedPartToActiveSet(
        MergeTreeMutableDataPartPtr & part,
        std::unique_ptr<PlainCommittingBlockHolder> block_holder,
        const MergeTreeTransactionPtr & txn) = 0;

    /// Rewrite the temp part from a filtered block, for the `ignore` conflict action.
    virtual MergeTreeTemporaryPartPtr writeNewTempPart(
        BlockWithPartition & block, const MergeTreeTransactionPtr & txn) = 0;
};

}
