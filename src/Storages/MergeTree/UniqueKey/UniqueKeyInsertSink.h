#pragma once

#include <Interpreters/InsertDeduplication.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/UniqueKey/BlockAllocation.h>

#include <memory>
#include <vector>

namespace DB
{

struct BlockWithPartition;

/// What a unique-key INSERT calls back into the sink that wrote its part. Two calls, so the
/// commit protocol depends on this and not on `MergeTreeSink`: the sink is the only implementation
/// today, and the insert path itself is not part of the protocol.
class IUniqueKeyInsertSink
{
public:
    virtual ~IUniqueKeyInsertSink() = default;

    /// Allocate the part's block number and register it in the dedup log. The caller decides when:
    /// allocation takes no lock that orders it against merges.
    virtual std::unique_ptr<BlockAllocation> allocateBlock(
        MergeTreeMutableDataPartPtr & part, const std::vector<DeduplicationHash> & deduplication_hashes) = 0;

    /// Rewrite the temp part from a filtered block, for the `ignore` conflict action.
    virtual MergeTreeTemporaryPartPtr writeNewTempPart(
        BlockWithPartition & block, const MergeTreeTransactionPtr & txn) = 0;
};

}
