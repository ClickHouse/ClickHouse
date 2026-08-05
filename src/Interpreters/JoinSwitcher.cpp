#include <Common/typeid_cast.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/JoinUtils.h>

namespace DB
{

JoinSwitcher::JoinSwitcher(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    const bool any_take_last_row_,
    const StatsCollectingParams & stats_collecting_params_)
    : limits(table_join_->sizeLimits())
    , switched(false)
    , table_join(table_join_)
    , right_sample_block(right_sample_block_->cloneEmpty())
{
    join = std::make_shared<HashJoin>(
        table_join, right_sample_block_, any_take_last_row_, /*reserve_num_=*/0, /*instance_id_=*/"",
        /*use_two_level_maps_=*/false, stats_collecting_params_);

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

bool JoinSwitcher::addBlockToJoin(const Block & block, bool)
{
    std::lock_guard lock(switch_mutex);

    if (switched)
        return join->addBlockToJoin(block);

    /// HashJoin with external limits check

    join->addBlockToJoin(block, false);
    size_t rows = join->getTotalRowCount();
    size_t bytes = join->getTotalByteCount();

    /// With `enable_join_in_memory_compression`, compression must get its chance before the join is
    /// abandoned for the disk-based MergeJoin, the same way it precedes external spilling in
    /// SpillingHashJoin: `addBlockToJoin(block, false)` above skips HashJoin's own shrink pass, so
    /// without this the switch decision would always see the uncompressed build size and the setting
    /// would be silently ineffective under `join_algorithm = 'auto'`. One pass over the already
    /// stored blocks is enough: if they compress, HashJoin keeps them compressed (and ignores
    /// further shrink calls); if they do not compress below the limit, re-running the pass on every
    /// subsequent insert would only burn CPU on the same data. The blocks added after the pass are
    /// compacted on insertion instead, because the forced pass does not arm that itself: arming it is
    /// unconditional, exactly as the plain `hash` path latches insert-time compaction once its own
    /// threshold fired. The pass can also win purely by reclaiming over-allocation (`cloneResized`,
    /// nothing worth compressing, so `haveCompressed` stays false), and in that shape the later blocks
    /// must stay on the compaction path too - otherwise the next limit crossing switches to the
    /// disk-based join with the tail of the build side stored uncompacted.
    if (!limits.softCheck(rows, bytes) && !compression_attempted && table_join->enableJoinInMemoryCompression())
    {
        compression_attempted = true;
        auto & hash_join = assert_cast<HashJoin &>(*join);
        hash_join.shrinkStoredBlocksToFit(bytes, /*force_optimize=*/true);
        hash_join.armCompactionForFurtherBlocks();
        rows = join->getTotalRowCount();
        bytes = join->getTotalByteCount();
    }

    if (!limits.softCheck(rows, bytes))
        return switchJoin();

    return true;
}

bool JoinSwitcher::switchJoin()
{
    HashJoin * hash_join = assert_cast<HashJoin *>(join.get());
    auto right_blocks = hash_join->releaseJoinedBlocks(true);

    /// Destroy old join & create new one.
    join = std::make_shared<MergeJoin>(table_join, std::make_shared<const Block>(right_sample_block));

    /// Consume the released blocks one by one: each is decompressed (if the hash join compressed
    /// its stored data under memory pressure) and freed right after insertion.
    bool success = true;
    while (success && !right_blocks.empty())
    {
        Block saved_block = right_blocks.next();
        success = join->addBlockToJoin(saved_block);
    }

    switched = true;
    return success;
}

}
