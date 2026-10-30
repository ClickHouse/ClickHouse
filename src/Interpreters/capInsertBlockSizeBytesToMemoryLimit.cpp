#include <Interpreters/capInsertBlockSizeBytesToMemoryLimit.h>

#include <Common/MemoryTracker.h>

#include <algorithm>

namespace DB
{

/// `min_insert_block_size_bytes` is not the amount of memory an `INSERT` needs, it is the size of one
/// block; the pipeline holds several of those at once. The squashing transform accumulates the next
/// block while the previous one is being concatenated, and the concatenated block is still alive in
/// the sink (where the writer additionally holds a permuted copy of one column at a time). Blocks are
/// also measured by their logical byte size while the columns behind them are `PODArray`s whose
/// capacity is rounded up, so a "256 MiB" block costs noticeably more than 256 MiB of memory.
/// Measured end to end, `INSERT ... SELECT` into a `MergeTree` table peaks at around five times
/// `min_insert_block_size_bytes` per insert stream.
///
/// Cap the threshold so those copies stay a bounded share of the server's memory limit. On a server
/// with several GiB of RAM or more this leaves the setting untouched; on a small one it is the
/// difference between the insert completing and failing with `MEMORY_LIMIT_EXCEEDED`.
///
/// One operation can run several squashing pipelines at once: an `INSERT` squashes for the target
/// table and again for every dependent materialized view (the view query result in every sink stream,
/// and possibly once more before the view's sink), and a merge or a mutation squashes once
/// for the projection input and once per rebuilt projection. Each of them holds its own copies, so
/// the share is split between `num_simultaneous_squashes` of them.
size_t capInsertBlockSizeBytesToMemoryLimit(size_t min_block_size_bytes, size_t num_simultaneous_squashes)
{
    const Int64 memory_limit = total_memory_tracker.getHardLimit();
    if (memory_limit <= 0)
        return min_block_size_bytes;

    /// Use 90% of the hard limit as the budget, leaving headroom for spikes and overhead, and allow the
    /// insert's blocks to take at most 40% of that budget - the rest is for the source (a format parser
    /// or another table's read pipeline), background merges and the server itself.
    static constexpr double budget_of_the_limit = 0.9;
    static constexpr double share_for_insert_blocks = 0.4;
    static constexpr double copies_held_by_the_pipeline = 5;

    const double budget = static_cast<double>(memory_limit) * budget_of_the_limit;
    const double copies = copies_held_by_the_pipeline * static_cast<double>(std::max<size_t>(1, num_simultaneous_squashes));
    return std::min(min_block_size_bytes, static_cast<size_t>(budget * share_for_insert_blocks / copies));
}

}
