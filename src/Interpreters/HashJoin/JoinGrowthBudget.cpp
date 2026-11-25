#include <Interpreters/HashJoin/JoinGrowthBudget.h>

#include <Common/MemoryTrackerUtils.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <bit>

namespace DB
{

void JoinGrowthBudget::configure(
    const std::atomic<size_t> & total_bytes_,
    const std::atomic<size_t> & stored_bytes_,
    const std::atomic<size_t> & stored_rows_,
    size_t expected_rows_,
    size_t cell_size_,
    size_t buckets_,
    size_t num_slots_)
{
    total_bytes = &total_bytes_;
    stored_bytes = &stored_bytes_;
    stored_rows = &stored_rows_;
    expected_rows = expected_rows_;
    cell_size = cell_size_;
    buckets = buckets_;
    num_slots = num_slots_;
}

size_t JoinGrowthBudget::futureStoredBytes() const
{
    const size_t rows = stored_rows->load(std::memory_order_relaxed);
    if (!expected_rows || !rows || rows >= expected_rows)
        return 0;
    return stored_bytes->load(std::memory_order_relaxed) / rows * (expected_rows - rows);
}

bool JoinGrowthBudget::preferSmallerTable(size_t growth_cells, size_t bucket_cells)
{
    if (latched.load(std::memory_order_relaxed))
    {
        switch_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    /// `bucket_cells` is a power of two, so its bit width minus one is below 64.
    auto & grown = grown_from_size[std::bit_width(bucket_cells) - 1];
    /// Every bucket that has not grown from this size is about to take the same step. Those that did are in the counts already.
    const size_t step_bytes = growth_cells * cell_size * (buckets - std::min(buckets, grown.load(std::memory_order_relaxed)));
    /// The old buffers stay charged while the new ones are allocated.
    const size_t transient_bytes = bucket_cells * cell_size * num_slots;

    const size_t join_bytes = total_bytes->load(std::memory_order_relaxed);
    const bool crosses_spill_threshold = max_bytes_before_external_join
        && reachesSpillThreshold(join_bytes + futureStoredBytes() + step_bytes, max_bytes_before_external_join);

    const auto available_memory = getAvailableMemoryForSpilling();
    const bool crosses_memory_limit = available_memory && *available_memory <= step_bytes + transient_bytes;

    if (!crosses_spill_threshold && !crosses_memory_limit)
    {
        grown.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    latched.store(true, std::memory_order_relaxed);
    switch_count.fetch_add(1, std::memory_order_relaxed);
    LOG_DEBUG(
        getLogger("JoinGrowthBudget"),
        "Hash tables stay smaller: a growth step of {} would take the join, now {}, over {}",
        ReadableSize(step_bytes),
        ReadableSize(join_bytes),
        crosses_spill_threshold ? fmt::format("the spill threshold of {}", ReadableSize(max_bytes_before_external_join))
                                : fmt::format("the memory limit, {} available", ReadableSize(*available_memory)));
    return true;
}

}
