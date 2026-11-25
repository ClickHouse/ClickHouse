#include <Interpreters/HashJoin/JoinGrowthBudget.h>

#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <bit>

namespace ProfileEvents
{
    extern const Event JoinRobinHoodSwitchesOnMemoryPressure;
}

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

namespace
{

void raiseTo(std::atomic<size_t> & value, size_t to)
{
    size_t current = value.load(std::memory_order_relaxed);
    while (current < to && !value.compare_exchange_weak(current, to, std::memory_order_relaxed))
    {
    }
}

}

size_t JoinGrowthBudget::bucketsToGrowFrom(size_t size_degree) const
{
    size_t result = 0;
    for (const auto & table : tables)
        if (table.largest_size_degree.load(std::memory_order_relaxed) >= size_degree)
            result += buckets - std::min(buckets, table.grown_from_size[size_degree].load(std::memory_order_relaxed));
    return result;
}

bool JoinGrowthBudget::Table::preferSmallerTable(size_t growth_cells, size_t bucket_cells)
{
    if (join.latched.load(std::memory_order_relaxed))
    {
        ProfileEvents::increment(ProfileEvents::JoinRobinHoodSwitchesOnMemoryPressure);
        return true;
    }

    /// The sizes are powers of two, so their bit widths minus one are below 64.
    const size_t size_degree = std::bit_width(bucket_cells) - 1;
    raiseTo(largest_size_degree, size_degree);
    const size_t step_bytes = growth_cells * join.cell_size * join.bucketsToGrowFrom(size_degree);
    const size_t transient_bytes = bucket_cells * join.cell_size * join.num_slots;
    if (join.crossesLimits(step_bytes, transient_bytes))
        return true;

    grown_from_size[size_degree].fetch_add(1, std::memory_order_relaxed);
    raiseTo(largest_size_degree, std::bit_width(bucket_cells + growth_cells) - 1);
    return false;
}

bool JoinGrowthBudget::crossesLimits(size_t step_bytes, size_t transient_bytes)
{
    const size_t join_bytes = total_bytes->load(std::memory_order_relaxed);
    const bool crosses_spill_threshold = max_bytes_before_external_join
        && reachesSpillThreshold(join_bytes + futureStoredBytes() + step_bytes, max_bytes_before_external_join);

    const auto available_memory = getAvailableMemoryForSpilling();
    const bool crosses_memory_limit = available_memory && *available_memory <= step_bytes + transient_bytes;

    if (!crosses_spill_threshold && !crosses_memory_limit)
        return false;

    latched.store(true, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::JoinRobinHoodSwitchesOnMemoryPressure);
    LOG_DEBUG(
        getLogger("JoinGrowthBudget"),
        "Hash tables stay smaller: a growth step of {} would take {}",
        ReadableSize(step_bytes),
        crosses_spill_threshold ? fmt::format(
                                      "the join, now {}, to half of its spill threshold of {}",
                                      ReadableSize(join_bytes),
                                      ReadableSize(max_bytes_before_external_join))
                                : fmt::format("the query over its memory limit, {} available", ReadableSize(*available_memory)));
    return true;
}

}
