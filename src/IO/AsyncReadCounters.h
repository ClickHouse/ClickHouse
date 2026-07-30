#pragma once

#include <atomic>
#include <cstddef>
#include <memory>

namespace DB
{

class IColumn;

/// Metrics for asynchronous reading feature.
///
/// All fields are atomic and updated with relaxed memory ordering: the values
/// are only used for monitoring (dumped into `system.query_log` via
/// `dumpToMapColumn`), so we don't need any synchronization between updaters
/// or between updaters and readers, only per-counter atomicity. This avoids a
/// mutex which used to show up in profiles of read-heavy workloads.
struct AsyncReadCounters
{
    /// Count current and max number of tasks in a asynchronous read pool.
    /// The tasks are requests to read the data.
    std::atomic<size_t> max_parallel_read_tasks = 0;
    std::atomic<size_t> current_parallel_read_tasks = 0;

    /// Count current and max number of tasks in a reader prefetch read pool.
    /// The tasks are calls to IMergeTreeReader::prefetch(), which does not do
    /// any reading but creates a request for read. But as we need to wait for
    /// marks to be loaded during this prefetch, we do it in a threadpool too.
    std::atomic<size_t> max_parallel_prefetch_tasks = 0;
    std::atomic<size_t> current_parallel_prefetch_tasks = 0;
    std::atomic<size_t> total_prefetch_tasks = 0;

    AsyncReadCounters() = default;

    void dumpToMapColumn(IColumn * column) const;

    /// Atomically increments `current` and updates `max` to be at least the new
    /// value of `current`, both with relaxed memory ordering.
    static void incrementAndUpdateMax(std::atomic<size_t> & current, std::atomic<size_t> & max)
    {
        size_t new_value = current.fetch_add(1, std::memory_order_relaxed) + 1;
        size_t prev_max = max.load(std::memory_order_relaxed);
        while (prev_max < new_value
               && !max.compare_exchange_weak(prev_max, new_value, std::memory_order_relaxed))
        {
        }
    }
};
using AsyncReadCountersPtr = std::shared_ptr<AsyncReadCounters>;

}
