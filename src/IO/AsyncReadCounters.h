#pragma once

#include <mutex>

namespace DB
{

class IColumn;

/// Metrics for asynchronous reading feature.
struct AsyncReadCounters
{
    /// Count current and max number of tasks in a asynchronous read pool.
    /// The tasks are requests to read the data.
    size_t max_parallel_read_tasks = 0;
    size_t current_parallel_read_tasks = 0;

    /// Count current and max number of tasks in a reader prefetch read pool.
    /// The tasks are calls to IMergeTreeReader::prefetch(), which does not do
    /// any reading but creates a request for read. But as we need to wait for
    /// marks to be loaded during this prefetch, we do it in a threadpool too.
    size_t max_parallel_prefetch_tasks = 0;
    size_t current_parallel_prefetch_tasks = 0;
    size_t total_prefetch_tasks = 0;

    mutable std::mutex mutex;

    AsyncReadCounters() = default;

    void dumpToMapColumn(IColumn * column) const;
};
using AsyncReadCountersPtr = std::shared_ptr<AsyncReadCounters>;

}
