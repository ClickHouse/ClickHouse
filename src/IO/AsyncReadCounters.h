#pragma once
#include <Core/Types.h>
#include <Columns/ColumnMap.h>

namespace DB
{

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

    AsyncReadCounters(const AsyncReadCounters & rhs)
    {
        *this = rhs;
    }

    AsyncReadCounters & operator=(const AsyncReadCounters & rhs)
    {
        std::scoped_lock lock(mutex, rhs.mutex);
        max_parallel_read_tasks = rhs.max_parallel_read_tasks;
        current_parallel_read_tasks = rhs.current_parallel_read_tasks;
        max_parallel_prefetch_tasks = rhs.max_parallel_prefetch_tasks;
        current_parallel_prefetch_tasks = rhs.current_parallel_prefetch_tasks;
        total_prefetch_tasks = rhs.total_prefetch_tasks;
        return *this;
    }

    void dumpToMapColumn(IColumn * column) const;
};

}
