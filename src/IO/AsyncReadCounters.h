#pragma once
#include <Core/Types.h>
#include <Columns/ColumnMap.h>

namespace DB
{

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

    AsyncReadCounters(const AsyncReadCounters & rhs)
        : max_parallel_read_tasks(rhs.max_parallel_read_tasks.load())
        , current_parallel_read_tasks(rhs.current_parallel_read_tasks.load())
        , max_parallel_prefetch_tasks(rhs.max_parallel_prefetch_tasks.load())
        , current_parallel_prefetch_tasks(rhs.current_parallel_prefetch_tasks.load())
    {}

    AsyncReadCounters & operator=(const AsyncReadCounters & rhs)
    {
        max_parallel_read_tasks = rhs.max_parallel_read_tasks.load();
        current_parallel_read_tasks = rhs.current_parallel_read_tasks.load();
        max_parallel_prefetch_tasks = rhs.max_parallel_prefetch_tasks.load();
        current_parallel_prefetch_tasks = rhs.current_parallel_prefetch_tasks.load();
        return *this;
    }

    void dumpToMapColumn(IColumn * column) const
    {
        auto * column_map = column ? &typeid_cast<DB::ColumnMap &>(*column) : nullptr;
        if (!column_map)
            return;

        auto & offsets = column_map->getNestedColumn().getOffsets();
        auto & tuple_column = column_map->getNestedData();
        auto & key_column = tuple_column.getColumn(0);
        auto & value_column = tuple_column.getColumn(1);

        const size_t size = 3;

        key_column.insert("max_parallel_read_tasks");
        value_column.insert(max_parallel_read_tasks.load());
        key_column.insert("max_parallel_prefetch_tasks");
        value_column.insert(max_parallel_prefetch_tasks.load());
        key_column.insert("total_prefetch_tasks");
        value_column.insert(total_prefetch_tasks.load());

        offsets.push_back(offsets.back() + size);
    }

};

}
