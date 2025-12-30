#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <IO/AsyncReadCounters.h>

namespace DB
{

void AsyncReadCounters::dumpToMapColumn(IColumn * column) const
{
    auto * column_map = column ? &typeid_cast<DB::ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getNestedColumn().getOffsets();
    auto & tuple_column = column_map->getNestedData();
    auto & key_column = tuple_column.getColumn(0);
    auto & value_column = tuple_column.getColumn(1);

    size_t size = 0;
    auto load_if_not_empty = [&](const auto & key, const auto & value)
    {
        if (value)
        {
            key_column.insert(key);
            value_column.insert(value);
            ++size;
        }
    };

    std::lock_guard lock(mutex);

    load_if_not_empty("max_parallel_read_tasks", max_parallel_read_tasks);
    load_if_not_empty("max_parallel_prefetch_tasks", max_parallel_prefetch_tasks);
    load_if_not_empty("total_prefetch_tasks", total_prefetch_tasks);

    offsets.push_back(offsets.back() + size);
}

}
