#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <IO/AsyncReadCounters.h>

namespace DB
{

void AsyncReadCounters::dumpToMapColumn(IColumn * column) const
{
    if (!column)
        return;
    auto & column_map = typeid_cast<DB::ColumnMap &>(*column);

    auto & offsets = column_map.getNestedColumn().getOffsets();
    auto & tuple_column = column_map.getNestedData();
    auto & key_column = typeid_cast<ColumnLowCardinality &>(tuple_column.getColumn(0));
    auto & value_column = typeid_cast<ColumnUInt64 &>(tuple_column.getColumn(1));

    size_t size = 0;
    auto load_if_not_empty = [&](std::string_view key, size_t value)
    {
        if (value)
        {
            key_column.insertData(key.data(), key.size());
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
