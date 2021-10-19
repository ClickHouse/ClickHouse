#include "ProfileEventsExt.h"
#include <Common/typeid_cast.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>

namespace ProfileEvents
{

std::shared_ptr<DB::DataTypeEnum8> TypeEnum = std::make_shared<DB::DataTypeEnum8>(DB::DataTypeEnum8::Values{
    { "increment", static_cast<Int8>(INCREMENT)},
    { "gauge",     static_cast<Int8>(GAUGE)},
});

/// Put implementation here to avoid extra linking dependencies for clickhouse_common_io
void dumpToMapColumn(const Counters::Snapshot & counters, DB::IColumn * column, bool nonzero_only)
{
    auto * column_map = column ? &typeid_cast<DB::ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getNestedColumn().getOffsets();
    auto & tuple_column = column_map->getNestedData();
    auto & key_column = tuple_column.getColumn(0);
    auto & value_column = tuple_column.getColumn(1);

    size_t size = 0;
    for (Event event = 0; event < Counters::num_counters; ++event)
    {
        UInt64 value = counters[event];

        if (nonzero_only && 0 == value)
            continue;

        const char * desc = ProfileEvents::getName(event);
        key_column.insertData(desc, strlen(desc));
        value_column.insert(value);
        size++;
    }

    offsets.push_back(offsets.back() + size);
}

}
