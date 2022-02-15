#include "ProfileEventsExt.h"
#include <Common/typeid_cast.h>
#include <Common/MemoryTracker.h>
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

        const char * desc = getName(event);
        key_column.insertData(desc, strlen(desc));
        value_column.insert(value);
        size++;
    }

    offsets.push_back(offsets.back() + size);
}


void dumpProfileEvents(ProfileEventsSnapshot const & snapshot, DB::MutableColumns & columns, String const & host_name)
{
    size_t rows = 0;
    auto & name_column = columns[NAME_COLUMN_INDEX];
    auto & value_column = columns[VALUE_COLUMN_INDEX];
    for (Event event = 0; event < Counters::num_counters; ++event)
    {
        Int64 value = snapshot.counters[event];

        if (value == 0)
            continue;

        const char * desc = getName(event);
        name_column->insertData(desc, strlen(desc));
        value_column->insert(value);
        rows++;
    }

    // Fill the rest of the columns with data
    for (size_t row = 0; row < rows; ++row)
    {
        size_t i = 0;
        columns[i++]->insertData(host_name.data(), host_name.size());
        columns[i++]->insert(UInt64(snapshot.current_time));
        columns[i++]->insert(UInt64{snapshot.thread_id});
        columns[i++]->insert(Type::INCREMENT);
    }
}

void dumpMemoryTracker(ProfileEventsSnapshot const & snapshot, DB::MutableColumns & columns, String const & host_name)
{
    {
        size_t i = 0;
        columns[i++]->insertData(host_name.data(), host_name.size());
        columns[i++]->insert(UInt64(snapshot.current_time));
        columns[i++]->insert(UInt64{snapshot.thread_id});
        columns[i++]->insert(Type::GAUGE);

        columns[i++]->insertData(MemoryTracker::USAGE_EVENT_NAME, strlen(MemoryTracker::USAGE_EVENT_NAME));
        columns[i++]->insert(snapshot.memory_usage);
    }
}

}
