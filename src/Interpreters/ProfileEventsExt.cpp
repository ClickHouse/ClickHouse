#include "ProfileEventsExt.h"
#include <Common/typeid_cast.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>

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
    for (Event event = Event(0); event < Counters::num_counters; ++event)
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

/// Add records about provided non-zero ProfileEvents::Counters.
static void dumpProfileEvents(ProfileEventsSnapshot const & snapshot, DB::MutableColumns & columns, String const & host_name)
{
    size_t rows = 0;
    auto & name_column = columns[NAME_COLUMN_INDEX];
    auto & value_column = columns[VALUE_COLUMN_INDEX];
    for (Event event = Event(0); event < Counters::num_counters; ++event)
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
        columns[i++]->insert(static_cast<UInt64>(snapshot.current_time));
        columns[i++]->insert(UInt64{snapshot.thread_id});
        columns[i++]->insert(Type::INCREMENT);
    }
}

static void dumpMemoryTracker(ProfileEventsSnapshot const & snapshot, DB::MutableColumns & columns, String const & host_name)
{
    size_t i = 0;
    columns[i++]->insertData(host_name.data(), host_name.size());
    columns[i++]->insert(static_cast<UInt64>(snapshot.current_time));
    columns[i++]->insert(static_cast<UInt64>(snapshot.thread_id));
    columns[i++]->insert(Type::GAUGE);
    columns[i++]->insertData(MemoryTracker::USAGE_EVENT_NAME, strlen(MemoryTracker::USAGE_EVENT_NAME));
    columns[i]->insert(snapshot.memory_usage);

    i = 0;
    columns[i++]->insertData(host_name.data(), host_name.size());
    columns[i++]->insert(static_cast<UInt64>(snapshot.current_time));
    columns[i++]->insert(static_cast<UInt64>(snapshot.thread_id));
    columns[i++]->insert(Type::GAUGE);
    columns[i++]->insertData(MemoryTracker::PEAK_USAGE_EVENT_NAME, strlen(MemoryTracker::PEAK_USAGE_EVENT_NAME));
    columns[i]->insert(snapshot.peak_memory_usage);
}

void getProfileEvents(
    const String & host_name,
    DB::InternalProfileEventsQueuePtr profile_queue,
    DB::Block & block,
    ThreadIdToCountersSnapshot & last_sent_snapshots)
{
    using namespace DB;
    static const NamesAndTypesList column_names_and_types = {
        {"host_name", std::make_shared<DataTypeString>()},
        {"current_time", std::make_shared<DataTypeDateTime>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"type", TypeEnum},
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeInt64>()},
    };

    ColumnsWithTypeAndName temp_columns;
    for (auto const & name_and_type : column_names_and_types)
        temp_columns.emplace_back(name_and_type.type, name_and_type.name);

    block = std::move(temp_columns);
    MutableColumns columns = block.mutateColumns();
    auto thread_group = CurrentThread::getGroup();
    ThreadIdToCountersSnapshot new_snapshots;

    ProfileEventsSnapshot group_snapshot;
    {
        group_snapshot.thread_id    = 0;
        group_snapshot.current_time = time(nullptr);
        group_snapshot.memory_usage = thread_group->memory_tracker.get();
        group_snapshot.peak_memory_usage = thread_group->memory_tracker.getPeak();
        auto group_counters         = thread_group->performance_counters.getPartiallyAtomicSnapshot();
        auto prev_group_snapshot    = last_sent_snapshots.find(0);
        group_snapshot.counters     =
            prev_group_snapshot != last_sent_snapshots.end()
            ? CountersIncrement(group_counters, prev_group_snapshot->second)
            : CountersIncrement(group_counters);
        new_snapshots[0]            = std::move(group_counters);
    }
    last_sent_snapshots = std::move(new_snapshots);

    dumpProfileEvents(group_snapshot, columns, host_name);
    dumpMemoryTracker(group_snapshot, columns, host_name);

    Block curr_block;

    while (profile_queue->tryPop(curr_block))
    {
        auto curr_columns = curr_block.getColumns();
        for (size_t j = 0; j < curr_columns.size(); ++j)
            columns[j]->insertRangeFrom(*curr_columns[j], 0, curr_columns[j]->size());
    }

    bool empty = columns[0]->empty();
    if (!empty)
        block.setColumns(std::move(columns));
}

}
