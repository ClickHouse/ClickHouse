#include <base/getFQDNOrHostName.h>
#include <Interpreters/TraceLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/ClickHouseRevision.h>
#include <Common/SymbolIndex.h>


namespace DB
{

using TraceDataType = TraceLogElement::TraceDataType;

const TraceDataType::Values TraceLogElement::trace_values =
{
    {"Real", static_cast<UInt8>(TraceType::Real)},
    {"CPU", static_cast<UInt8>(TraceType::CPU)},
    {"Memory", static_cast<UInt8>(TraceType::Memory)},
    {"MemorySample", static_cast<UInt8>(TraceType::MemorySample)},
    {"MemoryPeak", static_cast<UInt8>(TraceType::MemoryPeak)},
    {"ProfileEvent", static_cast<UInt8>(TraceType::ProfileEvent)},
};

ColumnsDescription TraceLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of sampling moment."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the sampling moment."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Timestamp of the sampling moment with microseconds precision."},
        {"timestamp_ns", std::make_shared<DataTypeUInt64>(), "Timestamp of the sampling moment in nanoseconds."},
        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse server build revision."},
        {"trace_type", std::make_shared<TraceDataType>(trace_values), "Trace type: "
            "`Real` represents collecting stack traces by wall-clock time. "
            "`CPU` represents collecting stack traces by CPU time. "
            "`Memory` represents collecting allocations and deallocations when memory allocation exceeds the subsequent watermark. "
            "`MemorySample` represents collecting random allocations and deallocations. "
            "`MemoryPeak` represents collecting updates of peak memory usage. "
            "`ProfileEvent` represents collecting of increments of profile events."
        },
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread identifier."},
        {"query_id", std::make_shared<DataTypeString>(), "Query identifier that can be used to get details about a query that was running from the query_log system table."},
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process."},
        {"size", std::make_shared<DataTypeInt64>(), "For trace types Memory, MemorySample or MemoryPeak is the amount of memory allocated, for other trace types is 0."},
        {"ptr", std::make_shared<DataTypeUInt64>(), "The address of the allocated chunk."},
        {"event", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "For trace type ProfileEvent is the name of updated profile event, for other trace types is an empty string."},
        {"increment", std::make_shared<DataTypeInt64>(), "For trace type ProfileEvent is the amount of increment of profile event, for other trace types is 0."},
    };
}

NamesAndAliases TraceLogElement::getNamesAndAliases()
{
    String build_id_hex;
#if defined(__ELF__) && !defined(OS_FREEBSD)
    build_id_hex = SymbolIndex::instance().getBuildIDHex();
#endif
    return
    {
        {"build_id", std::make_shared<DataTypeString>(), "\'" + build_id_hex + "\'"},
    };
}

void TraceLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(timestamp_ns);
    columns[i++]->insert(ClickHouseRevision::getVersionRevision());
    columns[i++]->insert(static_cast<UInt8>(trace_type));
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(trace);
    columns[i++]->insert(size);
    columns[i++]->insert(ptr);

    String event_name;
    if (event != ProfileEvents::end())
        event_name = ProfileEvents::getName(event);

    columns[i++]->insert(event_name);
    columns[i++]->insert(increment);
}

}
