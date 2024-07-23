#include <Interpreters/TraceLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/ClickHouseRevision.h>


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

NamesAndTypesList TraceLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"timestamp_ns", std::make_shared<DataTypeUInt64>()},
        {"revision", std::make_shared<DataTypeUInt32>()},
        {"trace_type", std::make_shared<TraceDataType>(trace_values)},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"size", std::make_shared<DataTypeInt64>()},
        {"ptr", std::make_shared<DataTypeUInt64>()},
        {"event", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"increment", std::make_shared<DataTypeInt64>()},
    };
}

void TraceLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

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
