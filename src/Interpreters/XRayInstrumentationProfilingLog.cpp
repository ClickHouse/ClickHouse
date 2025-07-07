#include <Interpreters/XRayInstrumentationProfilingLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

ColumnsDescription XRayInstrumentationProfilingLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "The date when the async insert happened."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the sampling moment."},
        {"event_time_microseconds", std::make_shared<DataTypeUInt64>(6), "Timestamp of the sampling moment with microseconds precision"},
        {"name", std::make_shared<DataTypeString>(), "Name of the instrumented function."},
        {"tid", std::make_shared<DataTypeUInt64>(), "Thread ID."},
        {"duration_microseconds", std::make_shared<DataTypeUInt64>(), "Time the function was running for in microseconds."},
        {"query_id", std::make_shared<DataTypeString>(), "Query identifier that can be used to get details about a query that was running from the query_log system table."},
        {"function_id", std::make_shared<DataTypeInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
    };
}

void XRayInstrumentationProfilingLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    auto event_date = DateLUT::instance().toDayNum(event_time).toUnderType();
    columns[i++]->insert(event_date);
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(function_name);
    columns[i++]->insert(tid);
    columns[i++]->insert(duration_microseconds);
    columns[i++]->insert(query_id);
    columns[i++]->insert(function_id);
}

}
