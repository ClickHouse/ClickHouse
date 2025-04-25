#include <Interpreters/InstrumentationProfilingLog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{

NamesAndTypesList InstrumentationProfilingLogElement::getNamesAndTypes()
{
    return NamesAndTypesList
    {
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"function_id", std::make_shared<DataTypeInt32>()},
        {"function_name", std::make_shared<DataTypeString>()},
        {"handler_name", std::make_shared<DataTypeString>()}
    };
}

ColumnsDescription InstrumentationProfilingLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"event_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the sampling moment."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Timestamp of the sampling moment with microseconds precision."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread identifier."},
        {"query_id", std::make_shared<DataTypeString>(), "Query identifier that can be used to get details about a query that was running from the query_log system table."},
        {"function_id", std::make_shared<DataTypeInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeString>(), "Name of theinstrumented function."},
        {"handler_name", std::make_shared<DataTypeString>(), "Handler that was patched into instrumentation points of the function."},
    };
}

NamesAndAliases InstrumentationProfilingLogElement::getNamesAndAliases()
{
    return NamesAndAliases{};
}

void InstrumentationProfilingLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(query_id);
    columns[i++]->insert(thread_id);
    columns[i++]->insert(function_id);
    columns[i++]->insert(function_name);
    columns[i++]->insert(handler_name);
}

}
