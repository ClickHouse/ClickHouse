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
        {"name", std::make_shared<DataTypeString>()},
        {"cat", std::make_shared<DataTypeString>()},
        {"ph", std::make_shared<DataTypeString>()},
        {"pid", std::make_shared<DataTypeUInt64>()},
        {"tid", std::make_shared<DataTypeUInt64>()},
        {"ts", std::make_shared<DataTypeDateTime64>(6)},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"function_id", std::make_shared<DataTypeInt32>()},
    };
}

ColumnsDescription InstrumentationProfilingLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the instrumented function."},
        {"cat", std::make_shared<DataTypeString>(), "Category of the instrumented function."},
        {"ph", std::make_shared<DataTypeString>(), "Phase: entry or exit."},
        {"pid", std::make_shared<DataTypeUInt64>(), "Process ID."},
        {"tid", std::make_shared<DataTypeUInt64>(), "Thread ID."},
        {"ts", std::make_shared<DataTypeDateTime64>(6), "Timestamp of the sampling moment with microseconds precision"},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the sampling moment."},
        {"query_id", std::make_shared<DataTypeString>(), "Query identifier that can be used to get details about a query that was running from the query_log system table."},
        {"function_id", std::make_shared<DataTypeInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
    };
}

NamesAndAliases InstrumentationProfilingLogElement::getNamesAndAliases()
{
    return NamesAndAliases{};
}

void InstrumentationProfilingLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(function_name);
    columns[i++]->insert(category);
    columns[i++]->insert(phase);
    columns[i++]->insert(pid);
    columns[i++]->insert(tid);
    columns[i++]->insert(timestamp);
    columns[i++]->insert(event_time);
    columns[i++]->insert(query_id);
    columns[i++]->insert(function_id);
}

}
