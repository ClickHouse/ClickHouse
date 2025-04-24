#include <Interpreters/InstrumentationProfilingLog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/IColumn.h>

namespace DB
{

// NamesAndTypesList InstrumentationProfilingLogElement::getNamesAndTypes()
// {
//     return {
//         {"event_time", std::make_shared<DataTypeDateTime>()},
//         {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
//         {"query_id", std::make_shared<DataTypeUUID>()},
//         {"thread_id", std::make_shared<DataTypeUInt64>()},
//         {"trace_type", std::make_shared<DataTypeUInt64>()},
//         {"function_id", std::make_shared<DataTypeInt32>()},
//         {"function_name", std::make_shared<DataTypeString>()},
//         {"handler_name", std::make_shared<DataTypeString>()}
//     };
// }

ColumnsDescription InstrumentationProfilingLogElement::getColumnsDescription()
{
    return ColumnsDescription{};
}

NamesAndAliases InstrumentationProfilingLogElement::getNamesAndAliases()
{
    return {};
}

void InstrumentationProfilingLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(query_id);
    columns[i++]->insert(thread_id);
    columns[i++]->insert(trace_type);
    columns[i++]->insert(function_id);
    columns[i++]->insert(function_name);
    columns[i++]->insert(handler_name);
}

}
