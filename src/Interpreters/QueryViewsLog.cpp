#include "QueryViewsLog.h"

#include<base/getFQDNOrHostName.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/DateLUT.h>
#include <base/types.h>

namespace DB
{
ColumnsDescription QueryViewsLogElement::getColumnsDescription()
{
    auto view_status_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"QueryStart", static_cast<Int8>(QUERY_START)},
        {"QueryFinish", static_cast<Int8>(QUERY_FINISH)},
        {"ExceptionBeforeStart", static_cast<Int8>(EXCEPTION_BEFORE_START)},
        {"ExceptionWhileProcessing", static_cast<Int8>(EXCEPTION_WHILE_PROCESSING)}});

    auto view_type_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Default", static_cast<Int8>(ViewType::DEFAULT)},
        {"Materialized", static_cast<Int8>(ViewType::MATERIALIZED)},
        {"Live", static_cast<Int8>(ViewType::LIVE)},
        {"Window", static_cast<Int8>(ViewType::WINDOW)}});

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "The date when the last event of the view happened."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "The date and time when the view finished execution."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "The date and time when the view finished execution with microseconds precision."},
        {"view_duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of view execution (sum of its stages) in milliseconds."},

        {"initial_query_id", std::make_shared<DataTypeString>(), "ID of the initial query (for distributed query execution)."},
        {"view_name", std::make_shared<DataTypeString>(), "Name of the view."},
        {"view_uuid", std::make_shared<DataTypeUUID>(), "UUID of the view."},
        {"view_type", std::move(view_type_datatype), "Type of the view. Values: 'Default' = 1 — Default views. Should not appear in this log, 'Materialized' = 2 — Materialized views, 'Live' = 3 — Live views."},
        {"view_query", std::make_shared<DataTypeString>(), "The query executed by the view."},
        {"view_target", std::make_shared<DataTypeString>(), "The name of the view target table."},

        {"read_rows", std::make_shared<DataTypeUInt64>(), "Number of read rows."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "Number of read bytes."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "Number of written rows."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "Number of written bytes."},
        {"peak_memory_usage", std::make_shared<DataTypeInt64>(), "The maximum difference between the amount of allocated and freed memory in context of this view."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "ProfileEvents that measure different metrics. The description of them could be found in the table system.events."},

        {"status", std::move(view_status_datatype), "Status of the view. Values: "
            "'QueryStart' = 1 — Successful start the view execution. Should not appear, "
            "'QueryFinish' = 2 — Successful end of the view execution, "
            "'ExceptionBeforeStart' = 3 — Exception before the start of the view execution., "
            "'ExceptionWhileProcessing' = 4 — Exception during the view execution."},
        {"exception_code", std::make_shared<DataTypeInt32>(), "Code of an exception."},
        {"exception", std::make_shared<DataTypeString>(), "Exception message."},
        {"stack_trace", std::make_shared<DataTypeString>(), "Stack trace. An empty string, if the query was completed successfully."}
    };
}

NamesAndAliases QueryViewsLogElement::getNamesAndAliases()
{
    return {
        {"ProfileEvents.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(ProfileEvents)"},
        {"ProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(ProfileEvents)"}};
}

void QueryViewsLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType()); // event_date
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(view_duration_ms);

    columns[i++]->insertData(initial_query_id.data(), initial_query_id.size());
    columns[i++]->insertData(view_name.data(), view_name.size());
    columns[i++]->insert(view_uuid);
    columns[i++]->insert(view_type);
    columns[i++]->insertData(view_query.data(), view_query.size());
    columns[i++]->insertData(view_target.data(), view_target.size());

    columns[i++]->insert(read_rows);
    columns[i++]->insert(read_bytes);
    columns[i++]->insert(written_rows);
    columns[i++]->insert(written_bytes);
    columns[i++]->insert(peak_memory_usage);

    if (profile_counters)
    {
        auto * column = columns[i++].get();
        ProfileEvents::dumpToMapColumn(*profile_counters, column, true);
    }
    else
    {
        columns[i++]->insertDefault();
    }

    columns[i++]->insert(status);
    columns[i++]->insert(exception_code);
    columns[i++]->insertData(exception.data(), exception.size());
    columns[i++]->insertData(stack_trace.data(), stack_trace.size());
}

}
