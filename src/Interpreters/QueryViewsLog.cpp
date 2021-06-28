#include "QueryViewsLog.h"

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ProfileEventsExt.h>
#include <common/DateLUT.h>
#include <common/types.h>

namespace DB
{
Block QueryViewsLogElement::createBlock()
{
    auto view_status_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"QueryStart", static_cast<Int8>(QUERY_START)},
        {"QueryFinish", static_cast<Int8>(QUERY_FINISH)},
        {"ExceptionBeforeStart", static_cast<Int8>(EXCEPTION_BEFORE_START)},
        {"ExceptionWhileProcessing", static_cast<Int8>(EXCEPTION_WHILE_PROCESSING)}});

    auto view_type_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Default", static_cast<Int8>(ViewType::DEFAULT)},
        {"Materialized", static_cast<Int8>(ViewType::MATERIALIZED)},
        {"Live", static_cast<Int8>(ViewType::LIVE)}});

    return {
        {std::make_shared<DataTypeDate>(), "event_date"},
        {std::make_shared<DataTypeDateTime>(), "event_time"},
        {std::make_shared<DataTypeDateTime64>(6), "event_time_microseconds"},
        {std::make_shared<DataTypeUInt64>(), "view_duration_ms"},

        {std::make_shared<DataTypeString>(), "initial_query_id"},
        {std::make_shared<DataTypeString>(), "view_name"},
        {std::make_shared<DataTypeUUID>(), "view_uuid"},
        {std::move(view_type_datatype), "view_type"},
        {std::make_shared<DataTypeString>(), "view_query"},
        {std::make_shared<DataTypeString>(), "view_target"},

        {std::make_shared<DataTypeUInt64>(), "read_rows"},
        {std::make_shared<DataTypeUInt64>(), "read_bytes"},
        {std::make_shared<DataTypeUInt64>(), "written_rows"},
        {std::make_shared<DataTypeUInt64>(), "written_bytes"},
        {std::make_shared<DataTypeInt64>(), "peak_memory_usage"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "ProfileEvents.Names"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "ProfileEvents.Values"},

        {std::move(view_status_datatype), "status"},
        {std::make_shared<DataTypeInt32>(), "exception_code"},
        {std::make_shared<DataTypeString>(), "exception"},
        {std::make_shared<DataTypeString>(), "stack_trace"}};
}

void QueryViewsLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

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
        auto * column_names = columns[i++].get();
        auto * column_values = columns[i++].get();
        ProfileEvents::dumpToArrayColumns(*profile_counters, column_names, column_values, true);
    }
    else
    {
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
    }

    columns[i++]->insert(status);
    columns[i++]->insert(exception_code);
    columns[i++]->insertData(exception.data(), exception.size());
    columns[i++]->insertData(stack_trace.data(), stack_trace.size());
}

}
