#include <memory>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/QueryPlanLog.h>
#include <Storages/ColumnsDescription.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/ClickHouseRevision.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryLogElement.h>
#include <Interpreters/QueryPlanProfiler.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 query_plan_max_step_description_length;
}

ColumnsDescription QueryPlanLogElement::getColumnsDescription() {
    auto query_status_datatype = std::make_shared<DataTypeEnum8>(
    DataTypeEnum8::Values
    {
        {"QueryFinish",                 static_cast<Int8>(QUERY_FINISH)},
        {"ExceptionBeforeStart",        static_cast<Int8>(EXCEPTION_BEFORE_START)},
        {"ExceptionWhileProcessing",    static_cast<Int8>(EXCEPTION_WHILE_PROCESSING)}
    });

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    return ColumnsDescription{
        {"hostname", low_cardinality_string, "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Query end date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Query end time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query end time with microseconds precision."},
        {"query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query start time with microseconds precision."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"query_string", std::make_shared<DataTypeString>(), "String of the query."},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>(), "Query duration in ms."},
        {"normalized_query_hash", std::make_shared<DataTypeUInt64>(), "A numeric hash value, such as it is identical for queries differ only by values of literals."},
        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse revision."},
        {"ascii_plan", std::make_shared<DataTypeString>(), "ASCII version of the query plan."},
        {"status", std::move(query_status_datatype), "Type of an event that occurred when executing the query. Values: `QueryFinish` — successful end of query execution, `ExceptionBeforeStart` — exception before the start of query execution, `ExceptionWhileProcessing` — exception during the query execution."},
    };
}

void QueryPlanLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(query_start_time);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insertData(query_string.data(), query_string.size());
    columns[i++]->insert(query_duration_ms);
    columns[i++]->insert(normalized_query_hash);
    columns[i++]->insert(ClickHouseRevision::getVersionRevision());
    columns[i++]->insertData(ascii_plan.data(), ascii_plan.size());
    columns[i++]->insert(status);
}

void logQueryPlan(const ContextPtr & context,
                  const QueryPlanProfiler & profiler,
                  const QueryLogElement & elem,
                  QueryPlanLogElement::Status status)
{
    auto query_plan_log = context->getQueryPlanLog();
    if (!query_plan_log)
        return;

    const auto max_description_length = context->getSettingsRef()[Setting::query_plan_max_step_description_length];

    QueryPlanLogElement plan_elem;
    plan_elem.event_time = elem.event_time;
    plan_elem.event_time_microseconds = elem.event_time_microseconds;
    plan_elem.query_start_time = elem.query_start_time_microseconds;
    plan_elem.query_id = elem.client_info.current_query_id;
    plan_elem.query_string = elem.query;
    plan_elem.query_duration_ms = elem.query_duration_ms;
    plan_elem.normalized_query_hash = elem.normalized_query_hash;
    plan_elem.ascii_plan = profiler.renderAsciiPlan(max_description_length);
    plan_elem.status = status;

    query_plan_log->add([&](QueryPlanLogElement & element) { element = std::move(plan_elem); });
}

}
