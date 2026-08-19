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

namespace DB {
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
        {"query_start_time", std::make_shared<DataTypeDateTime64>(6), "Query start time with microseconds precision."},
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
}
