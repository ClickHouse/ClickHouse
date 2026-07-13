#include <Interpreters/BackgroundSchedulePoolLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>


namespace DB
{

ColumnsDescription BackgroundSchedulePoolLogElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", low_cardinality_string, "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds precision."},

        {"query_id", std::make_shared<DataTypeString>(), "Identifier of the query associated with the background task."},
        {"database", low_cardinality_string, "Name of the database."},
        {"table", low_cardinality_string, "Name of the table."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "UUID of the table the background task belongs to."},
        {"log_name", low_cardinality_string, "Name of the background task."},

        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of the task execution in milliseconds."},

        {"error", std::make_shared<DataTypeUInt16>(), "The error code of the occurred exception."},
        {"exception", std::make_shared<DataTypeString>(), "Text message of the occurred error."},
    };
}

NamesAndAliases BackgroundSchedulePoolLogElement::getNamesAndAliases()
{
    return {};
}

void BackgroundSchedulePoolLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(query_id);
    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(table_uuid);
    columns[i++]->insert(log_name);

    columns[i++]->insert(duration_ms);

    columns[i++]->insert(error);
    columns[i++]->insert(exception);
}

}
