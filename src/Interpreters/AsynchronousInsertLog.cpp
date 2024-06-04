#include <Interpreters/AsynchronousInsertLog.h>

#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>


namespace DB
{

ColumnsDescription AsynchronousInsertLogElement::getColumnsDescription()
{
    auto type_status = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Ok",           static_cast<Int8>(Status::Ok)},
            {"ParsingError", static_cast<Int8>(Status::ParsingError)},
            {"FlushError",   static_cast<Int8>(Status::FlushError)},
        });

    auto type_data_kind = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Parsed",       static_cast<Int8>(DataKind::Parsed)},
            {"Preprocessed", static_cast<Int8>(DataKind::Preprocessed)},
        });

    return ColumnsDescription{
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "The date when the async insert happened."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "The date and time when the async insert finished execution."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "The date and time when the async insert finished execution with microseconds precision."},

        {"query", std::make_shared<DataTypeString>(), "Query string."},
        {"database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "The name of the database the table is in."},
        {"table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Table name."},
        {"format", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Format name."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the initial query."},
        {"bytes", std::make_shared<DataTypeUInt64>(), "Number of inserted bytes."},
        {"rows", std::make_shared<DataTypeUInt64>(), "Number of inserted rows."},
        {"exception", std::make_shared<DataTypeString>(), "Exception message."},
        {"status", type_status, "Status of the view. Values: 'Ok' = 1 — Successful insert, 'ParsingError' = 2 — Exception when parsing the data, 'FlushError' = 3 — Exception when flushing the data"},
        {"data_kind", type_data_kind, "The status of the data. Value: 'Parsed' and 'Preprocessed'."},

        {"flush_time", std::make_shared<DataTypeDateTime>(), "The date and time when the flush happened."},
        {"flush_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "The date and time when the flush happened with microseconds precision."},
        {"flush_query_id", std::make_shared<DataTypeString>(), "ID of the flush query."},
        {"timeout_milliseconds", std::make_shared<DataTypeUInt64>(), "The adaptive timeout calculated for this entry."},
    };
}

void AsynchronousInsertLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    auto event_date = DateLUT::instance().toDayNum(event_time).toUnderType();
    columns[i++]->insert(event_date);
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(query_for_logging);
    columns[i++]->insert(database);
    columns[i++]->insert(table);
    columns[i++]->insert(format);
    columns[i++]->insert(query_id);
    columns[i++]->insert(bytes);
    columns[i++]->insert(rows);
    columns[i++]->insert(exception);
    columns[i++]->insert(status);
    columns[i++]->insert(data_kind);

    columns[i++]->insert(flush_time);
    columns[i++]->insert(flush_time_microseconds);
    columns[i++]->insert(flush_query_id);
    columns[i++]->insert(timeout_milliseconds);
}

}
