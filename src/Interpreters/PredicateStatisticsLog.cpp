#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PredicateStatisticsLog.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/getFQDNOrHostName.h>


namespace DB
{

ColumnsDescription PredicateStatisticsLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    return ColumnsDescription
    {
        {
            "hostname",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Timestamp when this log entry was written."
        },
        {
            "database",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Database name of the target table."
        },
        {
            "table",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Table name of the target table."
        },
        {
            "column_name",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Column referenced in the predicate atom."
        },
        {
            "predicate_class",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Predicate classification: Equality, Range, In, LikeSubstring, IsNull, or Other."
        },
        {
            "function_name",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Original function name from the actions DAG (e.g., equals, less)."
        },
        {
            "input_rows",
            std::make_shared<DataTypeUInt64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Number of rows entering the filter for this chunk."
        },
        {
            "passed_rows",
            std::make_shared<DataTypeUInt64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Number of rows passing the filter for this chunk."
        },
        {
            "selectivity",
            std::make_shared<DataTypeFloat64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Selectivity ratio: passed_rows / input_rows."
        },
        {
            "query_id",
            std::make_shared<DataTypeString>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Query ID for linking back to query_log."
        }
    };
}

void PredicateStatisticsLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(event_date);
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(database);
    columns[column_idx++]->insert(table);
    columns[column_idx++]->insert(column_name);
    columns[column_idx++]->insert(predicate_class);
    columns[column_idx++]->insert(function_name);
    columns[column_idx++]->insert(input_rows);
    columns[column_idx++]->insert(passed_rows);
    columns[column_idx++]->insert(selectivity);
    columns[column_idx++]->insert(query_id);
}

}
