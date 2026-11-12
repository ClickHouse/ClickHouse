#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Parses a LogsQL query (the query language of VictoriaLogs) and translates it
/// into a SELECT query over the table given by the `logsql_database` and `logsql_table` settings.
///
/// Even when the dialect is disabled, this parser still parses SET queries,
/// so that `SET dialect = 'clickhouse'` always works and users cannot lock themselves out.
class ParserLogsQLQuery final : public IParserBase
{
public:
    ParserLogsQLQuery(
        String database_,
        String table_,
        String time_column_,
        String msg_column_,
        const char * raw_begin_,
        const char * raw_end_,
        bool feature_enabled_,
        size_t max_parser_depth_,
        size_t max_query_size_)
        : database(std::move(database_))
        , table(std::move(table_))
        , time_column(std::move(time_column_))
        , msg_column(std::move(msg_column_))
        , raw_begin(raw_begin_)
        , raw_end(raw_end_)
        , feature_enabled(feature_enabled_)
        , max_parser_depth(max_parser_depth_)
        , max_query_size(max_query_size_)
    {
    }

    const char * getName() const override { return "LogsQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    String database;
    String table;
    String time_column;
    String msg_column;

    /// The LogsQL text is parsed from the raw query string, because the ClickHouse Lexer
    /// cannot tokenize LogsQL correctly (e.g. `foo:=bar`, `_time:5m`, `range[1, 10)`).
    /// `raw_begin` is the start of the raw query text, before any leading whitespace
    /// or comments: the `max_query_size` budget is measured from it, like in the SQL path.
    const char * raw_begin;
    const char * raw_end;

    bool feature_enabled;
    size_t max_parser_depth;
    size_t max_query_size;
};

}
