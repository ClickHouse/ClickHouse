#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/ErrorCodes.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ErrorLog.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/SymbolsHelper.h>

#include <vector>

namespace DB
{

ColumnsDescription ErrorLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    DataTypePtr symbolized_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));
    return ColumnsDescription {
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
                "Event time."
            },
        {
                "code",
                std::make_shared<DataTypeInt32>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Error code."
            },
        {
                "error",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Error name."
            },
        {
                "last_error_time",
                std::make_shared<DataTypeDateTime>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "The time when the last error happened."
            },
        {
                "last_error_message",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Message for the last error."
            },
        {
                "value",
                std::make_shared<DataTypeUInt64>(),
                parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Number of errors happened in time interval."
            },
        {
                "remote",
                std::make_shared<DataTypeUInt8>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Remote exception (i.e. received during one of the distributed queries)."
            },
        {
                "last_error_query_id",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Id of a query that caused the last error (if available)."
            },
        {
                "last_error_trace",
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "A stack trace that represents a list of physical addresses where the called methods are stored."
            },
        {
                "symbols",
                symbolized_type,
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "If the symbolization is enabled, contains demangled symbol names, corresponding to the `trace`."
            },
        {
                "lines",
                symbolized_type,
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "If the symbolization is enabled, contains strings with file names with line numbers, corresponding to the `trace`."
            }
    };
}

void ErrorLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(code);
    columns[column_idx++]->insert(ErrorCodes::getName(code));
    columns[column_idx++]->insert(error_time_ms / 1000);
    columns[column_idx++]->insert(error_message);
    columns[column_idx++]->insert(value);
    columns[column_idx++]->insert(remote);
    columns[column_idx++]->insert(query_id);

    std::vector<UInt64> error_trace_array;
    error_trace_array.reserve(error_trace.size());
    for (size_t i = 0; i < error_trace.size(); ++i)
        error_trace_array.emplace_back(reinterpret_cast<UInt64>(error_trace[i]));

    columns[column_idx++]->insert(Array(error_trace_array.begin(), error_trace_array.end()));

    #if defined(__ELF__) && !defined(OS_FREEBSD)
    if (symbolize)
    {
        auto [symbols, lines] = generateArraysSymbolsLines(error_trace_array);

        columns[column_idx++]->insert(symbols);
        columns[column_idx++]->insert(lines);
    }
    else
    #endif
    {
        columns[column_idx++]->insertDefault();
        columns[column_idx++]->insertDefault();
    }
}

struct ValuePair
{
    UInt64 local = 0;
    UInt64 remote = 0;
};

void ErrorLog::stepFunction(TimePoint current_time)
{
    /// Static lazy initialization to avoid polluting the header with implementation details
    static std::vector<ValuePair> previous_values(ErrorCodes::end());

    auto event_time = std::chrono::system_clock::to_time_t(current_time);

    for (ErrorCodes::ErrorCode code = 0, end = ErrorCodes::end(); code < end; ++code)
    {
        const auto & error = ErrorCodes::values[code].get();
        if (error.local.count != previous_values.at(code).local)
        {
            ErrorLogElement local_elem {
                .event_time=event_time,
                .code=code,
                .error_time_ms=error.local.error_time_ms,
                .error_message=error.local.message,
                .value=error.local.count - previous_values.at(code).local,
                .remote=false,
                .query_id=error.local.query_id,
                .error_trace=error.local.trace,
                .symbolize=symbolize
            };
            this->add(std::move(local_elem));
            previous_values[code].local = error.local.count;
        }
        if (error.remote.count != previous_values.at(code).remote)
        {
            ErrorLogElement remote_elem {
                .event_time=event_time,
                .code=code,
                .error_time_ms=error.remote.error_time_ms,
                .error_message=error.remote.message,
                .value=error.remote.count - previous_values.at(code).remote,
                .remote=true,
                .query_id=error.remote.query_id,
                .error_trace=error.remote.trace,
                .symbolize=symbolize
            };
            add(std::move(remote_elem));
            previous_values[code].remote = error.remote.count;
        }
    }
}

}
