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

#include <vector>

namespace DB
{

ColumnsDescription ErrorLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
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
                "last_error_trace",
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "A stack trace that represents a list of physical addresses where the called methods are stored."
            },
        {
                "query_id",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Id of a query that caused an error (if available)."
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
    columns[column_idx++]->insert(error_message);
    columns[column_idx++]->insert(value);
    columns[column_idx++]->insert(remote);
    columns[column_idx++]->insert(error_trace);
    columns[column_idx++]->insert(query_id);
}

struct ValuePair
{
    UInt64 local = 0;
    UInt64 remote = 0;
};


Array ErrorLog::buildTraceArray(const ErrorCodes::FramePointers & trace)
{
    Array result(trace.size());
    for (size_t i = 0; i < trace.size(); ++i)
        result[i] = reinterpret_cast<intptr_t>(trace[i]);
    return result;
}

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
                .error_message=error.local.message,
                .value=error.local.count - previous_values.at(code).local,
                .remote=false,
                .error_trace=buildTraceArray(error.local.trace),
                .query_id=error.local.query_id
            };
            this->add(std::move(local_elem));
            previous_values[code].local = error.local.count;
        }
        if (error.remote.count != previous_values.at(code).remote)
        {
            ErrorLogElement remote_elem {
                .event_time=event_time,
                .code=code,
                .error_message=error.remote.message,
                .value=error.remote.count - previous_values.at(code).remote,
                .remote=true,
                .error_trace=buildTraceArray(error.remote.trace),
                .query_id=error.remote.query_id
            };
            add(std::move(remote_elem));
            previous_values[code].remote = error.remote.count;
        }
    }
}

}
