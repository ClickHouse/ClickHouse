#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/ErrorCodes.h>
#include <DataTypes/DataTypeArray.h>
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
    columns[column_idx++]->insert(value);
    columns[column_idx++]->insert(remote);
    columns[column_idx++]->insert(last_error_time);
    columns[column_idx++]->insert(last_error_message);
    columns[column_idx++]->insert(last_error_query_id);

    std::vector<uintptr_t> last_error_trace_array;
    last_error_trace_array.reserve(last_error_trace.size());

    for (auto * ptr : last_error_trace)
        last_error_trace_array.emplace_back(reinterpret_cast<uintptr_t>(ptr));

    columns[column_idx++]->insert(Array(last_error_trace_array.begin(), last_error_trace_array.end()));
}

struct ValuePair
{
    UInt64 local = 0;
    UInt64 remote = 0;
};

void ErrorLog::stepFunction(TimePoint current_time)
{
    std::lock_guard lock(previous_values_mutex);

    auto event_time = std::chrono::system_clock::to_time_t(current_time);

    for (ErrorCodes::ErrorCode code = 0, end = ErrorCodes::end(); code < end; ++code)
    {
        const auto & error = ErrorCodes::values[code].get();
        if (error.local.count != previous_values.at(code).local)
        {
            ErrorLogElement local_elem {
                .event_time=event_time,
                .code=code,
                .value=error.local.count - previous_values.at(code).local,
                .remote=false,
                .last_error_time=(error.local.error_time_ms / 1000),
                .last_error_message=error.local.message,
                .last_error_query_id=error.local.query_id,
                .last_error_trace=error.local.trace
            };
            this->add(std::move(local_elem));
            previous_values[code].local = error.local.count;
        }
        if (error.remote.count != previous_values.at(code).remote)
        {
            ErrorLogElement remote_elem {
                .event_time=event_time,
                .code=code,
                .value=error.remote.count - previous_values.at(code).remote,
                .remote=true,
                .last_error_time=(error.remote.error_time_ms / 1000),
                .last_error_message=error.remote.message,
                .last_error_query_id=error.remote.query_id,
                .last_error_trace=error.remote.trace
            };
            add(std::move(remote_elem));
            previous_values[code].remote = error.remote.count;
        }
    }
}

}
