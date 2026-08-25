#include <base/getFQDNOrHostName.h>
#include <Common/SystemTableDocumentation.h>
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

    columns[column_idx++]->insert(Array(last_error_trace.begin(), last_error_trace.end()));
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

    auto to_addrs = [](const auto & trace)
    {
        std::vector<UInt64> addrs;
        addrs.reserve(trace.size());
        for (auto * ptr : trace)
            addrs.push_back(reinterpret_cast<uintptr_t>(ptr));
        return addrs;
    };

    for (ErrorCodes::ErrorCode code = 0, end = ErrorCodes::end(); code < end; ++code)
    {
        const auto & error = ErrorCodes::values[code].get();
        /// previous_values is guarded by the mutex held above; thread-safety analysis cannot see the lock
        /// through the add() callback, so suppress the false positive on the accesses made inside it.
        if (error.local.count != previous_values.at(code).local)
        {
            this->add([&](ErrorLogElement & element)
            {
                element = ErrorLogElement {
                    .event_time=event_time,
                    .code=code,
                    .value=error.local.count - TSA_SUPPRESS_WARNING_FOR_READ(previous_values).at(code).local,
                    .remote=false,
                    .last_error_time=(error.local.error_time_ms / 1000),
                    .last_error_message=error.local.message,
                    .last_error_query_id=error.local.query_id,
                    .last_error_trace=to_addrs(error.local.trace)
                };
            });
            previous_values[code].local = error.local.count;
        }
        if (error.remote.count != previous_values.at(code).remote)
        {
            add([&](ErrorLogElement & element)
            {
                element = ErrorLogElement {
                    .event_time=event_time,
                    .code=code,
                    .value=error.remote.count - TSA_SUPPRESS_WARNING_FOR_READ(previous_values).at(code).remote,
                    .remote=true,
                    .last_error_time=(error.remote.error_time_ms / 1000),
                    .last_error_message=error.remote.message,
                    .last_error_query_id=error.remote.query_id,
                    .last_error_trace=to_addrs(error.remote.trace)
                };
            });
            previous_values[code].remote = error.remote.count;
        }
    }
}

}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "error_log",
    .description = R"DOCS_MD(
Contains history of error values from table `system.errors`, periodically flushed to disk.
)DOCS_MD",
    .get_columns = ErrorLogElement::getColumnsDescription,
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.error_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:            clickhouse.testing.internal
event_date:          2025-11-11
event_time:          2025-11-11 11:35:28
code:                60
error:               UNKNOWN_TABLE
value:               1
remote:              0
last_error_time:     2025-11-11 11:35:28
last_error_message:  Unknown table expression identifier 'system.table_not_exist' in scope SELECT * FROM system.table_not_exist
last_error_query_id: 77ad9ece-3db7-4236-9b5a-f789bce4aa2e
last_error_trace:    [100506790044914,100506534488542,100506409937998,100506409936517,100506425182891,100506618154123,100506617994473,100506617990486,100506617988112,100506618341386,100506630272160,100506630266232,100506630276900,100506629795243,100506633519500,100506633495783,100506692143858,100506692248921,100506790779783,100506790781278,100506790390399,100506790380047,123814948752036,123814949330028]
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [error_log setting](/reference/settings/server-settings/settings/other#error_log) — Enabling and disabling the setting.
- [system.errors](/reference/system-tables/errors) — Contains error codes with the number of times they have been triggered.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD")

}
