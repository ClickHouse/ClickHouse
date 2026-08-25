#include <Interpreters/AsynchronousInsertLog.h>
#include <Common/SystemTableDocumentation.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>


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
        {"status", type_status, "Status of the insert. Values: 'Ok' = 0 — Successful insert, 'ParsingError' = 1 — Exception when parsing the data, 'FlushError' = 2 — Exception when flushing the data."},
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

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "asynchronous_insert_log",
    .description = R"DOCS_MD(
Contains information about async inserts. Each entry represents an insert query buffered into an async insert query.

To start logging configure parameters in the [asynchronous_insert_log](/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log) section.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [asynchronous_insert_log](/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.
)DOCS_MD",
    .get_columns = AsynchronousInsertLogElement::getColumnsDescription,
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT * FROM system.asynchronous_insert_log LIMIT 1 \G;
```

```text title="Response"
hostname:                clickhouse.eu-central1.internal
event_date:              2023-06-08
event_time:              2023-06-08 10:08:53
event_time_microseconds: 2023-06-08 10:08:53.199516
query:                   INSERT INTO public.data_guess (user_id, datasource_id, timestamp, path, type, num, str) FORMAT CSV
database:                public
table:                   data_guess
format:                  CSV
query_id:                b46cd4c4-0269-4d0b-99f5-d27668c6102e
bytes:                   133223
exception:
status:                  Ok
flush_time:              2023-06-08 10:08:55
flush_time_microseconds: 2023-06-08 10:08:55.139676
flush_query_id:          cd2c1e43-83f5-49dc-92e4-2fbc7f8d3716
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_inserts](/reference/system-tables/asynchronous_inserts) — This table contains information about pending asynchronous inserts in queue.
)DOCS_MD")

}
