---
slug: /en/operations/system-tables/asynchronous_insert_log
---
# asynchronous_insert_log

Contains information about async inserts. Each entry represents an insert query buffered into an async insert query.

To start logging configure parameters in the [asynchronous_insert_log](../../operations/server-configuration-parameters/settings.md#asynchronous_insert_log) section.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [asynchronous_insert_log](../../operations/server-configuration-parameters/settings.md#asynchronous_insert_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](../../operations/system-tables/index.md#system-tables-introduction) for more details.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — The date when the async insert happened.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the async insert finished execution.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — The date and time when the async insert finished execution with microseconds precision.
- `query` ([String](../../sql-reference/data-types/string.md)) — Query string.
- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `format` ([String](/docs/en/sql-reference/data-types/string.md)) — Format name.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the initial query.
- `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of inserted bytes.
- `exception` ([String](../../sql-reference/data-types/string.md)) — Exception message.
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Status of the view. Values:
    - `'Ok' = 1` — Successful insert.
    - `'ParsingError' = 2` — Exception when parsing the data.
    - `'FlushError' = 3` — Exception when flushing the data.
- `flush_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The date and time when the flush happened.
- `flush_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — The date and time when the flush happened with microseconds precision.
- `flush_query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the flush query.

**Example**

Query:

``` sql
SELECT * FROM system.asynchronous_insert_log LIMIT 1 \G;
```

Result:

``` text
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

**See Also**

- [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.asynchronous_inserts](../../operations/system-tables/asynchronous_inserts.md#system_tables-asynchronous_inserts) — This table contains information about pending asynchronous inserts in queue.
