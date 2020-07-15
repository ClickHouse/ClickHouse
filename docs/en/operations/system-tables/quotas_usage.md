# system.quotas\_usage {#system_tables-quotas_usage}

Quota usage by all users.

Columns:
- `quota_name` ([String](../../sql-reference/data-types/string.md)) — Quota name.
- `quota_key` ([String](../../sql-reference/data-types/string.md)) — Key value.
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Quota usage for current user.
- `start_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — Start time for calculating resource consumption.
- `end_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — End time for calculating resource consumption.
- `duration` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt32](../../sql-reference/data-types/int-uint.md))) — Length of the time interval for calculating resource consumption, in seconds.
- `queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The total number of requests in this interval.
- `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum number of requests.
- `errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The number of queries that threw an exception.
- `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum number of errors.
- `result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The total number of rows given as a result.
- `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum of source rows read from tables.
- `result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — RAM volume in bytes used to store a queries result.
- `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum RAM volume used to store a queries result, in bytes.
- `read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md)))) — The total number of source rows read from tables for running the query on all remote servers.
- `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum number of rows read from all tables and table functions participated in queries.
- `read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The total number of bytes read from all tables and table functions participated in queries.
- `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum of bytes read from all tables and table functions.
- `execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — The total query execution time, in seconds (wall time).
- `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — Maximum of query execution time.
