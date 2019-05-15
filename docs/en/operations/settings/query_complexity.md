# Restrictions on query complexity

Restrictions on query complexity are part of the settings.
They are used in order to provide safer execution from the user interface.
Almost all the restrictions only apply to `SELECT`. For distributed query processing, restrictions are applied on each server separately.

ClickHouse checks the restrictions for data parts, not for each row. It means that you can exceed the value of restriction with a size of the data part.

Restrictions on the "maximum amount of something" can take the value 0, which means "unrestricted".
Most restrictions also have an 'overflow_mode' setting, meaning what to do when the limit is exceeded.
It can take one of two values: `throw` or `break`. Restrictions on aggregation (group_by_overflow_mode) also have the value `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don't add new keys to the set.

## max_memory_usage {#settings_max_memory_usage}

The maximum amount of RAM to use for running a query on a single server.

In the default configuration file, the maximum is 10 GB.

The setting doesn't consider the volume of available memory or the total volume of memory on the machine.
The restriction applies to a single query within a single server.
You can use `SHOW PROCESSLIST` to see the current memory consumption for each query.
In addition, the peak memory consumption is tracked for each query and written to the log.

Memory usage is not monitored for the states of certain aggregate functions.

Memory usage is not fully tracked for states of the aggregate functions `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` from `String` and `Array` arguments.

Memory consumption is also restricted by the parameters `max_memory_usage_for_user` and `max_memory_usage_for_all_queries`.

## max_memory_usage_for_user

The maximum amount of RAM to use for running a user's queries on a single server.

Default values are defined in [Settings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Interpreters/Settings.h#L244). By default, the amount is not restricted (`max_memory_usage_for_user = 0`).

See also the description of [max_memory_usage](#settings_max_memory_usage).

## max_memory_usage_for_all_queries

The maximum amount of RAM to use for running all queries on a single server.

Default values are defined in [Settings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Interpreters/Settings.h#L245). By default, the amount is not restricted (`max_memory_usage_for_all_queries = 0`).

See also the description of [max_memory_usage](#settings_max_memory_usage).

## max_rows_to_read

The following restrictions can be checked on each block (instead of on each row). That is, the restrictions can be broken a little.
When running a query in multiple threads, the following restrictions apply to each thread separately.

Maximum number of rows that can be read from a table when running a query.

## max_bytes_to_read

Maximum number of bytes (uncompressed data) that can be read from a table when running a query.

## read_overflow_mode

What to do when the volume of data read exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_rows_to_group_by

Maximum number of unique keys received from aggregation. This setting lets you limit memory consumption when aggregating.

## group_by_overflow_mode

What to do when the number of unique keys for aggregation exceeds the limit: 'throw', 'break', or 'any'. By default, throw.
Using the 'any' value lets you run an approximation of GROUP BY. The quality of this approximation depends on the statistical nature of the data.

## max_rows_to_sort

Maximum number of rows before sorting. This allows you to limit memory consumption when sorting.

## max_bytes_to_sort

Maximum number of bytes before sorting.

## sort_overflow_mode

What to do if the number of rows received before sorting exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_result_rows

Limit on the number of rows in the result. Also checked for subqueries, and on remote servers when running parts of a distributed query.

## max_result_bytes

Limit on the number of bytes in the result. The same as the previous setting.

## result_overflow_mode

What to do if the volume of the result exceeds one of the limits: 'throw' or 'break'. By default, throw.
Using 'break' is similar to using LIMIT.

## max_execution_time

Maximum query execution time in seconds.
At this time, it is not checked for one of the sorting stages, or when merging and finalizing aggregate functions.

## timeout_overflow_mode

What to do if the query is run longer than 'max_execution_time': 'throw' or 'break'. By default, throw.

## min_execution_speed

Minimal execution speed in rows per second. Checked on every data block when 'timeout_before_checking_execution_speed' expires. If the execution speed is lower, an exception is thrown.

## min_execution_speed_bytes

Minimum number of execution bytes per second. Checked on every data block when 'timeout_before_checking_execution_speed' expires. If the execution speed is lower, an exception is thrown.

## max_execution_speed

Maximum number of execution rows per second. Checked on every data block when 'timeout_before_checking_execution_speed' expires. If the execution speed is high, the execution speed will be reduced.

## max_execution_speed_bytes

Maximum number of execution bytes per second. Checked on every data block when 'timeout_before_checking_execution_speed' expires. If the execution speed is high, the execution speed will be reduced.

## timeout_before_checking_execution_speed

Checks that execution speed is not too slow (no less than 'min_execution_speed'), after the specified time in seconds has expired.

## max_columns_to_read

Maximum number of columns that can be read from a table in a single query. If a query requires reading a greater number of columns, it throws an exception.

## max_temporary_columns

Maximum number of temporary columns that must be kept in RAM at the same time when running a query, including constant columns. If there are more temporary columns than this, it throws an exception.

## max_temporary_non_const_columns

The same thing as 'max_temporary_columns', but without counting constant columns.
Note that constant columns are formed fairly often when running a query, but they require approximately zero computing resources.

## max_subquery_depth

Maximum nesting depth of subqueries. If subqueries are deeper, an exception is thrown. By default, 100.

## max_pipeline_depth

Maximum pipeline depth. Corresponds to the number of transformations that each data block goes through during query processing. Counted within the limits of a single server. If the pipeline depth is greater, an exception is thrown. By default, 1000.

## max_ast_depth

Maximum nesting depth of a query syntactic tree. If exceeded, an exception is thrown.
At this time, it isn't checked during parsing, but only after parsing the query. That is, a syntactic tree that is too deep can be created during parsing, but the query will fail. By default, 1000.

## max_ast_elements

Maximum number of elements in a query syntactic tree. If exceeded, an exception is thrown.
In the same way as the previous setting, it is checked only after parsing the query. By default, 50,000.

## max_rows_in_set

Maximum number of rows for a data set in the IN clause created from a subquery.

## max_bytes_in_set

Maximum number of bytes (uncompressed data) used by a set in the IN clause created from a subquery.

## set_overflow_mode

What to do when the amount of data exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_rows_in_distinct

Maximum number of different rows when using DISTINCT.

## max_bytes_in_distinct

Maximum number of bytes used by a hash table when using DISTINCT.

## distinct_overflow_mode

What to do when the amount of data exceeds one of the limits: 'throw' or 'break'. By default, throw.

## max_rows_to_transfer

Maximum number of rows that can be passed to a remote server or saved in a temporary table when using GLOBAL IN.

## max_bytes_to_transfer

Maximum number of bytes (uncompressed data) that can be passed to a remote server or saved in a temporary table when using GLOBAL IN.

## transfer_overflow_mode

What to do when the amount of data exceeds one of the limits: 'throw' or 'break'. By default, throw.

[Original article](https://clickhouse.yandex/docs/en/operations/settings/query_complexity/) <!--hide-->
