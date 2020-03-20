# Restrictions on query complexity {#restrictions-on-query-complexity}

Restrictions on query complexity are part of the settings.
They are used in order to provide safer execution from the user interface.
Almost all the restrictions only apply to SELECTs.For distributed query processing, restrictions are applied on each server separately.

Restrictions on the «maximum amount of something» can take the value 0, which means «unrestricted».
Most restrictions also have an ‘overflow\_mode’ setting, meaning what to do when the limit is exceeded.
It can take one of two values: `throw` or `break`. Restrictions on aggregation (group\_by\_overflow\_mode) also have the value `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don’t add new keys to the set.

## readonly {#query-complexity-readonly}

With a value of 0, you can execute any queries.
With a value of 1, you can only execute read requests (such as SELECT and SHOW). Requests for writing and changing settings (INSERT, SET) are prohibited.
With a value of 2, you can process read queries (SELECT, SHOW) and change settings (SET).

After enabling readonly mode, you can’t disable it in the current session.

When using the GET method in the HTTP interface, ‘readonly = 1’ is set automatically. In other words, for queries that modify data, you can only use the POST method. You can send the query itself either in the POST body, or in the URL parameter.

## max\_memory\_usage {#settings-max-memory-usage}

The maximum amount of RAM to use for running a query on a single server.

In the default configuration file, the maximum is 10 GB.

The setting doesn’t consider the volume of available memory or the total volume of memory on the machine.
The restriction applies to a single query within a single server.
You can use `SHOW PROCESSLIST` to see the current memory consumption for each query.
In addition, the peak memory consumption is tracked for each query and written to the log.

Memory usage is not monitored for the states of certain aggregate functions.

Memory usage is not fully tracked for states of the aggregate functions `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` from `String` and `Array` arguments.

Memory consumption is also restricted by the parameters `max_memory_usage_for_user` and `max_memory_usage_for_all_queries`.

## max\_memory\_usage\_for\_user {#max-memory-usage-for-user}

The maximum amount of RAM to use for running a user’s queries on a single server.

Default values are defined in [Settings.h](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Interpreters/Settings.h#L244). By default, the amount is not restricted (`max_memory_usage_for_user = 0`).

See also the description of [max\_memory\_usage](#settings_max_memory_usage).

## max\_memory\_usage\_for\_all\_queries {#max-memory-usage-for-all-queries}

The maximum amount of RAM to use for running all queries on a single server.

Default values are defined in [Settings.h](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Interpreters/Settings.h#L245). By default, the amount is not restricted (`max_memory_usage_for_all_queries = 0`).

See also the description of [max\_memory\_usage](#settings_max_memory_usage).

## max\_rows\_to\_read {#max-rows-to-read}

The following restrictions can be checked on each block (instead of on each row). That is, the restrictions can be broken a little.
When running a query in multiple threads, the following restrictions apply to each thread separately.

Maximum number of rows that can be read from a table when running a query.

## max\_bytes\_to\_read {#max-bytes-to-read}

Maximum number of bytes (uncompressed data) that can be read from a table when running a query.

## read\_overflow\_mode {#read-overflow-mode}

What to do when the volume of data read exceeds one of the limits: ‘throw’ or ‘break’. By default, throw.

## max\_rows\_to\_group\_by {#max-rows-to-group-by}

Maximum number of unique keys received from aggregation. This setting lets you limit memory consumption when aggregating.

## group\_by\_overflow\_mode {#group-by-overflow-mode}

What to do when the number of unique keys for aggregation exceeds the limit: ‘throw’, ‘break’, or ‘any’. By default, throw.
Using the ‘any’ value lets you run an approximation of GROUP BY. The quality of this approximation depends on the statistical nature of the data.

## max\_rows\_to\_sort {#max-rows-to-sort}

Maximum number of rows before sorting. This allows you to limit memory consumption when sorting.

## max\_bytes\_to\_sort {#max-bytes-to-sort}

Maximum number of bytes before sorting.

## sort\_overflow\_mode {#sort-overflow-mode}

What to do if the number of rows received before sorting exceeds one of the limits: ‘throw’ or ‘break’. By default, throw.

## max\_result\_rows {#max-result-rows}

Limit on the number of rows in the result. Also checked for subqueries, and on remote servers when running parts of a distributed query.

## max\_result\_bytes {#max-result-bytes}

Limit on the number of bytes in the result. The same as the previous setting.

## result\_overflow\_mode {#result-overflow-mode}

What to do if the volume of the result exceeds one of the limits: ‘throw’ or ‘break’. By default, throw.
Using ‘break’ is similar to using LIMIT.

## max\_execution\_time {#max-execution-time}

Maximum query execution time in seconds.
At this time, it is not checked for one of the sorting stages, or when merging and finalizing aggregate functions.

## timeout\_overflow\_mode {#timeout-overflow-mode}

What to do if the query is run longer than ‘max\_execution\_time’: ‘throw’ or ‘break’. By default, throw.

## min\_execution\_speed {#min-execution-speed}

Minimal execution speed in rows per second. Checked on every data block when ‘timeout\_before\_checking\_execution\_speed’ expires. If the execution speed is lower, an exception is thrown.

## timeout\_before\_checking\_execution\_speed {#timeout-before-checking-execution-speed}

Checks that execution speed is not too slow (no less than ‘min\_execution\_speed’), after the specified time in seconds has expired.

## max\_columns\_to\_read {#max-columns-to-read}

Maximum number of columns that can be read from a table in a single query. If a query requires reading a greater number of columns, it throws an exception.

## max\_temporary\_columns {#max-temporary-columns}

Maximum number of temporary columns that must be kept in RAM at the same time when running a query, including constant columns. If there are more temporary columns than this, it throws an exception.

## max\_temporary\_non\_const\_columns {#max-temporary-non-const-columns}

The same thing as ‘max\_temporary\_columns’, but without counting constant columns.
Note that constant columns are formed fairly often when running a query, but they require approximately zero computing resources.

## max\_subquery\_depth {#max-subquery-depth}

Maximum nesting depth of subqueries. If subqueries are deeper, an exception is thrown. By default, 100.

## max\_pipeline\_depth {#max-pipeline-depth}

Maximum pipeline depth. Corresponds to the number of transformations that each data block goes through during query processing. Counted within the limits of a single server. If the pipeline depth is greater, an exception is thrown. By default, 1000.

## max\_ast\_depth {#max-ast-depth}

Maximum nesting depth of a query syntactic tree. If exceeded, an exception is thrown.
At this time, it isn’t checked during parsing, but only after parsing the query. That is, a syntactic tree that is too deep can be created during parsing, but the query will fail. By default, 1000.

## max\_ast\_elements {#max-ast-elements}

Maximum number of elements in a query syntactic tree. If exceeded, an exception is thrown.
In the same way as the previous setting, it is checked only after parsing the query. By default, 50,000.

## max\_rows\_in\_set {#max-rows-in-set}

Maximum number of rows for a data set in the IN clause created from a subquery.

## max\_bytes\_in\_set {#max-bytes-in-set}

Maximum number of bytes (uncompressed data) used by a set in the IN clause created from a subquery.

## set\_overflow\_mode {#set-overflow-mode}

What to do when the amount of data exceeds one of the limits: ‘throw’ or ‘break’. By default, throw.

## max\_rows\_in\_distinct {#max-rows-in-distinct}

Maximum number of different rows when using DISTINCT.

## max\_bytes\_in\_distinct {#max-bytes-in-distinct}

Maximum number of bytes used by a hash table when using DISTINCT.

## distinct\_overflow\_mode {#distinct-overflow-mode}

What to do when the amount of data exceeds one of the limits: ‘throw’ or ‘break’. By default, throw.

## max\_rows\_to\_transfer {#max-rows-to-transfer}

Maximum number of rows that can be passed to a remote server or saved in a temporary table when using GLOBAL IN.

## max\_bytes\_to\_transfer {#max-bytes-to-transfer}

Maximum number of bytes (uncompressed data) that can be passed to a remote server or saved in a temporary table when using GLOBAL IN.

## transfer\_overflow\_mode {#transfer-overflow-mode}

What to do when the amount of data exceeds one of the limits: ‘throw’ or ‘break’. By default, throw.

[Original article](https://clickhouse.tech/docs/en/operations/settings/query_complexity/) <!--hide-->
