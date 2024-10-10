---
sidebar_label: Core Settings
sidebar_position: 2
slug: /en/operations/settings/settings
toc_max_heading_level: 2
---

# Core Settings

All below settings are also available in table [system.settings](/docs/en/operations/system-tables/settings).

## add_http_cors_header {#add_http_cors_header}

Type: Bool

Default value: 0

Write add http CORS header.

## additional_result_filter {#additional_result_filter}

Type: String

Default value: 

An additional filter expression to apply to the result of `SELECT` query.
This setting is not applied to any subquery.

**Example**

``` sql
INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');
SElECT * FROM table_1;
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 2 │ bb   │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
```sql
SELECT *
FROM table_1
SETTINGS additional_result_filter = 'x != 2'
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```

## additional_table_filters {#additional_table_filters}

Type: Map

Default value: {}

An additional filter expression that is applied after reading
from the specified table.

**Example**

``` sql
INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');
SELECT * FROM table_1;
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 2 │ bb   │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
```sql
SELECT *
FROM table_1
SETTINGS additional_table_filters = {'table_1': 'x != 2'}
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```

## aggregate_functions_null_for_empty {#aggregate_functions_null_for_empty}

Type: Bool

Default value: 0

Enables or disables rewriting all aggregate functions in a query, adding [-OrNull](../../sql-reference/aggregate-functions/combinators.md/#agg-functions-combinator-ornull) suffix to them. Enable it for SQL standard compatibility.
It is implemented via query rewrite (similar to [count_distinct_implementation](#count_distinct_implementation) setting) to get consistent results for distributed queries.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

Consider the following query with aggregate functions:
```sql
SELECT SUM(-1), MAX(0) FROM system.one WHERE 0;
```

With `aggregate_functions_null_for_empty = 0` it would produce:
```text
┌─SUM(-1)─┬─MAX(0)─┐
│       0 │      0 │
└─────────┴────────┘
```

With `aggregate_functions_null_for_empty = 1` the result would be:
```text
┌─SUMOrNull(-1)─┬─MAXOrNull(0)─┐
│          NULL │         NULL │
└───────────────┴──────────────┘
```

## aggregation_in_order_max_block_bytes {#aggregation_in_order_max_block_bytes}

Type: UInt64

Default value: 50000000

Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.

## aggregation_memory_efficient_merge_threads {#aggregation_memory_efficient_merge_threads}

Type: UInt64

Default value: 0

Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as 'max_threads'.

## allow_aggregate_partitions_independently {#allow_aggregate_partitions_independently}

Type: Bool

Default value: 0

Enable independent aggregation of partitions on separate threads when partition key suits group by key. Beneficial when number of partitions close to number of cores and partitions have roughly the same size

## allow_archive_path_syntax {#allow_archive_path_syntax}

Type: Bool

Default value: 1

File/S3 engines/table function will parse paths with '::' as '\\<archive\\> :: \\<file\\>' if archive has correct extension

## allow_asynchronous_read_from_io_pool_for_merge_tree {#allow_asynchronous_read_from_io_pool_for_merge_tree}

Type: Bool

Default value: 0

Use background I/O pool to read from MergeTree tables. This setting may increase performance for I/O bound queries

## allow_changing_replica_until_first_data_packet {#allow_changing_replica_until_first_data_packet}

Type: Bool

Default value: 0

If it's enabled, in hedged requests we can start new connection until receiving first data packet even if we have already made some progress
(but progress haven't updated for `receive_data_timeout` timeout), otherwise we disable changing replica after the first time we made progress.

## allow_create_index_without_type {#allow_create_index_without_type}

Type: Bool

Default value: 0

Allow CREATE INDEX query without TYPE. Query will be ignored. Made for SQL compatibility tests.

## allow_custom_error_code_in_throwif {#allow_custom_error_code_in_throwif}

Type: Bool

Default value: 0

Enable custom error code in function throwIf(). If true, thrown exceptions may have unexpected error codes.

## allow_ddl {#allow_ddl}

Type: Bool

Default value: 1

If it is set to true, then a user is allowed to executed DDL queries.

## allow_deprecated_database_ordinary {#allow_deprecated_database_ordinary}

Type: Bool

Default value: 0

Allow to create databases with deprecated Ordinary engine

## allow_deprecated_error_prone_window_functions {#allow_deprecated_error_prone_window_functions}

Type: Bool

Default value: 0

Allow usage of deprecated error prone window functions (neighbor, runningAccumulate, runningDifferenceStartingWithFirstValue, runningDifference)

## allow_deprecated_snowflake_conversion_functions {#allow_deprecated_snowflake_conversion_functions}

Type: Bool

Default value: 0

Functions `snowflakeToDateTime`, `snowflakeToDateTime64`, `dateTimeToSnowflake`, and `dateTime64ToSnowflake` are deprecated and disabled by default.
Please use functions `snowflakeIDToDateTime`, `snowflakeIDToDateTime64`, `dateTimeToSnowflakeID`, and `dateTime64ToSnowflakeID` instead.

To re-enable the deprecated functions (e.g., during a transition period), please set this setting to `true`.

## allow_deprecated_syntax_for_merge_tree {#allow_deprecated_syntax_for_merge_tree}

Type: Bool

Default value: 0

Allow to create *MergeTree tables with deprecated engine definition syntax

## allow_distributed_ddl {#allow_distributed_ddl}

Type: Bool

Default value: 1

If it is set to true, then a user is allowed to executed distributed DDL queries.

## allow_drop_detached {#allow_drop_detached}

Type: Bool

Default value: 0

Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries

## allow_execute_multiif_columnar {#allow_execute_multiif_columnar}

Type: Bool

Default value: 1

Allow execute multiIf function columnar

## allow_experimental_analyzer {#allow_experimental_analyzer}

Type: Bool

Default value: 1

Allow new query analyzer.

## allow_experimental_codecs {#allow_experimental_codecs}

Type: Bool

Default value: 0

If it is set to true, allow to specify experimental compression codecs (but we don't have those yet and this option does nothing).

## allow_experimental_database_materialized_mysql {#allow_experimental_database_materialized_mysql}

Type: Bool

Default value: 0

Allow to create database with Engine=MaterializedMySQL(...).

## allow_experimental_database_materialized_postgresql {#allow_experimental_database_materialized_postgresql}

Type: Bool

Default value: 0

Allow to create database with Engine=MaterializedPostgreSQL(...).

## allow_experimental_dynamic_type {#allow_experimental_dynamic_type}

Type: Bool

Default value: 0

Allow Dynamic data type

## allow_experimental_full_text_index {#allow_experimental_full_text_index}

Type: Bool

Default value: 0

If it is set to true, allow to use experimental full-text index.

## allow_experimental_funnel_functions {#allow_experimental_funnel_functions}

Type: Bool

Default value: 0

Enable experimental functions for funnel analysis.

## allow_experimental_hash_functions {#allow_experimental_hash_functions}

Type: Bool

Default value: 0

Enable experimental hash functions

## allow_experimental_inverted_index {#allow_experimental_inverted_index}

Type: Bool

Default value: 0

If it is set to true, allow to use experimental inverted index.

## allow_experimental_join_condition {#allow_experimental_join_condition}

Type: Bool

Default value: 0

Support join with inequal conditions which involve columns from both left and right table. e.g. t1.y < t2.y.

## allow_experimental_join_right_table_sorting {#allow_experimental_join_right_table_sorting}

Type: Bool

Default value: 0

If it is set to true, and the conditions of `join_to_sort_minimum_perkey_rows` and `join_to_sort_maximum_table_rows` are met, rerange the right table by key to improve the performance in left or inner hash join.

## allow_experimental_json_type {#allow_experimental_json_type}

Type: Bool

Default value: 0

Allow JSON data type

## allow_experimental_kafka_offsets_storage_in_keeper {#allow_experimental_kafka_offsets_storage_in_keeper}

Type: Bool

Default value: 0

Allow experimental feature to store Kafka related offsets in ClickHouse Keeper. When enabled a ClickHouse Keeper path and replica name can be specified to the Kafka table engine. As a result instead of the regular Kafka engine, a new type of storage engine will be used that stores the committed offsets primarily in ClickHouse Keeper

## allow_experimental_live_view {#allow_experimental_live_view}

Type: Bool

Default value: 0

Allows creation of a deprecated LIVE VIEW.

Possible values:

- 0 — Working with live views is disabled.
- 1 — Working with live views is enabled.

## allow_experimental_materialized_postgresql_table {#allow_experimental_materialized_postgresql_table}

Type: Bool

Default value: 0

Allows to use the MaterializedPostgreSQL table engine. Disabled by default, because this feature is experimental

## allow_experimental_nlp_functions {#allow_experimental_nlp_functions}

Type: Bool

Default value: 0

Enable experimental functions for natural language processing.

## allow_experimental_object_type {#allow_experimental_object_type}

Type: Bool

Default value: 0

Allow Object and JSON data types

## allow_experimental_parallel_reading_from_replicas {#allow_experimental_parallel_reading_from_replicas}

Type: UInt64

Default value: 0

Use up to `max_parallel_replicas` the number of replicas from each shard for SELECT query execution. Reading is parallelized and coordinated dynamically. 0 - disabled, 1 - enabled, silently disable them in case of failure, 2 - enabled, throw an exception in case of failure

## allow_experimental_query_deduplication {#allow_experimental_query_deduplication}

Type: Bool

Default value: 0

Experimental data deduplication for SELECT queries based on part UUIDs

## allow_experimental_refreshable_materialized_view {#allow_experimental_refreshable_materialized_view}

Type: Bool

Default value: 0

Allow refreshable materialized views (CREATE MATERIALIZED VIEW \\<name\\> REFRESH ...).

## allow_experimental_shared_set_join {#allow_experimental_shared_set_join}

Type: Bool

Default value: 1

Only in ClickHouse Cloud. Allow to create ShareSet and SharedJoin

## allow_experimental_statistics {#allow_experimental_statistics}

Type: Bool

Default value: 0

Allows defining columns with [statistics](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) and [manipulate statistics](../../engines/table-engines/mergetree-family/mergetree.md#column-statistics).

## allow_experimental_time_series_table {#allow_experimental_time_series_table}

Type: Bool

Default value: 0

Allows creation of tables with the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine.

Possible values:

- 0 — the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine is disabled.
- 1 — the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine is enabled.

## allow_experimental_variant_type {#allow_experimental_variant_type}

Type: Bool

Default value: 0

Allows creation of experimental [Variant](../../sql-reference/data-types/variant.md).

## allow_experimental_vector_similarity_index {#allow_experimental_vector_similarity_index}

Type: Bool

Default value: 0

Allow experimental vector similarity index

## allow_experimental_window_view {#allow_experimental_window_view}

Type: Bool

Default value: 0

Enable WINDOW VIEW. Not mature enough.

## allow_get_client_http_header {#allow_get_client_http_header}

Type: Bool

Default value: 0

Allow to use the function `getClientHTTPHeader` which lets to obtain a value of an the current HTTP request's header. It is not enabled by default for security reasons, because some headers, such as `Cookie`, could contain sensitive info. Note that the `X-ClickHouse-*` and `Authentication` headers are always restricted and cannot be obtained with this function.

## allow_hyperscan {#allow_hyperscan}

Type: Bool

Default value: 1

Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.

## allow_introspection_functions {#allow_introspection_functions}

Type: Bool

Default value: 0

Enables or disables [introspection functions](../../sql-reference/functions/introspection.md) for query profiling.

Possible values:

- 1 — Introspection functions enabled.
- 0 — Introspection functions disabled.

**See Also**

- [Sampling Query Profiler](../../operations/optimizing-performance/sampling-query-profiler.md)
- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)

## allow_materialized_view_with_bad_select {#allow_materialized_view_with_bad_select}

Type: Bool

Default value: 1

Allow CREATE MATERIALIZED VIEW with SELECT query that references nonexistent tables or columns. It must still be syntactically valid. Doesn't apply to refreshable MVs. Doesn't apply if the MV schema needs to be inferred from the SELECT query (i.e. if the CREATE has no column list and no TO table). Can be used for creating MV before its source table.

## allow_named_collection_override_by_default {#allow_named_collection_override_by_default}

Type: Bool

Default value: 1

Allow named collections' fields override by default.

## allow_non_metadata_alters {#allow_non_metadata_alters}

Type: Bool

Default value: 1

Allow to execute alters which affects not only tables metadata, but also data on disk

## allow_nonconst_timezone_arguments {#allow_nonconst_timezone_arguments}

Type: Bool

Default value: 0

Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*()

## allow_nondeterministic_mutations {#allow_nondeterministic_mutations}

Type: Bool

Default value: 0

User-level setting that allows mutations on replicated tables to make use of non-deterministic functions such as `dictGet`.

Given that, for example, dictionaries, can be out of sync across nodes, mutations that pull values from them are disallowed on replicated tables by default. Enabling this setting allows this behavior, making it the user's responsibility to ensure that the data used is in sync across all nodes.

**Example**

``` xml
<profiles>
    <default>
        <allow_nondeterministic_mutations>1</allow_nondeterministic_mutations>

        <!-- ... -->
    </default>

    <!-- ... -->

</profiles>
```

## allow_nondeterministic_optimize_skip_unused_shards {#allow_nondeterministic_optimize_skip_unused_shards}

Type: Bool

Default value: 0

Allow nondeterministic (like `rand` or `dictGet`, since later has some caveats with updates) functions in sharding key.

Possible values:

- 0 — Disallowed.
- 1 — Allowed.

## allow_prefetched_read_pool_for_local_filesystem {#allow_prefetched_read_pool_for_local_filesystem}

Type: Bool

Default value: 0

Prefer prefetched threadpool if all parts are on local filesystem

## allow_prefetched_read_pool_for_remote_filesystem {#allow_prefetched_read_pool_for_remote_filesystem}

Type: Bool

Default value: 1

Prefer prefetched threadpool if all parts are on remote filesystem

## allow_push_predicate_when_subquery_contains_with {#allow_push_predicate_when_subquery_contains_with}

Type: Bool

Default value: 1

Allows push predicate when subquery contains WITH clause

## allow_settings_after_format_in_insert {#allow_settings_after_format_in_insert}

Type: Bool

Default value: 0

Control whether `SETTINGS` after `FORMAT` in `INSERT` queries is allowed or not. It is not recommended to use this, since this may interpret part of `SETTINGS` as values.

Example:

```sql
INSERT INTO FUNCTION null('foo String') SETTINGS max_threads=1 VALUES ('bar');
```

But the following query will work only with `allow_settings_after_format_in_insert`:

```sql
SET allow_settings_after_format_in_insert=1;
INSERT INTO FUNCTION null('foo String') VALUES ('bar') SETTINGS max_threads=1;
```

Possible values:

- 0 — Disallow.
- 1 — Allow.

:::note
Use this setting only for backward compatibility if your use cases depend on old syntax.
:::

## allow_simdjson {#allow_simdjson}

Type: Bool

Default value: 1

Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.

## allow_statistics_optimize {#allow_statistics_optimize}

Type: Bool

Default value: 0

Allows using statistics to optimize queries

## allow_suspicious_codecs {#allow_suspicious_codecs}

Type: Bool

Default value: 0

If it is set to true, allow to specify meaningless compression codecs.

## allow_suspicious_fixed_string_types {#allow_suspicious_fixed_string_types}

Type: Bool

Default value: 0

In CREATE TABLE statement allows creating columns of type FixedString(n) with n > 256. FixedString with length >= 256 is suspicious and most likely indicates a misuse

## allow_suspicious_indices {#allow_suspicious_indices}

Type: Bool

Default value: 0

Reject primary/secondary indexes and sorting keys with identical expressions

## allow_suspicious_low_cardinality_types {#allow_suspicious_low_cardinality_types}

Type: Bool

Default value: 0

Allows or restricts using [LowCardinality](../../sql-reference/data-types/lowcardinality.md) with data types with fixed size of 8 bytes or less: numeric data types and `FixedString(8_bytes_or_less)`.

For small fixed values using of `LowCardinality` is usually inefficient, because ClickHouse stores a numeric index for each row. As a result:

- Disk space usage can rise.
- RAM consumption can be higher, depending on a dictionary size.
- Some functions can work slower due to extra coding/encoding operations.

Merge times in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables can grow due to all the reasons described above.

Possible values:

- 1 — Usage of `LowCardinality` is not restricted.
- 0 — Usage of `LowCardinality` is restricted.

## allow_suspicious_primary_key {#allow_suspicious_primary_key}

Type: Bool

Default value: 0

Allow suspicious `PRIMARY KEY`/`ORDER BY` for MergeTree (i.e. SimpleAggregateFunction).

## allow_suspicious_ttl_expressions {#allow_suspicious_ttl_expressions}

Type: Bool

Default value: 0

Reject TTL expressions that don't depend on any of table's columns. It indicates a user error most of the time.

## allow_suspicious_variant_types {#allow_suspicious_variant_types}

Type: Bool

Default value: 0

In CREATE TABLE statement allows specifying Variant type with similar variant types (for example, with different numeric or date types). Enabling this setting may introduce some ambiguity when working with values with similar types.

## allow_suspicious_types_in_group_by {#allow_suspicious_types_in_group_by}

Type: Bool

Default value: 0

Allows or restricts using [Variant](../../sql-reference/data-types/variant.md) and [Dynamic](../../sql-reference/data-types/dynamic.md) types in GROUP BY keys.

## allow_suspicious_types_in_order_by {#allow_suspicious_types_in_order_by}

Type: Bool

Default value: 0

Allows or restricts using [Variant](../../sql-reference/data-types/variant.md) and [Dynamic](../../sql-reference/data-types/dynamic.md) types in ORDER BY keys.

## allow_unrestricted_reads_from_keeper {#allow_unrestricted_reads_from_keeper}

Type: Bool

Default value: 0

Allow unrestricted (without condition on path) reads from system.zookeeper table, can be handy, but is not safe for zookeeper

## alter_move_to_space_execute_async {#alter_move_to_space_execute_async}

Type: Bool

Default value: 0

Execute ALTER TABLE MOVE ... TO [DISK|VOLUME] asynchronously

## alter_partition_verbose_result {#alter_partition_verbose_result}

Type: Bool

Default value: 0

Enables or disables the display of information about the parts to which the manipulation operations with partitions and parts have been successfully applied.
Applicable to [ATTACH PARTITION|PART](../../sql-reference/statements/alter/partition.md/#alter_attach-partition) and to [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md/#alter_freeze-partition).

Possible values:

- 0 — disable verbosity.
- 1 — enable verbosity.

**Example**

```sql
CREATE TABLE test(a Int64, d Date, s String) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY a;
INSERT INTO test VALUES(1, '2021-01-01', '');
INSERT INTO test VALUES(1, '2021-01-01', '');
ALTER TABLE test DETACH PARTITION ID '202101';

ALTER TABLE test ATTACH PARTITION ID '202101' SETTINGS alter_partition_verbose_result = 1;

┌─command_type─────┬─partition_id─┬─part_name────┬─old_part_name─┐
│ ATTACH PARTITION │ 202101       │ 202101_7_7_0 │ 202101_5_5_0  │
│ ATTACH PARTITION │ 202101       │ 202101_8_8_0 │ 202101_6_6_0  │
└──────────────────┴──────────────┴──────────────┴───────────────┘

ALTER TABLE test FREEZE SETTINGS alter_partition_verbose_result = 1;

┌─command_type─┬─partition_id─┬─part_name────┬─backup_name─┬─backup_path───────────────────┬─part_backup_path────────────────────────────────────────────┐
│ FREEZE ALL   │ 202101       │ 202101_7_7_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_7_7_0 │
│ FREEZE ALL   │ 202101       │ 202101_8_8_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_8_8_0 │
└──────────────┴──────────────┴──────────────┴─────────────┴───────────────────────────────┴─────────────────────────────────────────────────────────────┘
```

## alter_sync {#alter_sync}

Type: UInt64

Default value: 1

Allows to set up waiting for actions to be executed on replicas by [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries.

Possible values:

- 0 — Do not wait.
- 1 — Wait for own execution.
- 2 — Wait for everyone.

Cloud default value: `0`.

:::note
`alter_sync` is applicable to `Replicated` tables only, it does nothing to alters of not `Replicated` tables.
:::

## analyze_index_with_space_filling_curves {#analyze_index_with_space_filling_curves}

Type: Bool

Default value: 1

If a table has a space-filling curve in its index, e.g. `ORDER BY mortonEncode(x, y)` or `ORDER BY hilbertEncode(x, y)`, and the query has conditions on its arguments, e.g. `x >= 10 AND x <= 20 AND y >= 20 AND y <= 30`, use the space-filling curve for index analysis.

## analyzer_compatibility_join_using_top_level_identifier {#analyzer_compatibility_join_using_top_level_identifier}

Type: Bool

Default value: 0

Force to resolve identifier in JOIN USING from projection (for example, in `SELECT a + 1 AS b FROM t1 JOIN t2 USING (b)` join will be performed by `t1.a + 1 = t2.b`, rather then `t1.b = t2.b`).

## any_join_distinct_right_table_keys {#any_join_distinct_right_table_keys}

Type: Bool

Default value: 0

Enables legacy ClickHouse server behaviour in `ANY INNER|LEFT JOIN` operations.

:::note
Use this setting only for backward compatibility if your use cases depend on legacy `JOIN` behaviour.
:::

When the legacy behaviour is enabled:

- Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are not equal because ClickHouse uses the logic with many-to-one left-to-right table keys mapping.
- Results of `ANY INNER JOIN` operations contain all rows from the left table like the `SEMI LEFT JOIN` operations do.

When the legacy behaviour is disabled:

- Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are equal because ClickHouse uses the logic which provides one-to-many keys mapping in `ANY RIGHT JOIN` operations.
- Results of `ANY INNER JOIN` operations contain one row per key from both the left and right tables.

Possible values:

- 0 — Legacy behaviour is disabled.
- 1 — Legacy behaviour is enabled.

See also:

- [JOIN strictness](../../sql-reference/statements/select/join.md/#join-settings)

## apply_deleted_mask {#apply_deleted_mask}

Type: Bool

Default value: 1

Enables filtering out rows deleted with lightweight DELETE. If disabled, a query will be able to read those rows. This is useful for debugging and \"undelete\" scenarios

## apply_mutations_on_fly {#apply_mutations_on_fly}

Type: Bool

Default value: 0

If true, mutations (UPDATEs and DELETEs) which are not materialized in data part will be applied on SELECTs. Only available in ClickHouse Cloud.

## asterisk_include_alias_columns {#asterisk_include_alias_columns}

Type: Bool

Default value: 0

Include [ALIAS](../../sql-reference/statements/create/table.md#alias) columns for wildcard query (`SELECT *`).

Possible values:

- 0 - disabled
- 1 - enabled

## asterisk_include_materialized_columns {#asterisk_include_materialized_columns}

Type: Bool

Default value: 0

Include [MATERIALIZED](../../sql-reference/statements/create/table.md#materialized) columns for wildcard query (`SELECT *`).

Possible values:

- 0 - disabled
- 1 - enabled

## async_insert {#async_insert}

Type: Bool

Default value: 0

If true, data from INSERT query is stored in queue and later flushed to table in background. If wait_for_async_insert is false, INSERT query is processed almost instantly, otherwise client will wait until data will be flushed to table

## async_insert_busy_timeout_decrease_rate {#async_insert_busy_timeout_decrease_rate}

Type: Double

Default value: 0.2

The exponential growth rate at which the adaptive asynchronous insert timeout decreases

## async_insert_busy_timeout_increase_rate {#async_insert_busy_timeout_increase_rate}

Type: Double

Default value: 0.2

The exponential growth rate at which the adaptive asynchronous insert timeout increases

## async_insert_busy_timeout_max_ms {#async_insert_busy_timeout_max_ms}

Type: Milliseconds

Default value: 200

Maximum time to wait before dumping collected data per query since the first data appeared.

## async_insert_busy_timeout_min_ms {#async_insert_busy_timeout_min_ms}

Type: Milliseconds

Default value: 50

If auto-adjusting is enabled through async_insert_use_adaptive_busy_timeout, minimum time to wait before dumping collected data per query since the first data appeared. It also serves as the initial value for the adaptive algorithm

## async_insert_deduplicate {#async_insert_deduplicate}

Type: Bool

Default value: 0

For async INSERT queries in the replicated table, specifies that deduplication of inserting blocks should be performed

## async_insert_max_data_size {#async_insert_max_data_size}

Type: UInt64

Default value: 10485760

Maximum size in bytes of unparsed data collected per query before being inserted

## async_insert_max_query_number {#async_insert_max_query_number}

Type: UInt64

Default value: 450

Maximum number of insert queries before being inserted

## async_insert_poll_timeout_ms {#async_insert_poll_timeout_ms}

Type: Milliseconds

Default value: 10

Timeout for polling data from asynchronous insert queue

## async_insert_use_adaptive_busy_timeout {#async_insert_use_adaptive_busy_timeout}

Type: Bool

Default value: 1

If it is set to true, use adaptive busy timeout for asynchronous inserts

## async_query_sending_for_remote {#async_query_sending_for_remote}

Type: Bool

Default value: 1

Enables asynchronous connection creation and query sending while executing remote query.

Enabled by default.

## async_socket_for_remote {#async_socket_for_remote}

Type: Bool

Default value: 1

Enables asynchronous read from socket while executing remote query.

Enabled by default.

## azure_allow_parallel_part_upload {#azure_allow_parallel_part_upload}

Type: Bool

Default value: 1

Use multiple threads for azure multipart upload.

## azure_create_new_file_on_insert {#azure_create_new_file_on_insert}

Type: Bool

Default value: 0

Enables or disables creating a new file on each insert in azure engine tables

## azure_ignore_file_doesnt_exist {#azure_ignore_file_doesnt_exist}

Type: Bool

Default value: 0

Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.

## azure_list_object_keys_size {#azure_list_object_keys_size}

Type: UInt64

Default value: 1000

Maximum number of files that could be returned in batch by ListObject request

## azure_max_blocks_in_multipart_upload {#azure_max_blocks_in_multipart_upload}

Type: UInt64

Default value: 50000

Maximum number of blocks in multipart upload for Azure.

## azure_max_inflight_parts_for_one_file {#azure_max_inflight_parts_for_one_file}

Type: UInt64

Default value: 20

The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited.

## azure_max_single_part_copy_size {#azure_max_single_part_copy_size}

Type: UInt64

Default value: 268435456

The maximum size of object to copy using single part copy to Azure blob storage.

## azure_max_single_part_upload_size {#azure_max_single_part_upload_size}

Type: UInt64

Default value: 104857600

The maximum size of object to upload using singlepart upload to Azure blob storage.

## azure_max_single_read_retries {#azure_max_single_read_retries}

Type: UInt64

Default value: 4

The maximum number of retries during single Azure blob storage read.

## azure_max_unexpected_write_error_retries {#azure_max_unexpected_write_error_retries}

Type: UInt64

Default value: 4

The maximum number of retries in case of unexpected errors during Azure blob storage write

## azure_max_upload_part_size {#azure_max_upload_part_size}

Type: UInt64

Default value: 5368709120

The maximum size of part to upload during multipart upload to Azure blob storage.

## azure_min_upload_part_size {#azure_min_upload_part_size}

Type: UInt64

Default value: 16777216

The minimum size of part to upload during multipart upload to Azure blob storage.

## azure_sdk_max_retries {#azure_sdk_max_retries}

Type: UInt64

Default value: 10

Maximum number of retries in azure sdk

## azure_sdk_retry_initial_backoff_ms {#azure_sdk_retry_initial_backoff_ms}

Type: UInt64

Default value: 10

Minimal backoff between retries in azure sdk

## azure_sdk_retry_max_backoff_ms {#azure_sdk_retry_max_backoff_ms}

Type: UInt64

Default value: 1000

Maximal backoff between retries in azure sdk

## azure_skip_empty_files {#azure_skip_empty_files}

Type: Bool

Default value: 0

Enables or disables skipping empty files in S3 engine.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

## azure_strict_upload_part_size {#azure_strict_upload_part_size}

Type: UInt64

Default value: 0

The exact size of part to upload during multipart upload to Azure blob storage.

## azure_throw_on_zero_files_match {#azure_throw_on_zero_files_match}

Type: Bool

Default value: 0

Throw an error if matched zero files according to glob expansion rules.

Possible values:
- 1 — `SELECT` throws an exception.
- 0 — `SELECT` returns empty result.

## azure_truncate_on_insert {#azure_truncate_on_insert}

Type: Bool

Default value: 0

Enables or disables truncate before insert in azure engine tables.

## azure_upload_part_size_multiply_factor {#azure_upload_part_size_multiply_factor}

Type: UInt64

Default value: 2

Multiply azure_min_upload_part_size by this factor each time azure_multiply_parts_count_threshold parts were uploaded from a single write to Azure blob storage.

## azure_upload_part_size_multiply_parts_count_threshold {#azure_upload_part_size_multiply_parts_count_threshold}

Type: UInt64

Default value: 500

Each time this number of parts was uploaded to Azure blob storage, azure_min_upload_part_size is multiplied by azure_upload_part_size_multiply_factor.

## backup_restore_batch_size_for_keeper_multi {#backup_restore_batch_size_for_keeper_multi}

Type: UInt64

Default value: 1000

Maximum size of batch for multi request to [Zoo]Keeper during backup or restore

## backup_restore_batch_size_for_keeper_multiread {#backup_restore_batch_size_for_keeper_multiread}

Type: UInt64

Default value: 10000

Maximum size of batch for multiread request to [Zoo]Keeper during backup or restore

## backup_restore_keeper_fault_injection_probability {#backup_restore_keeper_fault_injection_probability}

Type: Float

Default value: 0

Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f]

## backup_restore_keeper_fault_injection_seed {#backup_restore_keeper_fault_injection_seed}

Type: UInt64

Default value: 0

0 - random seed, otherwise the setting value

## backup_restore_keeper_max_retries {#backup_restore_keeper_max_retries}

Type: UInt64

Default value: 20

Max retries for keeper operations during backup or restore

## backup_restore_keeper_retry_initial_backoff_ms {#backup_restore_keeper_retry_initial_backoff_ms}

Type: UInt64

Default value: 100

Initial backoff timeout for [Zoo]Keeper operations during backup or restore

## backup_restore_keeper_retry_max_backoff_ms {#backup_restore_keeper_retry_max_backoff_ms}

Type: UInt64

Default value: 5000

Max backoff timeout for [Zoo]Keeper operations during backup or restore

## backup_restore_keeper_value_max_size {#backup_restore_keeper_value_max_size}

Type: UInt64

Default value: 1048576

Maximum size of data of a [Zoo]Keeper's node during backup

## backup_restore_s3_retry_attempts {#backup_restore_s3_retry_attempts}

Type: UInt64

Default value: 1000

Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries. It takes place only for backup/restore.

## cache_warmer_threads {#cache_warmer_threads}

Type: UInt64

Default value: 4

Only available in ClickHouse Cloud. Number of background threads for speculatively downloading new data parts into file cache, when cache_populated_by_fetch is enabled. Zero to disable.

## calculate_text_stack_trace {#calculate_text_stack_trace}

Type: Bool

Default value: 1

Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when a huge amount of wrong queries are executed. In normal cases, you should not disable this option.

## cancel_http_readonly_queries_on_client_close {#cancel_http_readonly_queries_on_client_close}

Type: Bool

Default value: 0

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Cloud default value: `1`.

## cast_ipv4_ipv6_default_on_conversion_error {#cast_ipv4_ipv6_default_on_conversion_error}

Type: Bool

Default value: 0

CAST operator into IPv4, CAST operator into IPV6 type, toIPv4, toIPv6 functions will return default value instead of throwing exception on conversion error.

## cast_keep_nullable {#cast_keep_nullable}

Type: Bool

Default value: 0

Enables or disables keeping of the `Nullable` data type in [CAST](../../sql-reference/functions/type-conversion-functions.md/#castx-t) operations.

When the setting is enabled and the argument of `CAST` function is `Nullable`, the result is also transformed to `Nullable` type. When the setting is disabled, the result always has the destination type exactly.

Possible values:

- 0 — The `CAST` result has exactly the destination type specified.
- 1 — If the argument type is `Nullable`, the `CAST` result is transformed to `Nullable(DestinationDataType)`.

**Examples**

The following query results in the destination data type exactly:

```sql
SET cast_keep_nullable = 0;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Result:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Int32                                             │
└───┴───────────────────────────────────────────────────┘
```

The following query results in the `Nullable` modification on the destination data type:

```sql
SET cast_keep_nullable = 1;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Result:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Nullable(Int32)                                   │
└───┴───────────────────────────────────────────────────┘
```

**See Also**

- [CAST](../../sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) function

## cast_string_to_dynamic_use_inference {#cast_string_to_dynamic_use_inference}

Type: Bool

Default value: 0

Use types inference during String to Dynamic conversion

## check_query_single_value_result {#check_query_single_value_result}

Type: Bool

Default value: 1

Defines the level of detail for the [CHECK TABLE](../../sql-reference/statements/check-table.md/#checking-mergetree-tables) query result for `MergeTree` family engines .

Possible values:

- 0 — the query shows a check status for every individual data part of a table.
- 1 — the query shows the general table check status.

## check_referential_table_dependencies {#check_referential_table_dependencies}

Type: Bool

Default value: 0

Check that DDL query (such as DROP TABLE or RENAME) will not break referential dependencies

## check_table_dependencies {#check_table_dependencies}

Type: Bool

Default value: 1

Check that DDL query (such as DROP TABLE or RENAME) will not break dependencies

## checksum_on_read {#checksum_on_read}

Type: Bool

Default value: 1

Validate checksums on reading. It is enabled by default and should be always enabled in production. Please do not expect any benefits in disabling this setting. It may only be used for experiments and benchmarks. The setting is only applicable for tables of MergeTree family. Checksums are always validated for other table engines and when receiving data over the network.

## cloud_mode {#cloud_mode}

Type: Bool

Default value: 0

Cloud mode

## cloud_mode_database_engine {#cloud_mode_database_engine}

Type: UInt64

Default value: 1

The database engine allowed in Cloud. 1 - rewrite DDLs to use Replicated database, 2 - rewrite DDLs to use Shared database

## cloud_mode_engine {#cloud_mode_engine}

Type: UInt64

Default value: 1

The engine family allowed in Cloud. 0 - allow everything, 1 - rewrite DDLs to use *ReplicatedMergeTree, 2 - rewrite DDLs to use SharedMergeTree. UInt64 to minimize public part

## cluster_for_parallel_replicas {#cluster_for_parallel_replicas}

Type: String

Default value: 

Cluster for a shard in which current server is located

## collect_hash_table_stats_during_aggregation {#collect_hash_table_stats_during_aggregation}

Type: Bool

Default value: 1

Enable collecting hash table statistics to optimize memory allocation

## collect_hash_table_stats_during_joins {#collect_hash_table_stats_during_joins}

Type: Bool

Default value: 1

Enable collecting hash table statistics to optimize memory allocation

## compatibility {#compatibility}

Type: String

Default value: 

The `compatibility` setting causes ClickHouse to use the default settings of a previous version of ClickHouse, where the previous version is provided as the setting.

If settings are set to non-default values, then those settings are honored (only settings that have not been modified are affected by the `compatibility` setting).

This setting takes a ClickHouse version number as a string, like `22.3`, `22.8`. An empty value means that this setting is disabled.

Disabled by default.

:::note
In ClickHouse Cloud the compatibility setting must be set by ClickHouse Cloud support.  Please [open a case](https://clickhouse.cloud/support) to have it set.
:::

## compatibility_ignore_auto_increment_in_create_table {#compatibility_ignore_auto_increment_in_create_table}

Type: Bool

Default value: 0

Ignore AUTO_INCREMENT keyword in column declaration if true, otherwise return error. It simplifies migration from MySQL

## compatibility_ignore_collation_in_create_table {#compatibility_ignore_collation_in_create_table}

Type: Bool

Default value: 1

Compatibility ignore collation in create table

## compile_aggregate_expressions {#compile_aggregate_expressions}

Type: Bool

Default value: 1

Enables or disables JIT-compilation of aggregate functions to native code. Enabling this setting can improve the performance.

Possible values:

- 0 — Aggregation is done without JIT compilation.
- 1 — Aggregation is done using JIT compilation.

**See Also**

- [min_count_to_compile_aggregate_expression](#min_count_to_compile_aggregate_expression)

## compile_expressions {#compile_expressions}

Type: Bool

Default value: 0

Compile some scalar functions and operators to native code. Due to a bug in the LLVM compiler infrastructure, on AArch64 machines, it is known to lead to a nullptr dereference and, consequently, server crash. Do not enable this setting.

## compile_sort_description {#compile_sort_description}

Type: Bool

Default value: 1

Compile sort description to native code.

## connect_timeout {#connect_timeout}

Type: Seconds

Default value: 10

Connection timeout if there are no replicas.

## connect_timeout_with_failover_ms {#connect_timeout_with_failover_ms}

Type: Milliseconds

Default value: 1000

The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the ‘shard’ and ‘replica’ sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.

## connect_timeout_with_failover_secure_ms {#connect_timeout_with_failover_secure_ms}

Type: Milliseconds

Default value: 1000

Connection timeout for selecting first healthy replica (for secure connections).

## connection_pool_max_wait_ms {#connection_pool_max_wait_ms}

Type: Milliseconds

Default value: 0

The wait time in milliseconds for a connection when the connection pool is full.

Possible values:

- Positive integer.
- 0 — Infinite timeout.

## connections_with_failover_max_tries {#connections_with_failover_max_tries}

Type: UInt64

Default value: 3

The maximum number of connection attempts with each replica for the Distributed table engine.

## convert_query_to_cnf {#convert_query_to_cnf}

Type: Bool

Default value: 0

When set to `true`, a `SELECT` query will be converted to conjuctive normal form (CNF). There are scenarios where rewriting a query in CNF may execute faster (view this [Github issue](https://github.com/ClickHouse/ClickHouse/issues/11749) for an explanation).

For example, notice how the following `SELECT` query is not modified (the default behavior):

```sql
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT number AS x
    FROM numbers(20)
) AS a
WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))
SETTINGS convert_query_to_cnf = false;
```

The result is:

```response
┌─explain────────────────────────────────────────────────────────┐
│ SELECT x                                                       │
│ FROM                                                           │
│ (                                                              │
│     SELECT number AS x                                         │
│     FROM numbers(20)                                           │
│     WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) │
│ ) AS a                                                         │
│ WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))     │
│ SETTINGS convert_query_to_cnf = 0                              │
└────────────────────────────────────────────────────────────────┘
```

Let's set `convert_query_to_cnf` to `true` and see what changes:

```sql
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT number AS x
    FROM numbers(20)
) AS a
WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))
SETTINGS convert_query_to_cnf = true;
```

Notice the `WHERE` clause is rewritten in CNF, but the result set is the identical - the Boolean logic is unchanged:

```response
┌─explain───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ SELECT x                                                                                                              │
│ FROM                                                                                                                  │
│ (                                                                                                                     │
│     SELECT number AS x                                                                                                │
│     FROM numbers(20)                                                                                                  │
│     WHERE ((x <= 15) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x >= 10) OR (x >= 1)) │
│ ) AS a                                                                                                                │
│ WHERE ((x >= 10) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x <= 15) OR (x <= 5))     │
│ SETTINGS convert_query_to_cnf = 1                                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Possible values: true, false

## count_distinct_implementation {#count_distinct_implementation}

Type: String

Default value: uniqExact

Specifies which of the `uniq*` functions should be used to perform the [COUNT(DISTINCT ...)](../../sql-reference/aggregate-functions/reference/count.md/#agg_function-count) construction.

Possible values:

- [uniq](../../sql-reference/aggregate-functions/reference/uniq.md/#agg_function-uniq)
- [uniqCombined](../../sql-reference/aggregate-functions/reference/uniqcombined.md/#agg_function-uniqcombined)
- [uniqCombined64](../../sql-reference/aggregate-functions/reference/uniqcombined64.md/#agg_function-uniqcombined64)
- [uniqHLL12](../../sql-reference/aggregate-functions/reference/uniqhll12.md/#agg_function-uniqhll12)
- [uniqExact](../../sql-reference/aggregate-functions/reference/uniqexact.md/#agg_function-uniqexact)

## count_distinct_optimization {#count_distinct_optimization}

Type: Bool

Default value: 0

Rewrite count distinct to subquery of group by

## create_if_not_exists {#create_if_not_exists}

Type: Bool

Default value: 0

Enable `IF NOT EXISTS` for `CREATE` statement by default. If either this setting or `IF NOT EXISTS` is specified and a table with the provided name already exists, no exception will be thrown.

## create_index_ignore_unique {#create_index_ignore_unique}

Type: Bool

Default value: 0

Ignore UNIQUE keyword in CREATE UNIQUE INDEX. Made for SQL compatibility tests.

## create_replicated_merge_tree_fault_injection_probability {#create_replicated_merge_tree_fault_injection_probability}

Type: Float

Default value: 0

The probability of a fault injection during table creation after creating metadata in ZooKeeper

## create_table_empty_primary_key_by_default {#create_table_empty_primary_key_by_default}

Type: Bool

Default value: 0

Allow to create *MergeTree tables with empty primary key when ORDER BY and PRIMARY KEY not specified

## cross_join_min_bytes_to_compress {#cross_join_min_bytes_to_compress}

Type: UInt64

Default value: 1073741824

Minimal size of block to compress in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.

## cross_join_min_rows_to_compress {#cross_join_min_rows_to_compress}

Type: UInt64

Default value: 10000000

Minimal count of rows to compress block in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.

## data_type_default_nullable {#data_type_default_nullable}

Type: Bool

Default value: 0

Allows data types without explicit modifiers [NULL or NOT NULL](../../sql-reference/statements/create/table.md/#null-modifiers) in column definition will be [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable).

Possible values:

- 1 — The data types in column definitions are set to `Nullable` by default.
- 0 — The data types in column definitions are set to not `Nullable` by default.

## database_atomic_wait_for_drop_and_detach_synchronously {#database_atomic_wait_for_drop_and_detach_synchronously}

Type: Bool

Default value: 0

Adds a modifier `SYNC` to all `DROP` and `DETACH` queries.

Possible values:

- 0 — Queries will be executed with delay.
- 1 — Queries will be executed without delay.

## database_replicated_allow_explicit_uuid {#database_replicated_allow_explicit_uuid}

Type: UInt64

Default value: 0

0 - Don't allow to explicitly specify UUIDs for tables in Replicated databases. 1 - Allow. 2 - Allow, but ignore the specified UUID and generate a random one instead.

## database_replicated_allow_heavy_create {#database_replicated_allow_heavy_create}

Type: Bool

Default value: 0

Allow long-running DDL queries (CREATE AS SELECT and POPULATE) in Replicated database engine. Note that it can block DDL queue for a long time.

## database_replicated_allow_only_replicated_engine {#database_replicated_allow_only_replicated_engine}

Type: Bool

Default value: 0

Allow to create only Replicated tables in database with engine Replicated

## database_replicated_allow_replicated_engine_arguments {#database_replicated_allow_replicated_engine_arguments}

Type: UInt64

Default value: 0

0 - Don't allow to explicitly specify ZooKeeper path and replica name for *MergeTree tables in Replicated databases. 1 - Allow. 2 - Allow, but ignore the specified path and use default one instead. 3 - Allow and don't log a warning.

## database_replicated_always_detach_permanently {#database_replicated_always_detach_permanently}

Type: Bool

Default value: 0

Execute DETACH TABLE as DETACH TABLE PERMANENTLY if database engine is Replicated

## database_replicated_enforce_synchronous_settings {#database_replicated_enforce_synchronous_settings}

Type: Bool

Default value: 0

Enforces synchronous waiting for some queries (see also database_atomic_wait_for_drop_and_detach_synchronously, mutation_sync, alter_sync). Not recommended to enable these settings.

## database_replicated_initial_query_timeout_sec {#database_replicated_initial_query_timeout_sec}

Type: UInt64

Default value: 300

Sets how long initial DDL query should wait for Replicated database to process previous DDL queue entries in seconds.

Possible values:

- Positive integer.
- 0 — Unlimited.

## decimal_check_overflow {#decimal_check_overflow}

Type: Bool

Default value: 1

Check overflow of decimal arithmetic/comparison operations

## deduplicate_blocks_in_dependent_materialized_views {#deduplicate_blocks_in_dependent_materialized_views}

Type: Bool

Default value: 0

Enables or disables the deduplication check for materialized views that receive data from Replicated\* tables.

Possible values:

      0 — Disabled.
      1 — Enabled.

Usage

By default, deduplication is not performed for materialized views but is done upstream, in the source table.
If an INSERTed block is skipped due to deduplication in the source table, there will be no insertion into attached materialized views. This behaviour exists to enable the insertion of highly aggregated data into materialized views, for cases where inserted blocks are the same after materialized view aggregation but derived from different INSERTs into the source table.
At the same time, this behaviour “breaks” `INSERT` idempotency. If an `INSERT` into the main table was successful and `INSERT` into a materialized view failed (e.g. because of communication failure with ClickHouse Keeper) a client will get an error and can retry the operation. However, the materialized view won’t receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` allows for changing this behaviour. On retry, a materialized view will receive the repeat insert and will perform a deduplication check by itself,
ignoring check result for the source table, and will insert rows lost because of the first failure.

## default_materialized_view_sql_security {#default_materialized_view_sql_security}

Type: SQLSecurityType

Default value: DEFINER

Allows to set a default value for SQL SECURITY option when creating a materialized view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `DEFINER`.

## default_max_bytes_in_join {#default_max_bytes_in_join}

Type: UInt64

Default value: 1000000000

Maximum size of right-side table if limit is required but max_bytes_in_join is not set.

## default_normal_view_sql_security {#default_normal_view_sql_security}

Type: SQLSecurityType

Default value: INVOKER

Allows to set default `SQL SECURITY` option while creating a normal view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `INVOKER`.

## default_table_engine {#default_table_engine}

Type: DefaultTableEngine

Default value: MergeTree

Default table engine to use when `ENGINE` is not set in a `CREATE` statement.

Possible values:

- a string representing any valid table engine name

Cloud default value: `SharedMergeTree`.

**Example**

Query:

```sql
SET default_table_engine = 'Log';

SELECT name, value, changed FROM system.settings WHERE name = 'default_table_engine';
```

Result:

```response
┌─name─────────────────┬─value─┬─changed─┐
│ default_table_engine │ Log   │       1 │
└──────────────────────┴───────┴─────────┘
```

In this example, any new table that does not specify an `Engine` will use the `Log` table engine:

Query:

```sql
CREATE TABLE my_table (
    x UInt32,
    y UInt32
);

SHOW CREATE TABLE my_table;
```

Result:

```response
┌─statement────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.my_table
(
    `x` UInt32,
    `y` UInt32
)
ENGINE = Log
└──────────────────────────────────────────────────────────────────────────┘
```

## default_temporary_table_engine {#default_temporary_table_engine}

Type: DefaultTableEngine

Default value: Memory

Same as [default_table_engine](#default_table_engine) but for temporary tables.

In this example, any new temporary table that does not specify an `Engine` will use the `Log` table engine:

Query:

```sql
SET default_temporary_table_engine = 'Log';

CREATE TEMPORARY TABLE my_table (
    x UInt32,
    y UInt32
);

SHOW CREATE TEMPORARY TABLE my_table;
```

Result:

```response
┌─statement────────────────────────────────────────────────────────────────┐
│ CREATE TEMPORARY TABLE default.my_table
(
    `x` UInt32,
    `y` UInt32
)
ENGINE = Log
└──────────────────────────────────────────────────────────────────────────┘
```

## default_view_definer {#default_view_definer}

Type: String

Default value: CURRENT_USER

Allows to set default `DEFINER` option while creating a view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `CURRENT_USER`.

## describe_compact_output {#describe_compact_output}

Type: Bool

Default value: 0

If true, include only column names and types into result of DESCRIBE query

## describe_extend_object_types {#describe_extend_object_types}

Type: Bool

Default value: 0

Deduce concrete type of columns of type Object in DESCRIBE query

## describe_include_subcolumns {#describe_include_subcolumns}

Type: Bool

Default value: 0

Enables describing subcolumns for a [DESCRIBE](../../sql-reference/statements/describe-table.md) query. For example, members of a [Tuple](../../sql-reference/data-types/tuple.md) or subcolumns of a [Map](../../sql-reference/data-types/map.md/#map-subcolumns), [Nullable](../../sql-reference/data-types/nullable.md/#finding-null) or an [Array](../../sql-reference/data-types/array.md/#array-size) data type.

Possible values:

- 0 — Subcolumns are not included in `DESCRIBE` queries.
- 1 — Subcolumns are included in `DESCRIBE` queries.

**Example**

See an example for the [DESCRIBE](../../sql-reference/statements/describe-table.md) statement.

## describe_include_virtual_columns {#describe_include_virtual_columns}

Type: Bool

Default value: 0

If true, virtual columns of table will be included into result of DESCRIBE query

## dialect {#dialect}

Type: Dialect

Default value: clickhouse

Which dialect will be used to parse query

## dictionary_validate_primary_key_type {#dictionary_validate_primary_key_type}

Type: Bool

Default value: 0

Validate primary key type for dictionaries. By default id type for simple layouts will be implicitly converted to UInt64.

## distinct_overflow_mode {#distinct_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## distributed_aggregation_memory_efficient {#distributed_aggregation_memory_efficient}

Type: Bool

Default value: 1

Is the memory-saving mode of distributed aggregation enabled.

## distributed_background_insert_batch {#distributed_background_insert_batch}

Type: Bool

Default value: 0

Enables/disables inserted data sending in batches.

When batch sending is enabled, the [Distributed](../../engines/table-engines/special/distributed.md) table engine tries to send multiple files of inserted data in one operation instead of sending them separately. Batch sending improves cluster performance by better-utilizing server and network resources.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

## distributed_background_insert_max_sleep_time_ms {#distributed_background_insert_max_sleep_time_ms}

Type: Milliseconds

Default value: 30000

Maximum interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. Limits exponential growth of the interval set in the [distributed_background_insert_sleep_time_ms](#distributed_background_insert_sleep_time_ms) setting.

Possible values:

- A positive integer number of milliseconds.

## distributed_background_insert_sleep_time_ms {#distributed_background_insert_sleep_time_ms}

Type: Milliseconds

Default value: 100

Base interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. The actual interval grows exponentially in the event of errors.

Possible values:

- A positive integer number of milliseconds.

## distributed_background_insert_split_batch_on_failure {#distributed_background_insert_split_batch_on_failure}

Type: Bool

Default value: 0

Enables/disables splitting batches on failures.

Sometimes sending particular batch to the remote shard may fail, because of some complex pipeline after (i.e. `MATERIALIZED VIEW` with `GROUP BY`) due to `Memory limit exceeded` or similar errors. In this case, retrying will not help (and this will stuck distributed sends for the table) but sending files from that batch one by one may succeed INSERT.

So installing this setting to `1` will disable batching for such batches (i.e. temporary disables `distributed_background_insert_batch` for failed batches).

Possible values:

- 1 — Enabled.
- 0 — Disabled.

:::note
This setting also affects broken batches (that may appears because of abnormal server (machine) termination and no `fsync_after_insert`/`fsync_directories` for [Distributed](../../engines/table-engines/special/distributed.md) table engine).
:::

:::note
You should not rely on automatic batch splitting, since this may hurt performance.
:::

## distributed_background_insert_timeout {#distributed_background_insert_timeout}

Type: UInt64

Default value: 0

Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.

## distributed_cache_bypass_connection_pool {#distributed_cache_bypass_connection_pool}

Type: Bool

Default value: 0

Only in ClickHouse Cloud. Allow to bypass distributed cache connection pool

## distributed_cache_connect_max_tries {#distributed_cache_connect_max_tries}

Type: UInt64

Default value: 100

Only in ClickHouse Cloud. Number of tries to connect to distributed cache if unsuccessful

## distributed_cache_data_packet_ack_window {#distributed_cache_data_packet_ack_window}

Type: UInt64

Default value: 5

Only in ClickHouse Cloud. A window for sending ACK for DataPacket sequence in a single distributed cache read request

## distributed_cache_fetch_metrics_only_from_current_az {#distributed_cache_fetch_metrics_only_from_current_az}

Type: Bool

Default value: 1

Only in ClickHouse Cloud. Fetch metrics only from current availability zone in system.distributed_cache_metrics, system.distributed_cache_events

## distributed_cache_log_mode {#distributed_cache_log_mode}

Type: DistributedCacheLogMode

Default value: on_error

Only in ClickHouse Cloud. Mode for writing to system.distributed_cache_log

## distributed_cache_max_unacked_inflight_packets {#distributed_cache_max_unacked_inflight_packets}

Type: UInt64

Default value: 10

Only in ClickHouse Cloud. A maximum number of unacknowledged in-flight packets in a single distributed cache read request

## distributed_cache_pool_behaviour_on_limit {#distributed_cache_pool_behaviour_on_limit}

Type: DistributedCachePoolBehaviourOnLimit

Default value: allocate_bypassing_pool

Only in ClickHouse Cloud. Identifies behaviour of distributed cache connection on pool limit reached

## distributed_cache_read_alignment {#distributed_cache_read_alignment}

Type: UInt64

Default value: 0

Only in ClickHouse Cloud. A setting for testing purposes, do not change it

## distributed_cache_receive_response_wait_milliseconds {#distributed_cache_receive_response_wait_milliseconds}

Type: UInt64

Default value: 60000

Only in ClickHouse Cloud. Wait time in milliseconds to receive data for request from distributed cache

## distributed_cache_receive_timeout_milliseconds {#distributed_cache_receive_timeout_milliseconds}

Type: UInt64

Default value: 10000

Only in ClickHouse Cloud. Wait time in milliseconds to receive any kind of response from distributed cache

## distributed_cache_throw_on_error {#distributed_cache_throw_on_error}

Type: Bool

Default value: 0

Only in ClickHouse Cloud. Rethrow exception happened during communication with distributed cache or exception received from distributed cache. Otherwise fallback to skipping distributed cache on error

## distributed_cache_wait_connection_from_pool_milliseconds {#distributed_cache_wait_connection_from_pool_milliseconds}

Type: UInt64

Default value: 100

Only in ClickHouse Cloud. Wait time in milliseconds to receive connection from connection pool if distributed_cache_pool_behaviour_on_limit is wait

## distributed_connections_pool_size {#distributed_connections_pool_size}

Type: UInt64

Default value: 1024

The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

## distributed_ddl_entry_format_version {#distributed_ddl_entry_format_version}

Type: UInt64

Default value: 5

Compatibility version of distributed DDL (ON CLUSTER) queries

## distributed_ddl_output_mode {#distributed_ddl_output_mode}

Type: DistributedDDLOutputMode

Default value: throw

Sets format of distributed DDL query result.

Possible values:

- `throw` — Returns result set with query execution status for all hosts where query is finished. If query has failed on some hosts, then it will rethrow the first exception. If query is not finished yet on some hosts and [distributed_ddl_task_timeout](#distributed_ddl_task_timeout) exceeded, then it throws `TIMEOUT_EXCEEDED` exception.
- `none` — Is similar to throw, but distributed DDL query returns no result set.
- `null_status_on_timeout` — Returns `NULL` as execution status in some rows of result set instead of throwing `TIMEOUT_EXCEEDED` if query is not finished on the corresponding hosts.
- `never_throw` — Do not throw `TIMEOUT_EXCEEDED` and do not rethrow exceptions if query has failed on some hosts.
- `none_only_active` - similar to `none`, but doesn't wait for inactive replicas of the `Replicated` database. Note: with this mode it's impossible to figure out that the query was not executed on some replica and will be executed in background.
- `null_status_on_timeout_only_active` — similar to `null_status_on_timeout`, but doesn't wait for inactive replicas of the `Replicated` database
- `throw_only_active` — similar to `throw`, but doesn't wait for inactive replicas of the `Replicated` database

Cloud default value: `none`.

## distributed_ddl_task_timeout {#distributed_ddl_task_timeout}

Type: Int64

Default value: 180

Sets timeout for DDL query responses from all hosts in cluster. If a DDL request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite.

Possible values:

- Positive integer.
- 0 — Async mode.
- Negative integer — infinite timeout.

## distributed_foreground_insert {#distributed_foreground_insert}

Type: Bool

Default value: 0

Enables or disables synchronous data insertion into a [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table.

By default, when inserting data into a `Distributed` table, the ClickHouse server sends data to cluster nodes in background mode. When `distributed_foreground_insert=1`, the data is processed synchronously, and the `INSERT` operation succeeds only after all the data is saved on all shards (at least one replica for each shard if `internal_replication` is true).

Possible values:

- 0 — Data is inserted in background mode.
- 1 — Data is inserted in synchronous mode.

Cloud default value: `1`.

**See Also**

- [Distributed Table Engine](../../engines/table-engines/special/distributed.md/#distributed)
- [Managing Distributed Tables](../../sql-reference/statements/system.md/#query-language-system-distributed)

## distributed_group_by_no_merge {#distributed_group_by_no_merge}

Type: UInt64

Default value: 0

Do not merge aggregation states from different servers for distributed query processing, you can use this in case it is for certain that there are different keys on different shards

Possible values:

- `0` — Disabled (final query processing is done on the initiator node).
- `1` - Do not merge aggregation states from different servers for distributed query processing (query completely processed on the shard, initiator only proxy the data), can be used in case it is for certain that there are different keys on different shards.
- `2` - Same as `1` but applies `ORDER BY` and `LIMIT` (it is not possible when the query processed completely on the remote node, like for `distributed_group_by_no_merge=1`) on the initiator (can be used for queries with `ORDER BY` and/or `LIMIT`).

**Example**

```sql
SELECT *
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY dummy
LIMIT 1
SETTINGS distributed_group_by_no_merge = 1
FORMAT PrettyCompactMonoBlock

┌─dummy─┐
│     0 │
│     0 │
└───────┘
```

```sql
SELECT *
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY dummy
LIMIT 1
SETTINGS distributed_group_by_no_merge = 2
FORMAT PrettyCompactMonoBlock

┌─dummy─┐
│     0 │
└───────┘
```

## distributed_insert_skip_read_only_replicas {#distributed_insert_skip_read_only_replicas}

Type: Bool

Default value: 0

Enables skipping read-only replicas for INSERT queries into Distributed.

Possible values:

- 0 — INSERT was as usual, if it will go to read-only replica it will fail
- 1 — Initiator will skip read-only replicas before sending data to shards.

## distributed_product_mode {#distributed_product_mode}

Type: DistributedProductMode

Default value: deny

Changes the behaviour of [distributed subqueries](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restrictions:

- Only applied for IN and JOIN subqueries.
- Only if the FROM section uses a distributed table containing more than one shard.
- If the subquery concerns a distributed table containing more than one shard.
- Not used for a table-valued [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

- `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” exception).
- `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
- `global` — Replaces the `IN`/`JOIN` query with `GLOBAL IN`/`GLOBAL JOIN.`
- `allow` — Allows the use of these types of subqueries.

## distributed_push_down_limit {#distributed_push_down_limit}

Type: UInt64

Default value: 1

Enables or disables [LIMIT](#limit) applying on each shard separately.

This will allow to avoid:
- Sending extra rows over network;
- Processing rows behind the limit on the initiator.

Starting from 21.9 version you cannot get inaccurate results anymore, since `distributed_push_down_limit` changes query execution only if at least one of the conditions met:
- [distributed_group_by_no_merge](#distributed-group-by-no-merge) > 0.
- Query **does not have** `GROUP BY`/`DISTINCT`/`LIMIT BY`, but it has `ORDER BY`/`LIMIT`.
- Query **has** `GROUP BY`/`DISTINCT`/`LIMIT BY` with `ORDER BY`/`LIMIT` and:
    - [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled.
    - [optimize_distributed_group_by_sharding_key](#optimize-distributed-group-by-sharding-key) is enabled.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [distributed_group_by_no_merge](#distributed-group-by-no-merge)
- [optimize_skip_unused_shards](#optimize-skip-unused-shards)
- [optimize_distributed_group_by_sharding_key](#optimize-distributed-group-by-sharding-key)

## distributed_replica_error_cap {#distributed_replica_error_cap}

Type: UInt64

Default value: 1000

- Type: unsigned int
- Default value: 1000

The error count of each replica is capped at this value, preventing a single replica from accumulating too many errors.

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_half_life](#distributed_replica_error_half_life)
- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

## distributed_replica_error_half_life {#distributed_replica_error_half_life}

Type: Seconds

Default value: 60

- Type: seconds
- Default value: 60 seconds

Controls how fast errors in distributed tables are zeroed. If a replica is unavailable for some time, accumulates 5 errors, and distributed_replica_error_half_life is set to 1 second, then the replica is considered normal 3 seconds after the last error.

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap](#distributed_replica_error_cap)
- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

## distributed_replica_max_ignored_errors {#distributed_replica_max_ignored_errors}

Type: UInt64

Default value: 0

- Type: unsigned int
- Default value: 0

The number of errors that will be ignored while choosing replicas (according to `load_balancing` algorithm).

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap](#distributed_replica_error_cap)
- [distributed_replica_error_half_life](#distributed_replica_error_half_life)

## do_not_merge_across_partitions_select_final {#do_not_merge_across_partitions_select_final}

Type: Bool

Default value: 0

Merge parts only in one partition in select final

## empty_result_for_aggregation_by_constant_keys_on_empty_set {#empty_result_for_aggregation_by_constant_keys_on_empty_set}

Type: Bool

Default value: 1

Return empty result when aggregating by constant keys on empty set.

## empty_result_for_aggregation_by_empty_set {#empty_result_for_aggregation_by_empty_set}

Type: Bool

Default value: 0

Return empty result when aggregating without keys on empty set.

## enable_blob_storage_log {#enable_blob_storage_log}

Type: Bool

Default value: 1

Write information about blob storage operations to system.blob_storage_log table

## enable_deflate_qpl_codec {#enable_deflate_qpl_codec}

Type: Bool

Default value: 0

If turned on, the DEFLATE_QPL codec may be used to compress columns.

Possible values:

- 0 - Disabled
- 1 - Enabled

## enable_early_constant_folding {#enable_early_constant_folding}

Type: Bool

Default value: 1

Enable query optimization where we analyze function and subqueries results and rewrite query if there are constants there

## enable_extended_results_for_datetime_functions {#enable_extended_results_for_datetime_functions}

Type: Bool

Default value: 0

Enables or disables returning results of type:
- `Date32` with extended range (compared to type `Date`) for functions [toStartOfYear](../../sql-reference/functions/date-time-functions.md#tostartofyear), [toStartOfISOYear](../../sql-reference/functions/date-time-functions.md#tostartofisoyear), [toStartOfQuarter](../../sql-reference/functions/date-time-functions.md#tostartofquarter), [toStartOfMonth](../../sql-reference/functions/date-time-functions.md#tostartofmonth), [toLastDayOfMonth](../../sql-reference/functions/date-time-functions.md#tolastdayofmonth), [toStartOfWeek](../../sql-reference/functions/date-time-functions.md#tostartofweek), [toLastDayOfWeek](../../sql-reference/functions/date-time-functions.md#tolastdayofweek) and [toMonday](../../sql-reference/functions/date-time-functions.md#tomonday).
- `DateTime64` with extended range (compared to type `DateTime`) for functions [toStartOfDay](../../sql-reference/functions/date-time-functions.md#tostartofday), [toStartOfHour](../../sql-reference/functions/date-time-functions.md#tostartofhour), [toStartOfMinute](../../sql-reference/functions/date-time-functions.md#tostartofminute), [toStartOfFiveMinutes](../../sql-reference/functions/date-time-functions.md#tostartoffiveminutes), [toStartOfTenMinutes](../../sql-reference/functions/date-time-functions.md#tostartoftenminutes), [toStartOfFifteenMinutes](../../sql-reference/functions/date-time-functions.md#tostartoffifteenminutes) and [timeSlot](../../sql-reference/functions/date-time-functions.md#timeslot).

Possible values:

- 0 — Functions return `Date` or `DateTime` for all types of arguments.
- 1 — Functions return `Date32` or `DateTime64` for `Date32` or `DateTime64` arguments and `Date` or `DateTime` otherwise.

## enable_filesystem_cache {#enable_filesystem_cache}

Type: Bool

Default value: 1

Use cache for remote filesystem. This setting does not turn on/off cache for disks (must be done via disk config), but allows to bypass cache for some queries if intended

## enable_filesystem_cache_log {#enable_filesystem_cache_log}

Type: Bool

Default value: 0

Allows to record the filesystem caching log for each query

## enable_filesystem_cache_on_write_operations {#enable_filesystem_cache_on_write_operations}

Type: Bool

Default value: 0

Write into cache on write operations. To actually work this setting requires be added to disk config too

## enable_filesystem_read_prefetches_log {#enable_filesystem_read_prefetches_log}

Type: Bool

Default value: 0

Log to system.filesystem prefetch_log during query. Should be used only for testing or debugging, not recommended to be turned on by default

## enable_global_with_statement {#enable_global_with_statement}

Type: Bool

Default value: 1

Propagate WITH statements to UNION queries and all subqueries

## enable_http_compression {#enable_http_compression}

Type: Bool

Default value: 0

Enables or disables data compression in the response to an HTTP request.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## enable_job_stack_trace {#enable_job_stack_trace}

Type: Bool

Default value: 0

Output stack trace of a job creator when job results in exception

## enable_lightweight_delete {#enable_lightweight_delete}

Type: Bool

Default value: 1

Enable lightweight DELETE mutations for mergetree tables.

## enable_memory_bound_merging_of_aggregation_results {#enable_memory_bound_merging_of_aggregation_results}

Type: Bool

Default value: 1

Enable memory bound merging strategy for aggregation.

## enable_multiple_prewhere_read_steps {#enable_multiple_prewhere_read_steps}

Type: Bool

Default value: 1

Move more conditions from WHERE to PREWHERE and do reads from disk and filtering in multiple steps if there are multiple conditions combined with AND

## enable_named_columns_in_function_tuple {#enable_named_columns_in_function_tuple}

Type: Bool

Default value: 1

Generate named tuples in function tuple() when all names are unique and can be treated as unquoted identifiers.

## enable_optimize_predicate_expression {#enable_optimize_predicate_expression}

Type: Bool

Default value: 1

Turns on predicate pushdown in `SELECT` queries.

Predicate pushdown may significantly reduce network traffic for distributed queries.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Usage

Consider the following queries:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

If `enable_optimize_predicate_expression = 1`, then the execution time of these queries is equal because ClickHouse applies `WHERE` to the subquery when processing it.

If `enable_optimize_predicate_expression = 0`, then the execution time of the second query is much longer because the `WHERE` clause applies to all the data after the subquery finishes.

## enable_optimize_predicate_expression_to_final_subquery {#enable_optimize_predicate_expression_to_final_subquery}

Type: Bool

Default value: 1

Allow push predicate to final subquery.

## enable_order_by_all {#enable_order_by_all}

Type: Bool

Default value: 1

Enables or disables sorting with `ORDER BY ALL` syntax, see [ORDER BY](../../sql-reference/statements/select/order-by.md).

Possible values:

- 0 — Disable ORDER BY ALL.
- 1 — Enable ORDER BY ALL.

**Example**

Query:

```sql
CREATE TABLE TAB(C1 Int, C2 Int, ALL Int) ENGINE=Memory();

INSERT INTO TAB VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM TAB ORDER BY ALL; -- returns an error that ALL is ambiguous

SELECT * FROM TAB ORDER BY ALL SETTINGS enable_order_by_all = 0;
```

Result:

```text
┌─C1─┬─C2─┬─ALL─┐
│ 20 │ 20 │  10 │
│ 30 │ 10 │  20 │
│ 10 │ 20 │  30 │
└────┴────┴─────┘
```

## enable_parsing_to_custom_serialization {#enable_parsing_to_custom_serialization}

Type: Bool

Default value: 1

If true then data can be parsed directly to columns with custom serialization (e.g. Sparse) according to hints for serialization got from the table.

## enable_positional_arguments {#enable_positional_arguments}

Type: Bool

Default value: 1

Enables or disables supporting positional arguments for [GROUP BY](../../sql-reference/statements/select/group-by.md), [LIMIT BY](../../sql-reference/statements/select/limit-by.md), [ORDER BY](../../sql-reference/statements/select/order-by.md) statements.

Possible values:

- 0 — Positional arguments aren't supported.
- 1 — Positional arguments are supported: column numbers can use instead of column names.

**Example**

Query:

```sql
CREATE TABLE positional_arguments(one Int, two Int, three Int) ENGINE=Memory();

INSERT INTO positional_arguments VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM positional_arguments ORDER BY 2,3;
```

Result:

```text
┌─one─┬─two─┬─three─┐
│  30 │  10 │   20  │
│  20 │  20 │   10  │
│  10 │  20 │   30  │
└─────┴─────┴───────┘
```

## enable_reads_from_query_cache {#enable_reads_from_query_cache}

Type: Bool

Default value: 1

If turned on, results of `SELECT` queries are retrieved from the [query cache](../query-cache.md).

Possible values:

- 0 - Disabled
- 1 - Enabled

## enable_s3_requests_logging {#enable_s3_requests_logging}

Type: Bool

Default value: 0

Enable very explicit logging of S3 requests. Makes sense for debug only.

## enable_scalar_subquery_optimization {#enable_scalar_subquery_optimization}

Type: Bool

Default value: 1

If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.

## enable_secure_identifiers {#enable_secure_identifiers}

Type: Bool

Default value: 0

If enabled, only allow secure identifiers which contain only underscore and alphanumeric characters

## enable_sharing_sets_for_mutations {#enable_sharing_sets_for_mutations}

Type: Bool

Default value: 1

Allow sharing set objects build for IN subqueries between different tasks of the same mutation. This reduces memory usage and CPU consumption

## enable_software_prefetch_in_aggregation {#enable_software_prefetch_in_aggregation}

Type: Bool

Default value: 1

Enable use of software prefetch in aggregation

## enable_unaligned_array_join {#enable_unaligned_array_join}

Type: Bool

Default value: 0

Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.

## enable_url_encoding {#enable_url_encoding}

Type: Bool

Default value: 1

Allows to enable/disable decoding/encoding path in uri in [URL](../../engines/table-engines/special/url.md) engine tables.

Enabled by default.

## enable_vertical_final {#enable_vertical_final}

Type: Bool

Default value: 1

If enable, remove duplicated rows during FINAL by marking rows as deleted and filtering them later instead of merging rows

## enable_writes_to_query_cache {#enable_writes_to_query_cache}

Type: Bool

Default value: 1

If turned on, results of `SELECT` queries are stored in the [query cache](../query-cache.md).

Possible values:

- 0 - Disabled
- 1 - Enabled

## enable_zstd_qat_codec {#enable_zstd_qat_codec}

Type: Bool

Default value: 0

If turned on, the ZSTD_QAT codec may be used to compress columns.

Possible values:

- 0 - Disabled
- 1 - Enabled

## engine_file_allow_create_multiple_files {#engine_file_allow_create_multiple_files}

Type: Bool

Default value: 0

Enables or disables creating a new file on each insert in file engine tables if the format has the suffix (`JSON`, `ORC`, `Parquet`, etc.). If enabled, on each insert a new file will be created with a name following this pattern:

`data.Parquet` -> `data.1.Parquet` -> `data.2.Parquet`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.

## engine_file_empty_if_not_exists {#engine_file_empty_if_not_exists}

Type: Bool

Default value: 0

Allows to select data from a file engine table without file.

Possible values:
- 0 — `SELECT` throws exception.
- 1 — `SELECT` returns empty result.

## engine_file_skip_empty_files {#engine_file_skip_empty_files}

Type: Bool

Default value: 0

Enables or disables skipping empty files in [File](../../engines/table-engines/special/file.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

## engine_file_truncate_on_insert {#engine_file_truncate_on_insert}

Type: Bool

Default value: 0

Enables or disables truncate before insert in [File](../../engines/table-engines/special/file.md) engine tables.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.

## engine_url_skip_empty_files {#engine_url_skip_empty_files}

Type: Bool

Default value: 0

Enables or disables skipping empty files in [URL](../../engines/table-engines/special/url.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

## except_default_mode {#except_default_mode}

Type: SetOperationMode

Default value: ALL

Set default mode in EXCEPT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.

## external_storage_connect_timeout_sec {#external_storage_connect_timeout_sec}

Type: UInt64

Default value: 10

Connect timeout in seconds. Now supported only for MySQL

## external_storage_max_read_bytes {#external_storage_max_read_bytes}

Type: UInt64

Default value: 0

Limit maximum number of bytes when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled

## external_storage_max_read_rows {#external_storage_max_read_rows}

Type: UInt64

Default value: 0

Limit maximum number of rows when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled

## external_storage_rw_timeout_sec {#external_storage_rw_timeout_sec}

Type: UInt64

Default value: 300

Read/write timeout in seconds. Now supported only for MySQL

## external_table_functions_use_nulls {#external_table_functions_use_nulls}

Type: Bool

Default value: 1

Defines how [mysql](../../sql-reference/table-functions/mysql.md), [postgresql](../../sql-reference/table-functions/postgresql.md) and [odbc](../../sql-reference/table-functions/odbc.md) table functions use Nullable columns.

Possible values:

- 0 — The table function explicitly uses Nullable columns.
- 1 — The table function implicitly uses Nullable columns.

**Usage**

If the setting is set to `0`, the table function does not make Nullable columns and inserts default values instead of NULL. This is also applicable for NULL values inside arrays.

## external_table_strict_query {#external_table_strict_query}

Type: Bool

Default value: 0

If it is set to true, transforming expression to local filter is forbidden for queries to external tables.

## extract_key_value_pairs_max_pairs_per_row {#extract_key_value_pairs_max_pairs_per_row}

Type: UInt64

Default value: 1000

Max number of pairs that can be produced by the `extractKeyValuePairs` function. Used as a safeguard against consuming too much memory.

## extremes {#extremes}

Type: Bool

Default value: 0

Whether to count extreme values (the minimums and maximums in columns of a query result). Accepts 0 or 1. By default, 0 (disabled).
For more information, see the section “Extreme values”.

## fallback_to_stale_replicas_for_distributed_queries {#fallback_to_stale_replicas_for_distributed_queries}

Type: Bool

Default value: 1

Forces a query to an out-of-date replica if updated data is not available. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing `SELECT` from a distributed table that points to replicated tables.

By default, 1 (enabled).

## filesystem_cache_max_download_size {#filesystem_cache_max_download_size}

Type: UInt64

Default value: 137438953472

Max remote filesystem cache size that can be downloaded by a single query

## filesystem_cache_reserve_space_wait_lock_timeout_milliseconds {#filesystem_cache_reserve_space_wait_lock_timeout_milliseconds}

Type: UInt64

Default value: 1000

Wait time to lock cache for space reservation in filesystem cache

## filesystem_cache_segments_batch_size {#filesystem_cache_segments_batch_size}

Type: UInt64

Default value: 20

Limit on size of a single batch of file segments that a read buffer can request from cache. Too low value will lead to excessive requests to cache, too large may slow down eviction from cache

## filesystem_prefetch_max_memory_usage {#filesystem_prefetch_max_memory_usage}

Type: UInt64

Default value: 1073741824

Maximum memory usage for prefetches.

## filesystem_prefetch_step_bytes {#filesystem_prefetch_step_bytes}

Type: UInt64

Default value: 0

Prefetch step in bytes. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task

## filesystem_prefetch_step_marks {#filesystem_prefetch_step_marks}

Type: UInt64

Default value: 0

Prefetch step in marks. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task

## filesystem_prefetches_limit {#filesystem_prefetches_limit}

Type: UInt64

Default value: 200

Maximum number of prefetches. Zero means unlimited. A setting `filesystem_prefetches_max_memory_usage` is more recommended if you want to limit the number of prefetches

## final {#final}

Type: Bool

Default value: 0

Automatically applies [FINAL](../../sql-reference/statements/select/from.md#final-modifier) modifier to all tables in a query, to tables where [FINAL](../../sql-reference/statements/select/from.md#final-modifier) is applicable, including joined tables and tables in sub-queries, and
distributed tables.

Possible values:

- 0 - disabled
- 1 - enabled

Example:

```sql
CREATE TABLE test
(
    key Int64,
    some String
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO test FORMAT Values (1, 'first');
INSERT INTO test FORMAT Values (1, 'second');

SELECT * FROM test;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘
┌─key─┬─some──┐
│   1 │ first │
└─────┴───────┘

SELECT * FROM test SETTINGS final = 1;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘

SET final = 1;
SELECT * FROM test;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘
```

## flatten_nested {#flatten_nested}

Type: Bool

Default value: 1

Sets the data format of a [nested](../../sql-reference/data-types/nested-data-structures/index.md) columns.

Possible values:

- 1 — Nested column is flattened to separate arrays.
- 0 — Nested column stays a single array of tuples.

**Usage**

If the setting is set to `0`, it is possible to use an arbitrary level of nesting.

**Examples**

Query:

``` sql
SET flatten_nested = 1;
CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Result:

``` text
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n.a` Array(UInt32),
    `n.b` Array(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SET flatten_nested = 0;

CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Result:

``` text
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n` Nested(a UInt32, b UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## force_aggregate_partitions_independently {#force_aggregate_partitions_independently}

Type: Bool

Default value: 0

Force the use of optimization when it is applicable, but heuristics decided not to use it

## force_aggregation_in_order {#force_aggregation_in_order}

Type: Bool

Default value: 0

The setting is used by the server itself to support distributed queries. Do not change it manually, because it will break normal operations. (Forces use of aggregation in order on remote nodes during distributed aggregation).

## force_data_skipping_indices {#force_data_skipping_indices}

Type: String

Default value: 

Disables query execution if passed data skipping indices wasn't used.

Consider the following example:

```sql
CREATE TABLE data
(
    key Int,
    d1 Int,
    d1_null Nullable(Int),
    INDEX d1_idx d1 TYPE minmax GRANULARITY 1,
    INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

SELECT * FROM data_01515;
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error.
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_idx'; -- query will produce INDEX_NOT_USED error.
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx'; -- Ok.
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`'; -- Ok (example of full featured parser).
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- query will produce INDEX_NOT_USED error, since d1_null_idx is not used.
SELECT * FROM data_01515 WHERE d1 = 0 AND assumeNotNull(d1_null) = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- Ok.
```

## force_grouping_standard_compatibility {#force_grouping_standard_compatibility}

Type: Bool

Default value: 1

Make GROUPING function to return 1 when argument is not used as an aggregation key

## force_index_by_date {#force_index_by_date}

Type: Bool

Default value: 0

Disables query execution if the index can’t be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_optimize_projection {#force_optimize_projection}

Type: Bool

Default value: 0

Enables or disables the obligatory use of [projections](../../engines/table-engines/mergetree-family/mergetree.md/#projections) in `SELECT` queries, when projection optimization is enabled (see [optimize_use_projections](#optimize_use_projections) setting).

Possible values:

- 0 — Projection optimization is not obligatory.
- 1 — Projection optimization is obligatory.

## force_optimize_projection_name {#force_optimize_projection_name}

Type: String

Default value: 

If it is set to a non-empty string, check that this projection is used in the query at least once.

Possible values:

- string: name of projection that used in a query

## force_optimize_skip_unused_shards {#force_optimize_skip_unused_shards}

Type: UInt64

Default value: 0

Enables or disables query execution if [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled, an exception will be thrown.

Possible values:

- 0 — Disabled. ClickHouse does not throw an exception.
- 1 — Enabled. Query execution is disabled only if the table has a sharding key.
- 2 — Enabled. Query execution is disabled regardless of whether a sharding key is defined for the table.

## force_optimize_skip_unused_shards_nesting {#force_optimize_skip_unused_shards_nesting}

Type: UInt64

Default value: 0

Controls [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards) (hence still requires [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

- 0 - Disabled, `force_optimize_skip_unused_shards` works always.
- 1 — Enables `force_optimize_skip_unused_shards` only for the first level.
- 2 — Enables `force_optimize_skip_unused_shards` up to the second level.

## force_primary_key {#force_primary_key}

Type: Bool

Default value: 0

Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For more information about data ranges in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_remove_data_recursively_on_drop {#force_remove_data_recursively_on_drop}

Type: Bool

Default value: 0

Recursively remove data on DROP query. Avoids 'Directory not empty' error, but may silently remove detached data

## formatdatetime_f_prints_single_zero {#formatdatetime_f_prints_single_zero}

Type: Bool

Default value: 0

Formatter '%f' in function 'formatDateTime()' prints a single zero instead of six zeros if the formatted value has no fractional seconds.

## formatdatetime_format_without_leading_zeros {#formatdatetime_format_without_leading_zeros}

Type: Bool

Default value: 0

Formatters '%c', '%l' and '%k' in function 'formatDateTime()' print months and hours without leading zeros.

## formatdatetime_parsedatetime_m_is_month_name {#formatdatetime_parsedatetime_m_is_month_name}

Type: Bool

Default value: 1

Formatter '%M' in functions 'formatDateTime()' and 'parseDateTime()' print/parse the month name instead of minutes.

## fsync_metadata {#fsync_metadata}

Type: Bool

Default value: 1

Enables or disables [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) when writing `.sql` files. Enabled by default.

It makes sense to disable it if the server has millions of tiny tables that are constantly being created and destroyed.

## function_implementation {#function_implementation}

Type: String

Default value: 

Choose function implementation for specific target or variant (experimental). If empty enable all of them.

## function_json_value_return_type_allow_complex {#function_json_value_return_type_allow_complex}

Type: Bool

Default value: 0

Control whether allow to return complex type (such as: struct, array, map) for json_value function.

```sql
SELECT JSON_VALUE('{"hello":{"world":"!"}}', '$.hello') settings function_json_value_return_type_allow_complex=true

┌─JSON_VALUE('{"hello":{"world":"!"}}', '$.hello')─┐
│ {"world":"!"}                                    │
└──────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

Possible values:

- true — Allow.
- false — Disallow.

## function_json_value_return_type_allow_nullable {#function_json_value_return_type_allow_nullable}

Type: Bool

Default value: 0

Control whether allow to return `NULL` when value is not exist for JSON_VALUE function.

```sql
SELECT JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true;

┌─JSON_VALUE('{"hello":"world"}', '$.b')─┐
│ ᴺᵁᴸᴸ                                   │
└────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

Possible values:

- true — Allow.
- false — Disallow.

## function_locate_has_mysql_compatible_argument_order {#function_locate_has_mysql_compatible_argument_order}

Type: Bool

Default value: 1

Controls the order of arguments in function [locate](../../sql-reference/functions/string-search-functions.md#locate).

Possible values:

- 0 — Function `locate` accepts arguments `(haystack, needle[, start_pos])`.
- 1 — Function `locate` accepts arguments `(needle, haystack, [, start_pos])` (MySQL-compatible behavior)

## function_range_max_elements_in_block {#function_range_max_elements_in_block}

Type: UInt64

Default value: 500000000

Sets the safety threshold for data volume generated by function [range](../../sql-reference/functions/array-functions.md/#range). Defines the maximum number of values generated by function per block of data (sum of array sizes for every row in a block).

Possible values:

- Positive integer.

**See Also**

- [max_block_size](#setting-max_block_size)
- [min_insert_block_size_rows](#min-insert-block-size-rows)

## function_sleep_max_microseconds_per_block {#function_sleep_max_microseconds_per_block}

Type: UInt64

Default value: 3000000

Maximum number of microseconds the function `sleep` is allowed to sleep for each block. If a user called it with a larger value, it throws an exception. It is a safety threshold.

## function_visible_width_behavior {#function_visible_width_behavior}

Type: UInt64

Default value: 1

The version of `visibleWidth` behavior. 0 - only count the number of code points; 1 - correctly count zero-width and combining characters, count full-width characters as two, estimate the tab width, count delete characters.

## geo_distance_returns_float64_on_float64_arguments {#geo_distance_returns_float64_on_float64_arguments}

Type: Bool

Default value: 1

If all four arguments to `geoDistance`, `greatCircleDistance`, `greatCircleAngle` functions are Float64, return Float64 and use double precision for internal calculations. In previous ClickHouse versions, the functions always returned Float32.

## glob_expansion_max_elements {#glob_expansion_max_elements}

Type: UInt64

Default value: 1000

Maximum number of allowed addresses (For external storages, table functions, etc).

## grace_hash_join_initial_buckets {#grace_hash_join_initial_buckets}

Type: UInt64

Default value: 1

Initial number of grace hash join buckets

## grace_hash_join_max_buckets {#grace_hash_join_max_buckets}

Type: UInt64

Default value: 1024

Limit on the number of grace hash join buckets

## group_by_overflow_mode {#group_by_overflow_mode}

Type: OverflowModeGroupBy

Default value: throw

What to do when the limit is exceeded.

## group_by_two_level_threshold {#group_by_two_level_threshold}

Type: UInt64

Default value: 100000

From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.

## group_by_two_level_threshold_bytes {#group_by_two_level_threshold_bytes}

Type: UInt64

Default value: 50000000

From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.

## group_by_use_nulls {#group_by_use_nulls}

Type: Bool

Default value: 0

Changes the way the [GROUP BY clause](/docs/en/sql-reference/statements/select/group-by.md) treats the types of aggregation keys.
When the `ROLLUP`, `CUBE`, or `GROUPING SETS` specifiers are used, some aggregation keys may not be used to produce some result rows.
Columns for these keys are filled with either default value or `NULL` in corresponding rows depending on this setting.

Possible values:

- 0 — The default value for the aggregation key type is used to produce missing values.
- 1 — ClickHouse executes `GROUP BY` the same way as the SQL standard says. The types of aggregation keys are converted to [Nullable](/docs/en/sql-reference/data-types/nullable.md/#data_type-nullable). Columns for corresponding aggregation keys are filled with [NULL](/docs/en/sql-reference/syntax.md) for rows that didn't use it.

See also:

- [GROUP BY clause](/docs/en/sql-reference/statements/select/group-by.md)

## handshake_timeout_ms {#handshake_timeout_ms}

Type: Milliseconds

Default value: 10000

Timeout in milliseconds for receiving Hello packet from replicas during handshake.

## hdfs_create_new_file_on_insert {#hdfs_create_new_file_on_insert}

Type: Bool

Default value: 0

Enables or disables creating a new file on each insert in HDFS engine tables. If enabled, on each insert a new HDFS file will be created with the name, similar to this pattern:

initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.

## hdfs_ignore_file_doesnt_exist {#hdfs_ignore_file_doesnt_exist}

Type: Bool

Default value: 0

Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.

## hdfs_replication {#hdfs_replication}

Type: UInt64

Default value: 0

The actual number of replications can be specified when the hdfs file is created.

## hdfs_skip_empty_files {#hdfs_skip_empty_files}

Type: Bool

Default value: 0

Enables or disables skipping empty files in [HDFS](../../engines/table-engines/integrations/hdfs.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

## hdfs_throw_on_zero_files_match {#hdfs_throw_on_zero_files_match}

Type: Bool

Default value: 0

Throw an error if matched zero files according to glob expansion rules.

Possible values:
- 1 — `SELECT` throws an exception.
- 0 — `SELECT` returns empty result.

## hdfs_truncate_on_insert {#hdfs_truncate_on_insert}

Type: Bool

Default value: 0

Enables or disables truncation before an insert in hdfs engine tables. If disabled, an exception will be thrown on an attempt to insert if a file in HDFS already exists.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.

## hedged_connection_timeout_ms {#hedged_connection_timeout_ms}

Type: Milliseconds

Default value: 50

Connection timeout for establishing connection with replica for Hedged requests

## hsts_max_age {#hsts_max_age}

Type: UInt64

Default value: 0

Expired time for HSTS. 0 means disable HSTS.

## http_connection_timeout {#http_connection_timeout}

Type: Seconds

Default value: 1

HTTP connection timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

## http_headers_progress_interval_ms {#http_headers_progress_interval_ms}

Type: UInt64

Default value: 100

Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.

## http_make_head_request {#http_make_head_request}

Type: Bool

Default value: 1

The `http_make_head_request` setting allows the execution of a `HEAD` request while reading data from HTTP to retrieve information about the file to be read, such as its size. Since it's enabled by default, it may be desirable to disable this setting in cases where the server does not support `HEAD` requests.

## http_max_field_name_size {#http_max_field_name_size}

Type: UInt64

Default value: 131072

Maximum length of field name in HTTP header

## http_max_field_value_size {#http_max_field_value_size}

Type: UInt64

Default value: 131072

Maximum length of field value in HTTP header

## http_max_fields {#http_max_fields}

Type: UInt64

Default value: 1000000

Maximum number of fields in HTTP header

## http_max_multipart_form_data_size {#http_max_multipart_form_data_size}

Type: UInt64

Default value: 1073741824

Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in a user profile. Note that content is parsed and external tables are created in memory before the start of query execution. And this is the only limit that has an effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).

## http_max_request_param_data_size {#http_max_request_param_data_size}

Type: UInt64

Default value: 10485760

Limit on size of request data used as a query parameter in predefined HTTP requests.

## http_max_tries {#http_max_tries}

Type: UInt64

Default value: 10

Max attempts to read via http.

## http_max_uri_size {#http_max_uri_size}

Type: UInt64

Default value: 1048576

Sets the maximum URI length of an HTTP request.

Possible values:

- Positive integer.

## http_native_compression_disable_checksumming_on_decompress {#http_native_compression_disable_checksumming_on_decompress}

Type: Bool

Default value: 0

Enables or disables checksum verification when decompressing the HTTP POST data from the client. Used only for ClickHouse native compression format (not used with `gzip` or `deflate`).

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## http_receive_timeout {#http_receive_timeout}

Type: Seconds

Default value: 30

HTTP receive timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

## http_response_buffer_size {#http_response_buffer_size}

Type: UInt64

Default value: 0

The number of bytes to buffer in the server memory before sending a HTTP response to the client or flushing to disk (when http_wait_end_of_query is enabled).

## http_retry_initial_backoff_ms {#http_retry_initial_backoff_ms}

Type: UInt64

Default value: 100

Min milliseconds for backoff, when retrying read via http

## http_retry_max_backoff_ms {#http_retry_max_backoff_ms}

Type: UInt64

Default value: 10000

Max milliseconds for backoff, when retrying read via http

## http_send_timeout {#http_send_timeout}

Type: Seconds

Default value: 30

HTTP send timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

:::note
It's applicable only to the default profile. A server reboot is required for the changes to take effect.
:::

## http_skip_not_found_url_for_globs {#http_skip_not_found_url_for_globs}

Type: Bool

Default value: 1

Skip URLs for globs with HTTP_NOT_FOUND error

## http_wait_end_of_query {#http_wait_end_of_query}

Type: Bool

Default value: 0

Enable HTTP response buffering on the server-side.

## http_write_exception_in_output_format {#http_write_exception_in_output_format}

Type: Bool

Default value: 1

Write exception in output format to produce valid output. Works with JSON and XML formats.

## http_zlib_compression_level {#http_zlib_compression_level}

Type: Int64

Default value: 3

Sets the level of data compression in the response to an HTTP request if [enable_http_compression = 1](#enable_http_compression).

Possible values: Numbers from 1 to 9.

## iceberg_engine_ignore_schema_evolution {#iceberg_engine_ignore_schema_evolution}

Type: Bool

Default value: 0

Allow to ignore schema evolution in Iceberg table engine and read all data using schema specified by the user on table creation or latest schema parsed from metadata on table creation.

:::note
Enabling this setting can lead to incorrect result as in case of evolved schema all data files will be read using the same schema.
:::

## idle_connection_timeout {#idle_connection_timeout}

Type: UInt64

Default value: 3600

Timeout to close idle TCP connections after specified number of seconds.

Possible values:

- Positive integer (0 - close immediately, after 0 seconds).

## ignore_cold_parts_seconds {#ignore_cold_parts_seconds}

Type: Int64

Default value: 0

Only available in ClickHouse Cloud. Exclude new data parts from SELECT queries until they're either pre-warmed (see cache_populated_by_fetch) or this many seconds old. Only for Replicated-/SharedMergeTree.

## ignore_data_skipping_indices {#ignore_data_skipping_indices}

Type: String

Default value: 

Ignores the skipping indexes specified if used by the query.

Consider the following example:

```sql
CREATE TABLE data
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1,
    INDEX y_idx y TYPE minmax GRANULARITY 1,
    INDEX xy_idx (x,y) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO data VALUES (1, 2, 3);

SELECT * FROM data;
SELECT * FROM data SETTINGS ignore_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error.
SELECT * FROM data SETTINGS ignore_data_skipping_indices='x_idx'; -- Ok.
SELECT * FROM data SETTINGS ignore_data_skipping_indices='na_idx'; -- Ok.

SELECT * FROM data WHERE x = 1 AND y = 1 SETTINGS ignore_data_skipping_indices='xy_idx',force_data_skipping_indices='xy_idx' ; -- query will produce INDEX_NOT_USED error, since xy_idx is explicitly ignored.
SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';
```

The query without ignoring any indexes:
```sql
EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2;

Expression ((Projection + Before ORDER BY))
  Filter (WHERE)
    ReadFromMergeTree (default.data)
    Indexes:
      PrimaryKey
        Condition: true
        Parts: 1/1
        Granules: 1/1
      Skip
        Name: x_idx
        Description: minmax GRANULARITY 1
        Parts: 0/1
        Granules: 0/1
      Skip
        Name: y_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
      Skip
        Name: xy_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
```

Ignoring the `xy_idx` index:
```sql
EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';

Expression ((Projection + Before ORDER BY))
  Filter (WHERE)
    ReadFromMergeTree (default.data)
    Indexes:
      PrimaryKey
        Condition: true
        Parts: 1/1
        Granules: 1/1
      Skip
        Name: x_idx
        Description: minmax GRANULARITY 1
        Parts: 0/1
        Granules: 0/1
      Skip
        Name: y_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
```

Works with tables in the MergeTree family.

## ignore_drop_queries_probability {#ignore_drop_queries_probability}

Type: Float

Default value: 0

If enabled, server will ignore all DROP table queries with specified probability (for Memory and JOIN engines it will replcase DROP to TRUNCATE). Used for testing purposes

## ignore_materialized_views_with_dropped_target_table {#ignore_materialized_views_with_dropped_target_table}

Type: Bool

Default value: 0

Ignore MVs with dropped target table during pushing to views

## ignore_on_cluster_for_replicated_access_entities_queries {#ignore_on_cluster_for_replicated_access_entities_queries}

Type: Bool

Default value: 0

Ignore ON CLUSTER clause for replicated access entities management queries.

## ignore_on_cluster_for_replicated_named_collections_queries {#ignore_on_cluster_for_replicated_named_collections_queries}

Type: Bool

Default value: 0

Ignore ON CLUSTER clause for replicated named collections management queries.

## ignore_on_cluster_for_replicated_udf_queries {#ignore_on_cluster_for_replicated_udf_queries}

Type: Bool

Default value: 0

Ignore ON CLUSTER clause for replicated UDF management queries.

## implicit_transaction {#implicit_transaction}

Type: Bool

Default value: 0

If enabled and not already inside a transaction, wraps the query inside a full transaction (begin + commit or rollback)

## input_format_parallel_parsing {#input_format_parallel_parsing}

Type: Bool

Default value: 1

Enables or disables order-preserving parallel parsing of data formats. Supported only for [TSV](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [CSV](../../interfaces/formats.md/#csv) and [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) formats.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

## insert_allow_materialized_columns {#insert_allow_materialized_columns}

Type: Bool

Default value: 0

If setting is enabled, Allow materialized columns in INSERT.

## insert_deduplicate {#insert_deduplicate}

Type: Bool

Default value: 1

Enables or disables block deduplication of `INSERT` (for Replicated\* tables).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

By default, blocks inserted into replicated tables by the `INSERT` statement are deduplicated (see [Data Replication](../../engines/table-engines/mergetree-family/replication.md)).
For the replicated tables by default the only 100 of the most recent blocks for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated-deduplication-window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated-deduplication-window-seconds)).
For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non-replicated-deduplication-window).

## insert_deduplication_token {#insert_deduplication_token}

Type: String

Default value: 

The setting allows a user to provide own deduplication semantic in MergeTree/ReplicatedMergeTree
For example, by providing a unique value for the setting in each INSERT statement,
user can avoid the same inserted data being deduplicated.

Possible values:

- Any string

`insert_deduplication_token` is used for deduplication _only_ when not empty.

For the replicated tables by default the only 100 of the most recent inserts for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated-deduplication-window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated-deduplication-window-seconds)).
For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non-replicated-deduplication-window).

:::note
`insert_deduplication_token` works on a partition level (the same as `insert_deduplication` checksum). Multiple partitions can have the same `insert_deduplication_token`.
:::

Example:

```sql
CREATE TABLE test_table
( A Int64 )
ENGINE = MergeTree
ORDER BY A
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (1);

-- the next insert won't be deduplicated because insert_deduplication_token is different
INSERT INTO test_table SETTINGS insert_deduplication_token = 'test1' VALUES (1);

-- the next insert will be deduplicated because insert_deduplication_token
-- is the same as one of the previous
INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (2);

SELECT * FROM test_table

┌─A─┐
│ 1 │
└───┘
┌─A─┐
│ 1 │
└───┘
```

## insert_keeper_fault_injection_probability {#insert_keeper_fault_injection_probability}

Type: Float

Default value: 0

Approximate probability of failure for a keeper request during insert. Valid value is in interval [0.0f, 1.0f]

## insert_keeper_fault_injection_seed {#insert_keeper_fault_injection_seed}

Type: UInt64

Default value: 0

0 - random seed, otherwise the setting value

## insert_keeper_max_retries {#insert_keeper_max_retries}

Type: UInt64

Default value: 20

The setting sets the maximum number of retries for ClickHouse Keeper (or ZooKeeper) requests during insert into replicated MergeTree. Only Keeper requests which failed due to network error, Keeper session timeout, or request timeout are considered for retries.

Possible values:

- Positive integer.
- 0 — Retries are disabled

Cloud default value: `20`.

Keeper request retries are done after some timeout. The timeout is controlled by the following settings: `insert_keeper_retry_initial_backoff_ms`, `insert_keeper_retry_max_backoff_ms`.
The first retry is done after `insert_keeper_retry_initial_backoff_ms` timeout. The consequent timeouts will be calculated as follows:
```
timeout = min(insert_keeper_retry_max_backoff_ms, latest_timeout * 2)
```

For example, if `insert_keeper_retry_initial_backoff_ms=100`, `insert_keeper_retry_max_backoff_ms=10000` and `insert_keeper_max_retries=8` then timeouts will be `100, 200, 400, 800, 1600, 3200, 6400, 10000`.

Apart from fault tolerance, the retries aim to provide a better user experience - they allow to avoid returning an error during INSERT execution if Keeper is restarted, for example, due to an upgrade.

## insert_keeper_retry_initial_backoff_ms {#insert_keeper_retry_initial_backoff_ms}

Type: UInt64

Default value: 100

Initial timeout(in milliseconds) to retry a failed Keeper request during INSERT query execution

Possible values:

- Positive integer.
- 0 — No timeout

## insert_keeper_retry_max_backoff_ms {#insert_keeper_retry_max_backoff_ms}

Type: UInt64

Default value: 10000

Maximum timeout (in milliseconds) to retry a failed Keeper request during INSERT query execution

Possible values:

- Positive integer.
- 0 — Maximum timeout is not limited

## insert_null_as_default {#insert_null_as_default}

Type: Bool

Default value: 1

Enables or disables the insertion of [default values](../../sql-reference/statements/create/table.md/#create-default-values) instead of [NULL](../../sql-reference/syntax.md/#null-literal) into columns with not [nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable) data type.
If column type is not nullable and this setting is disabled, then inserting `NULL` causes an exception. If column type is nullable, then `NULL` values are inserted as is, regardless of this setting.

This setting is applicable to [INSERT ... SELECT](../../sql-reference/statements/insert-into.md/#inserting-the-results-of-select) queries. Note that `SELECT` subqueries may be concatenated with `UNION ALL` clause.

Possible values:

- 0 — Inserting `NULL` into a not nullable column causes an exception.
- 1 — Default column value is inserted instead of `NULL`.

## insert_quorum {#insert_quorum}

Type: UInt64Auto

Default value: 0

:::note
This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information.
:::

Enables the quorum writes.

- If `insert_quorum < 2`, the quorum writes are disabled.
- If `insert_quorum >= 2`, the quorum writes are enabled.
- If `insert_quorum = 'auto'`, use majority number (`number_of_replicas / 2 + 1`) as quorum number.

Quorum writes

`INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written.

When `insert_quorum_parallel` is disabled, all replicas in the quorum are consistent, i.e. they contain data from all previous `INSERT` queries (the `INSERT` sequence is linearized). When reading data written using `insert_quorum` and `insert_quorum_parallel` is disabled, you can turn on sequential consistency for `SELECT` queries using [select_sequential_consistency](#select_sequential_consistency).

ClickHouse generates an exception:

- If the number of available replicas at the time of the query is less than the `insert_quorum`.
- When `insert_quorum_parallel` is disabled and an attempt to write data is made when the previous block has not yet been inserted in `insert_quorum` of replicas. This situation may occur if the user tries to perform another `INSERT` query to the same table before the previous one with `insert_quorum` is completed.

See also:

- [insert_quorum_timeout](#insert_quorum_timeout)
- [insert_quorum_parallel](#insert_quorum_parallel)
- [select_sequential_consistency](#select_sequential_consistency)

## insert_quorum_parallel {#insert_quorum_parallel}

Type: Bool

Default value: 1

:::note
This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information.
:::

Enables or disables parallelism for quorum `INSERT` queries. If enabled, additional `INSERT` queries can be sent while previous queries have not yet finished. If disabled, additional writes to the same table will be rejected.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_timeout](#insert_quorum_timeout)
- [select_sequential_consistency](#select_sequential_consistency)

## insert_quorum_timeout {#insert_quorum_timeout}

Type: Milliseconds

Default value: 600000

Write to a quorum timeout in milliseconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_parallel](#insert_quorum_parallel)
- [select_sequential_consistency](#select_sequential_consistency)

## insert_shard_id {#insert_shard_id}

Type: UInt64

Default value: 0

If not `0`, specifies the shard of [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table into which the data will be inserted synchronously.

If `insert_shard_id` value is incorrect, the server will throw an exception.

To get the number of shards on `requested_cluster`, you can check server config or use this query:

``` sql
SELECT uniq(shard_num) FROM system.clusters WHERE cluster = 'requested_cluster';
```

Possible values:

- 0 — Disabled.
- Any number from `1` to `shards_num` of corresponding [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table.

**Example**

Query:

```sql
CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;
CREATE TABLE x_dist AS x ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), x);
INSERT INTO x_dist SELECT * FROM numbers(5) SETTINGS insert_shard_id = 1;
SELECT * FROM x_dist ORDER BY number ASC;
```

Result:

``` text
┌─number─┐
│      0 │
│      0 │
│      1 │
│      1 │
│      2 │
│      2 │
│      3 │
│      3 │
│      4 │
│      4 │
└────────┘
```

## interactive_delay {#interactive_delay}

Type: UInt64

Default value: 100000

The interval in microseconds for checking whether request execution has been canceled and sending the progress.

## intersect_default_mode {#intersect_default_mode}

Type: SetOperationMode

Default value: ALL

Set default mode in INTERSECT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.

## join_algorithm {#join_algorithm}

Type: JoinAlgorithm

Default value: default

Specifies which [JOIN](../../sql-reference/statements/select/join.md) algorithm is used.

Several algorithms can be specified, and an available one would be chosen for a particular query based on kind/strictness and table engine.

Possible values:

- default

 This is the equivalent of `hash` or `direct`, if possible (same as `direct,hash`)

- grace_hash

 [Grace hash join](https://en.wikipedia.org/wiki/Hash_join#Grace_hash_join) is used.  Grace hash provides an algorithm option that provides performant complex joins while limiting memory use.

 The first phase of a grace join reads the right table and splits it into N buckets depending on the hash value of key columns (initially, N is `grace_hash_join_initial_buckets`). This is done in a way to ensure that each bucket can be processed independently. Rows from the first bucket are added to an in-memory hash table while the others are saved to disk. If the hash table grows beyond the memory limit (e.g., as set by [`max_bytes_in_join`](/docs/en/operations/settings/query-complexity.md/#max_bytes_in_join)), the number of buckets is increased and the assigned bucket for each row. Any rows which don’t belong to the current bucket are flushed and reassigned.

 Supports `INNER/LEFT/RIGHT/FULL ALL/ANY JOIN`.

- hash

 [Hash join algorithm](https://en.wikipedia.org/wiki/Hash_join) is used. The most generic implementation that supports all combinations of kind and strictness and multiple join keys that are combined with `OR` in the `JOIN ON` section.

- parallel_hash

 A variation of `hash` join that splits the data into buckets and builds several hashtables instead of one concurrently to speed up this process.

 When using the `hash` algorithm, the right part of `JOIN` is uploaded into RAM.

- partial_merge

 A variation of the [sort-merge algorithm](https://en.wikipedia.org/wiki/Sort-merge_join), where only the right table is fully sorted.

 The `RIGHT JOIN` and `FULL JOIN` are supported only with `ALL` strictness (`SEMI`, `ANTI`, `ANY`, and `ASOF` are not supported).

 When using the `partial_merge` algorithm, ClickHouse sorts the data and dumps it to the disk. The `partial_merge` algorithm in ClickHouse differs slightly from the classic realization. First, ClickHouse sorts the right table by joining keys in blocks and creates a min-max index for sorted blocks. Then it sorts parts of the left table by the `join key` and joins them over the right table. The min-max index is also used to skip unneeded right table blocks.

- direct

 This algorithm can be applied when the storage for the right table supports key-value requests.

 The `direct` algorithm performs a lookup in the right table using rows from the left table as keys. It's supported only by special storage such as [Dictionary](../../engines/table-engines/special/dictionary.md/#dictionary) or [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) and only the `LEFT` and `INNER` JOINs.

- auto

 When set to `auto`, `hash` join is tried first, and the algorithm is switched on the fly to another algorithm if the memory limit is violated.

- full_sorting_merge

 [Sort-merge algorithm](https://en.wikipedia.org/wiki/Sort-merge_join) with full sorting joined tables before joining.

- prefer_partial_merge

 ClickHouse always tries to use `partial_merge` join if possible, otherwise, it uses `hash`. *Deprecated*, same as `partial_merge,hash`.

## join_any_take_last_row {#join_any_take_last_row}

Type: Bool

Default value: 0

Changes the behaviour of join operations with `ANY` strictness.

:::note
This setting applies only for `JOIN` operations with [Join](../../engines/table-engines/special/join.md) engine tables.
:::

Possible values:

- 0 — If the right table has more than one matching row, only the first one found is joined.
- 1 — If the right table has more than one matching row, only the last one found is joined.

See also:

- [JOIN clause](../../sql-reference/statements/select/join.md/#select-join)
- [Join table engine](../../engines/table-engines/special/join.md)
- [join_default_strictness](#join_default_strictness)

## join_default_strictness {#join_default_strictness}

Type: JoinStrictness

Default value: ALL

Sets default strictness for [JOIN clauses](../../sql-reference/statements/select/join.md/#select-join).

Possible values:

- `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the normal `JOIN` behaviour from standard SQL.
- `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same.
- `ASOF` — For joining sequences with an uncertain match.
- `Empty string` — If `ALL` or `ANY` is not specified in the query, ClickHouse throws an exception.

## join_on_disk_max_files_to_merge {#join_on_disk_max_files_to_merge}

Type: UInt64

Default value: 64

Limits the number of files allowed for parallel sorting in MergeJoin operations when they are executed on disk.

The bigger the value of the setting, the more RAM is used and the less disk I/O is needed.

Possible values:

- Any positive integer, starting from 2.

## join_output_by_rowlist_perkey_rows_threshold {#join_output_by_rowlist_perkey_rows_threshold}

Type: UInt64

Default value: 5

The lower limit of per-key average rows in the right table to determine whether to output by row list in hash join.

## join_overflow_mode {#join_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## join_to_sort_maximum_table_rows {#join_to_sort_maximum_table_rows}

Type: UInt64

Default value: 10000

The maximum number of rows in the right table to determine whether to rerange the right table by key in left or inner join.

## join_to_sort_minimum_perkey_rows {#join_to_sort_minimum_perkey_rows}

Type: UInt64

Default value: 40

The lower limit of per-key average rows in the right table to determine whether to rerange the right table by key in left or inner join. This setting ensures that the optimization is not applied for sparse table keys

## join_use_nulls {#join_use_nulls}

Type: Bool

Default value: 0

Sets the type of [JOIN](../../sql-reference/statements/select/join.md) behaviour. When merging tables, empty cells may appear. ClickHouse fills them differently based on this setting.

Possible values:

- 0 — The empty cells are filled with the default value of the corresponding field type.
- 1 — `JOIN` behaves the same way as in standard SQL. The type of the corresponding field is converted to [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable), and empty cells are filled with [NULL](../../sql-reference/syntax.md).

## joined_subquery_requires_alias {#joined_subquery_requires_alias}

Type: Bool

Default value: 1

Force joined subqueries and table functions to have aliases for correct name qualification.

## kafka_disable_num_consumers_limit {#kafka_disable_num_consumers_limit}

Type: Bool

Default value: 0

Disable limit on kafka_num_consumers that depends on the number of available CPU cores.

## kafka_max_wait_ms {#kafka_max_wait_ms}

Type: Milliseconds

Default value: 5000

The wait time in milliseconds for reading messages from [Kafka](../../engines/table-engines/integrations/kafka.md/#kafka) before retry.

Possible values:

- Positive integer.
- 0 — Infinite timeout.

See also:

- [Apache Kafka](https://kafka.apache.org/)

## keeper_map_strict_mode {#keeper_map_strict_mode}

Type: Bool

Default value: 0

Enforce additional checks during operations on KeeperMap. E.g. throw an exception on an insert for already existing key

## keeper_max_retries {#keeper_max_retries}

Type: UInt64

Default value: 10

Max retries for general keeper operations

## keeper_retry_initial_backoff_ms {#keeper_retry_initial_backoff_ms}

Type: UInt64

Default value: 100

Initial backoff timeout for general keeper operations

## keeper_retry_max_backoff_ms {#keeper_retry_max_backoff_ms}

Type: UInt64

Default value: 5000

Max backoff timeout for general keeper operations

## legacy_column_name_of_tuple_literal {#legacy_column_name_of_tuple_literal}

Type: Bool

Default value: 0

List all names of element of large tuple literals in their column names instead of hash. This settings exists only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher.

## lightweight_deletes_sync {#lightweight_deletes_sync}

Type: UInt64

Default value: 2

The same as [`mutations_sync`](#mutations_sync), but controls only execution of lightweight deletes.

Possible values:

- 0 - Mutations execute asynchronously.
- 1 - The query waits for the lightweight deletes to complete on the current server.
- 2 - The query waits for the lightweight deletes to complete on all replicas (if they exist).

**See Also**

- [Synchronicity of ALTER Queries](../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [Mutations](../../sql-reference/statements/alter/index.md#mutations)

## limit {#limit}

Type: UInt64

Default value: 0

Sets the maximum number of rows to get from the query result. It adjusts the value set by the [LIMIT](../../sql-reference/statements/select/limit.md/#limit-clause) clause, so that the limit, specified in the query, cannot exceed the limit, set by this setting.

Possible values:

- 0 — The number of rows is not limited.
- Positive integer.

## live_view_heartbeat_interval {#live_view_heartbeat_interval}

Type: Seconds

Default value: 15

The heartbeat interval in seconds to indicate live query is alive.

## load_balancing {#load_balancing}

Type: LoadBalancing

Default value: random

Specifies the algorithm of replicas selection that is used for distributed query processing.

ClickHouse supports the following algorithms of choosing replicas:

- [Random](#load_balancing-random) (by default)
- [Nearest hostname](#load_balancing-nearest_hostname)
- [Hostname levenshtein distance](#load_balancing-hostname_levenshtein_distance)
- [In order](#load_balancing-in_order)
- [First or random](#load_balancing-first_or_random)
- [Round robin](#load_balancing-round_robin)

See also:

- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

### Random (by Default) {#load_balancing-random}

``` sql
load_balancing = random
```

The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to anyone of them.
Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data.

### Nearest Hostname {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server’s hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

For instance, example01-01-1 and example01-01-2 are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem primitive, but it does not require external data about network topology, and it does not compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

### Hostname levenshtein distance {#load_balancing-hostname_levenshtein_distance}

``` sql
load_balancing = hostname_levenshtein_distance
```

Just like `nearest_hostname`, but it compares hostname in a [levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) manner. For example:

``` text
example-clickhouse-0-0 ample-clickhouse-0-0
1

example-clickhouse-0-0 example-clickhouse-1-10
2

example-clickhouse-0-0 example-clickhouse-12-0
3
```

### In Order {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Replicas with the same number of errors are accessed in the same order as they are specified in the configuration.
This method is appropriate when you know exactly which replica is preferable.

### First or Random {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

This algorithm chooses the first replica in the set or a random replica if the first is unavailable. It’s effective in cross-replication topology setups, but useless in other configurations.

The `first_or_random` algorithm solves the problem of the `in_order` algorithm. With `in_order`, if one replica goes down, the next one gets a double load while the remaining replicas handle the usual amount of traffic. When using the `first_or_random` algorithm, the load is evenly distributed among replicas that are still available.

It's possible to explicitly define what the first replica is by using the setting `load_balancing_first_offset`. This gives more control to rebalance query workloads among replicas.

### Round Robin {#load_balancing-round_robin}

``` sql
load_balancing = round_robin
```

This algorithm uses a round-robin policy across replicas with the same number of errors (only the queries with `round_robin` policy is accounted).

## load_balancing_first_offset {#load_balancing_first_offset}

Type: UInt64

Default value: 0

Which replica to preferably send a query when FIRST_OR_RANDOM load balancing strategy is used.

## load_marks_asynchronously {#load_marks_asynchronously}

Type: Bool

Default value: 0

Load MergeTree marks asynchronously

## local_filesystem_read_method {#local_filesystem_read_method}

Type: String

Default value: pread_threadpool

Method of reading data from local filesystem, one of: read, pread, mmap, io_uring, pread_threadpool. The 'io_uring' method is experimental and does not work for Log, TinyLog, StripeLog, File, Set and Join, and other tables with append-able files in presence of concurrent reads and writes.

## local_filesystem_read_prefetch {#local_filesystem_read_prefetch}

Type: Bool

Default value: 0

Should use prefetching when reading data from local filesystem.

## lock_acquire_timeout {#lock_acquire_timeout}

Type: Seconds

Default value: 120

Defines how many seconds a locking request waits before failing.

Locking timeout is used to protect from deadlocks while executing read/write operations with tables. When the timeout expires and the locking request fails, the ClickHouse server throws an exception "Locking attempt timed out! Possible deadlock avoided. Client should retry." with error code `DEADLOCK_AVOIDED`.

Possible values:

- Positive integer (in seconds).
- 0 — No locking timeout.

## log_comment {#log_comment}

Type: String

Default value: 

Specifies the value for the `log_comment` field of the [system.query_log](../system-tables/query_log.md) table and comment text for the server log.

It can be used to improve the readability of server logs. Additionally, it helps to select queries related to the test from the `system.query_log` after running [clickhouse-test](../../development/tests.md).

Possible values:

- Any string no longer than [max_query_size](#max_query_size). If the max_query_size is exceeded, the server throws an exception.

**Example**

Query:

``` sql
SET log_comment = 'log_comment test', log_queries = 1;
SELECT 1;
SYSTEM FLUSH LOGS;
SELECT type, query FROM system.query_log WHERE log_comment = 'log_comment test' AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 2;
```

Result:

``` text
┌─type────────┬─query─────┐
│ QueryStart  │ SELECT 1; │
│ QueryFinish │ SELECT 1; │
└─────────────┴───────────┘
```

## log_formatted_queries {#log_formatted_queries}

Type: Bool

Default value: 0

Allows to log formatted queries to the [system.query_log](../../operations/system-tables/query_log.md) system table (populates `formatted_query` column in the [system.query_log](../../operations/system-tables/query_log.md)).

Possible values:

- 0 — Formatted queries are not logged in the system table.
- 1 — Formatted queries are logged in the system table.

## log_processors_profiles {#log_processors_profiles}

Type: Bool

Default value: 1

Write time that processor spent during execution/waiting for data to `system.processors_profile_log` table.

See also:

- [`system.processors_profile_log`](../../operations/system-tables/processors_profile_log.md)
- [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)

## log_profile_events {#log_profile_events}

Type: Bool

Default value: 1

Log query performance statistics into the query_log, query_thread_log and query_views_log.

## log_queries {#log_queries}

Type: Bool

Default value: 1

Setting up query logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../../operations/server-configuration-parameters/settings.md/#query-log) server configuration parameter.

Example:

``` text
log_queries=1
```

## log_queries_cut_to_length {#log_queries_cut_to_length}

Type: UInt64

Default value: 100000

If query length is greater than a specified threshold (in bytes), then cut query when writing to query log. Also limit the length of printed query in ordinary text log.

## log_queries_min_query_duration_ms {#log_queries_min_query_duration_ms}

Type: Milliseconds

Default value: 0

If enabled (non-zero), queries faster than the value of this setting will not be logged (you can think about this as a `long_query_time` for [MySQL Slow Query Log](https://dev.mysql.com/doc/refman/5.7/en/slow-query-log.html)), and this basically means that you will not find them in the following tables:

- `system.query_log`
- `system.query_thread_log`

Only the queries with the following type will get to the log:

- `QUERY_FINISH`
- `EXCEPTION_WHILE_PROCESSING`

- Type: milliseconds
- Default value: 0 (any query)

## log_queries_min_type {#log_queries_min_type}

Type: LogQueriesType

Default value: QUERY_START

`query_log` minimal type to log.

Possible values:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Can be used to limit which entities will go to `query_log`, say you are interested only in errors, then you can use `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log_queries_probability {#log_queries_probability}

Type: Float

Default value: 1

Allows a user to write to [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), and [query_views_log](../../operations/system-tables/query_views_log.md) system tables only a sample of queries selected randomly with the specified probability. It helps to reduce the load with a large volume of queries in a second.

Possible values:

- 0 — Queries are not logged in the system tables.
- Positive floating-point number in the range [0..1]. For example, if the setting value is `0.5`, about half of the queries are logged in the system tables.
- 1 — All queries are logged in the system tables.

## log_query_settings {#log_query_settings}

Type: Bool

Default value: 1

Log query settings into the query_log.

## log_query_threads {#log_query_threads}

Type: Bool

Default value: 0

Setting up query threads logging.

Query threads log into the [system.query_thread_log](../../operations/system-tables/query_thread_log.md) table. This setting has effect only when [log_queries](#log-queries) is true. Queries’ threads run by ClickHouse with this setup are logged according to the rules in the [query_thread_log](../../operations/server-configuration-parameters/settings.md/#query_thread_log) server configuration parameter.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

``` text
log_query_threads=1
```

## log_query_views {#log_query_views}

Type: Bool

Default value: 1

Setting up query views logging.

When a query run by ClickHouse with this setting enabled has associated views (materialized or live views), they are logged in the [query_views_log](../../operations/server-configuration-parameters/settings.md/#query_views_log) server configuration parameter.

Example:

``` text
log_query_views=1
```

## low_cardinality_allow_in_native_format {#low_cardinality_allow_in_native_format}

Type: Bool

Default value: 1

Allows or restricts using the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type with the [Native](../../interfaces/formats.md/#native) format.

If usage of `LowCardinality` is restricted, ClickHouse server converts `LowCardinality`-columns to ordinary ones for `SELECT` queries, and convert ordinary columns to `LowCardinality`-columns for `INSERT` queries.

This setting is required mainly for third-party clients which do not support `LowCardinality` data type.

Possible values:

- 1 — Usage of `LowCardinality` is not restricted.
- 0 — Usage of `LowCardinality` is restricted.

## low_cardinality_max_dictionary_size {#low_cardinality_max_dictionary_size}

Type: UInt64

Default value: 8192

Sets a maximum size in rows of a shared global dictionary for the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type that can be written to a storage file system. This setting prevents issues with RAM in case of unlimited dictionary growth. All the data that can’t be encoded due to maximum dictionary size limitation ClickHouse writes in an ordinary method.

Possible values:

- Any positive integer.

## low_cardinality_use_single_dictionary_for_part {#low_cardinality_use_single_dictionary_for_part}

Type: Bool

Default value: 0

Turns on or turns off using of single dictionary for the data part.

By default, the ClickHouse server monitors the size of dictionaries and if a dictionary overflows then the server starts to write the next one. To prohibit creating several dictionaries set `low_cardinality_use_single_dictionary_for_part = 1`.

Possible values:

- 1 — Creating several dictionaries for the data part is prohibited.
- 0 — Creating several dictionaries for the data part is not prohibited.

## materialize_skip_indexes_on_insert {#materialize_skip_indexes_on_insert}

Type: Bool

Default value: 1

If true skip indexes are calculated on inserts, otherwise skip indexes will be calculated only during merges

## materialize_statistics_on_insert {#materialize_statistics_on_insert}

Type: Bool

Default value: 1

If true statistics are calculated on inserts, otherwise statistics will be calculated only during merges

## materialize_ttl_after_modify {#materialize_ttl_after_modify}

Type: Bool

Default value: 1

Apply TTL for old data, after ALTER MODIFY TTL query

## materialized_views_ignore_errors {#materialized_views_ignore_errors}

Type: Bool

Default value: 0

Allows to ignore errors for MATERIALIZED VIEW, and deliver original block to the table regardless of MVs

## max_analyze_depth {#max_analyze_depth}

Type: UInt64

Default value: 5000

Maximum number of analyses performed by interpreter.

## max_ast_depth {#max_ast_depth}

Type: UInt64

Default value: 1000

Maximum depth of query syntax tree. Checked after parsing.

## max_ast_elements {#max_ast_elements}

Type: UInt64

Default value: 50000

Maximum size of query syntax tree in number of nodes. Checked after parsing.

## max_backup_bandwidth {#max_backup_bandwidth}

Type: UInt64

Default value: 0

The maximum read speed in bytes per second for particular backup on server. Zero means unlimited.

## max_block_size {#max_block_size}

Type: UInt64

Default value: 65409

In ClickHouse, data is processed by blocks, which are sets of column parts. The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block.

The `max_block_size` setting indicates the recommended maximum number of rows to include in a single block when loading data from tables. Blocks the size of `max_block_size` are not always loaded from the table: if ClickHouse determines that less data needs to be retrieved, a smaller block is processed.

The block size should not be too small to avoid noticeable costs when processing each block. It should also not be too large to ensure that queries with a LIMIT clause execute quickly after processing the first block. When setting `max_block_size`, the goal should be to avoid consuming too much memory when extracting a large number of columns in multiple threads and to preserve at least some cache locality.

## max_bytes_before_external_group_by {#max_bytes_before_external_group_by}

Type: UInt64

Default value: 0

If memory usage during GROUP BY operation is exceeding this threshold in bytes, activate the 'external aggregation' mode (spill data to disk). Recommended value is half of the available system memory.

## max_bytes_before_external_sort {#max_bytes_before_external_sort}

Type: UInt64

Default value: 0

If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the 'external sorting' mode (spill data to disk). Recommended value is half of the available system memory.

## max_bytes_before_remerge_sort {#max_bytes_before_remerge_sort}

Type: UInt64

Default value: 1000000000

In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.

## max_bytes_in_distinct {#max_bytes_in_distinct}

Type: UInt64

Default value: 0

Maximum total size of the state (in uncompressed bytes) in memory for the execution of DISTINCT.

## max_bytes_in_join {#max_bytes_in_join}

Type: UInt64

Default value: 0

Maximum size of the hash table for JOIN (in number of bytes in memory).

## max_bytes_in_set {#max_bytes_in_set}

Type: UInt64

Default value: 0

Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.

## max_bytes_to_read {#max_bytes_to_read}

Type: UInt64

Default value: 0

Limit on read bytes (after decompression) from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.

## max_bytes_to_read_leaf {#max_bytes_to_read_leaf}

Type: UInt64

Default value: 0

Limit on read bytes (after decompression) on the leaf nodes for distributed queries. Limit is applied for local reads only, excluding the final merge stage on the root node. Note, the setting is unstable with prefer_localhost_replica=1.

## max_bytes_to_sort {#max_bytes_to_sort}

Type: UInt64

Default value: 0

If more than the specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception

## max_bytes_to_transfer {#max_bytes_to_transfer}

Type: UInt64

Default value: 0

Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.

## max_columns_to_read {#max_columns_to_read}

Type: UInt64

Default value: 0

If a query requires reading more than specified number of columns, exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.

## max_compress_block_size {#max_compress_block_size}

Type: UInt64

Default value: 1048576

The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). Specifying a smaller block size generally leads to slightly reduced compression ratio, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced.

:::note
This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse.
:::

Don’t confuse blocks for compression (a chunk of memory consisting of bytes) with blocks for query processing (a set of rows from a table).

## max_concurrent_queries_for_all_users {#max_concurrent_queries_for_all_users}

Type: UInt64

Default value: 0

Throw exception if the value of this setting is less or equal than the current number of simultaneously processed queries.

Example: `max_concurrent_queries_for_all_users` can be set to 99 for all users and database administrator can set it to 100 for itself to run queries for investigation even when the server is overloaded.

Modifying the setting for one query or user does not affect other queries.

Possible values:

- Positive integer.
- 0 — No limit.

**Example**

``` xml
<max_concurrent_queries_for_all_users>99</max_concurrent_queries_for_all_users>
```

**See Also**

- [max_concurrent_queries](/docs/en/operations/server-configuration-parameters/settings.md/#max_concurrent_queries)

## max_concurrent_queries_for_user {#max_concurrent_queries_for_user}

Type: UInt64

Default value: 0

The maximum number of simultaneously processed queries per user.

Possible values:

- Positive integer.
- 0 — No limit.

**Example**

``` xml
<max_concurrent_queries_for_user>5</max_concurrent_queries_for_user>
```

## max_distributed_connections {#max_distributed_connections}

Type: UInt64

Default value: 1024

The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.

## max_distributed_depth {#max_distributed_depth}

Type: UInt64

Default value: 5

Limits the maximum depth of recursive queries for [Distributed](../../engines/table-engines/special/distributed.md) tables.

If the value is exceeded, the server throws an exception.

Possible values:

- Positive integer.
- 0 — Unlimited depth.

## max_download_buffer_size {#max_download_buffer_size}

Type: UInt64

Default value: 10485760

The maximal size of buffer for parallel downloading (e.g. for URL engine) per each thread.

## max_download_threads {#max_download_threads}

Type: MaxThreads

Default value: 4

The maximum number of threads to download data (e.g. for URL engine).

## max_estimated_execution_time {#max_estimated_execution_time}

Type: Seconds

Default value: 0

Maximum query estimate execution time in seconds.

## max_execution_speed {#max_execution_speed}

Type: UInt64

Default value: 0

Maximum number of execution rows per second.

## max_execution_speed_bytes {#max_execution_speed_bytes}

Type: UInt64

Default value: 0

Maximum number of execution bytes per second.

## max_execution_time {#max_execution_time}

Type: Seconds

Default value: 0

If query runtime exceeds the specified number of seconds, the behavior will be determined by the 'timeout_overflow_mode', which by default is - throw an exception. Note that the timeout is checked and the query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting.

## max_execution_time_leaf {#max_execution_time_leaf}

Type: Seconds

Default value: 0

Similar semantic to max_execution_time but only apply on leaf node for distributed queries, the time out behavior will be determined by 'timeout_overflow_mode_leaf' which by default is - throw an exception

## max_expanded_ast_elements {#max_expanded_ast_elements}

Type: UInt64

Default value: 500000

Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.

## max_fetch_partition_retries_count {#max_fetch_partition_retries_count}

Type: UInt64

Default value: 5

Amount of retries while fetching partition from another host.

## max_final_threads {#max_final_threads}

Type: MaxThreads

Default value: 'auto(16)'

Sets the maximum number of parallel threads for the `SELECT` query data read phase with the [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier.

Possible values:

- Positive integer.
- 0 or 1 — Disabled. `SELECT` queries are executed in a single thread.

## max_http_get_redirects {#max_http_get_redirects}

Type: UInt64

Default value: 0

Max number of HTTP GET redirects hops allowed. Ensures additional security measures are in place to prevent a malicious server from redirecting your requests to unexpected services.\n\nIt is the case when an external server redirects to another address, but that address appears to be internal to the company's infrastructure, and by sending an HTTP request to an internal server, you could request an internal API from the internal network, bypassing the auth, or even query other services, such as Redis or Memcached. When you don't have an internal infrastructure (including something running on your localhost), or you trust the server, it is safe to allow redirects. Although keep in mind, that if the URL uses HTTP instead of HTTPS, and you will have to trust not only the remote server but also your ISP and every network in the middle.

## max_hyperscan_regexp_length {#max_hyperscan_regexp_length}

Type: UInt64

Default value: 0

Defines the maximum length for each regular expression in the [hyperscan multi-match functions](../../sql-reference/functions/string-search-functions.md/#multimatchanyhaystack-pattern1-pattern2-patternn).

Possible values:

- Positive integer.
- 0 - The length is not limited.

**Example**

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 3;
```

Result:

```text
┌─multiMatchAny('abcd', ['ab', 'bcd', 'c', 'd'])─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 2;
```

Result:

```text
Exception: Regexp length too large.
```

**See Also**

- [max_hyperscan_regexp_total_length](#max-hyperscan-regexp-total-length)

## max_hyperscan_regexp_total_length {#max_hyperscan_regexp_total_length}

Type: UInt64

Default value: 0

Sets the maximum length total of all regular expressions in each [hyperscan multi-match function](../../sql-reference/functions/string-search-functions.md/#multimatchanyhaystack-pattern1-pattern2-patternn).

Possible values:

- Positive integer.
- 0 - The length is not limited.

**Example**

Query:

```sql
SELECT multiMatchAny('abcd', ['a','b','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Result:

```text
┌─multiMatchAny('abcd', ['a', 'b', 'c', 'd'])─┐
│                                           1 │
└─────────────────────────────────────────────┘
```

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bc','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Result:

```text
Exception: Total regexp lengths too large.
```

**See Also**

- [max_hyperscan_regexp_length](#max-hyperscan-regexp-length)

## max_insert_block_size {#max_insert_block_size}

Type: UInt64

Default value: 1048449

The size of blocks (in a count of rows) to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the ‘max_insert_block_size’ setting on the server does not affect the size of the inserted blocks.
The setting also does not have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

The default is slightly more than `max_block_size`. The reason for this is that certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allow sorting more data in RAM.

## max_insert_delayed_streams_for_parallel_write {#max_insert_delayed_streams_for_parallel_write}

Type: UInt64

Default value: 0

The maximum number of streams (columns) to delay final part flush. Default - auto (1000 in case of underlying storage supports parallel write, for example S3 and disabled otherwise)

## max_insert_threads {#max_insert_threads}

Type: UInt64

Default value: 0

The maximum number of threads to execute the `INSERT SELECT` query.

Possible values:

- 0 (or 1) — `INSERT SELECT` no parallel execution.
- Positive integer. Bigger than 1.

Cloud default value: from `2` to `4`, depending on the service size.

Parallel `INSERT SELECT` has effect only if the `SELECT` part is executed in parallel, see [max_threads](#max_threads) setting.
Higher values will lead to higher memory usage.

## max_joined_block_size_rows {#max_joined_block_size_rows}

Type: UInt64

Default value: 65409

Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.

## max_limit_for_ann_queries {#max_limit_for_ann_queries}

Type: UInt64

Default value: 1000000

SELECT queries with LIMIT bigger than this setting cannot use ANN indexes. Helps to prevent memory overflows in ANN search indexes.

## max_live_view_insert_blocks_before_refresh {#max_live_view_insert_blocks_before_refresh}

Type: UInt64

Default value: 64

Limit maximum number of inserted blocks after which mergeable blocks are dropped and query is re-executed.

## max_local_read_bandwidth {#max_local_read_bandwidth}

Type: UInt64

Default value: 0

The maximum speed of local reads in bytes per second.

## max_local_write_bandwidth {#max_local_write_bandwidth}

Type: UInt64

Default value: 0

The maximum speed of local writes in bytes per second.

## max_memory_usage {#max_memory_usage}

Type: UInt64

Default value: 0

Maximum memory usage for processing of single query. Zero means unlimited.

## max_memory_usage_for_user {#max_memory_usage_for_user}

Type: UInt64

Default value: 0

Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.

## max_network_bandwidth {#max_network_bandwidth}

Type: UInt64

Default value: 0

Limits the speed of the data exchange over the network in bytes per second. This setting applies to every query.

Possible values:

- Positive integer.
- 0 — Bandwidth control is disabled.

## max_network_bandwidth_for_all_users {#max_network_bandwidth_for_all_users}

Type: UInt64

Default value: 0

Limits the speed that data is exchanged at over the network in bytes per second. This setting applies to all concurrently running queries on the server.

Possible values:

- Positive integer.
- 0 — Control of the data speed is disabled.

## max_network_bandwidth_for_user {#max_network_bandwidth_for_user}

Type: UInt64

Default value: 0

Limits the speed of the data exchange over the network in bytes per second. This setting applies to all concurrently running queries performed by a single user.

Possible values:

- Positive integer.
- 0 — Control of the data speed is disabled.

## max_network_bytes {#max_network_bytes}

Type: UInt64

Default value: 0

Limits the data volume (in bytes) that is received or transmitted over the network when executing a query. This setting applies to every individual query.

Possible values:

- Positive integer.
- 0 — Data volume control is disabled.

## max_number_of_partitions_for_independent_aggregation {#max_number_of_partitions_for_independent_aggregation}

Type: UInt64

Default value: 128

Maximal number of partitions in table to apply optimization

## max_parallel_replicas {#max_parallel_replicas}

Type: NonZeroUInt64

Default value: 1

The maximum number of replicas for each shard when executing a query.

Possible values:

- Positive integer.

**Additional Info**

This options will produce different results depending on the settings used.

:::note
This setting will produce incorrect results when joins or subqueries are involved, and all tables don't meet certain requirements. See [Distributed Subqueries and max_parallel_replicas](../../sql-reference/operators/in.md/#max_parallel_replica-subqueries) for more details.
:::

### Parallel processing using `SAMPLE` key

A query may be processed faster if it is executed on several servers in parallel. But the query performance may degrade in the following cases:

- The position of the sampling key in the partitioning key does not allow efficient range scans.
- Adding a sampling key to the table makes filtering by other columns less efficient.
- The sampling key is an expression that is expensive to calculate.
- The cluster latency distribution has a long tail, so that querying more servers increases the query overall latency.

### Parallel processing using [parallel_replicas_custom_key](#parallel_replicas_custom_key)

This setting is useful for any replicated table.

## max_parser_backtracks {#max_parser_backtracks}

Type: UInt64

Default value: 1000000

Maximum parser backtracking (how many times it tries different alternatives in the recursive descend parsing process).

## max_parser_depth {#max_parser_depth}

Type: UInt64

Default value: 1000

Limits maximum recursion depth in the recursive descent parser. Allows controlling the stack size.

Possible values:

- Positive integer.
- 0 — Recursion depth is unlimited.

## max_parsing_threads {#max_parsing_threads}

Type: MaxThreads

Default value: 'auto(16)'

The maximum number of threads to parse data in input formats that support parallel parsing. By default, it is determined automatically

## max_partition_size_to_drop {#max_partition_size_to_drop}

Type: UInt64

Default value: 50000000000

Restriction on dropping partitions in query time. The value 0 means that you can drop partitions without any restrictions.

Cloud default value: 1 TB.

:::note
This query setting overwrites its server setting equivalent, see [max_partition_size_to_drop](/docs/en/operations/server-configuration-parameters/settings.md/#max-partition-size-to-drop)
:::

## max_partitions_per_insert_block {#max_partitions_per_insert_block}

Type: UInt64

Default value: 100

Limit maximum number of partitions in the single INSERTed block. Zero means unlimited. Throw an exception if the block contains too many partitions. This setting is a safety threshold because using a large number of partitions is a common misconception.

## max_partitions_to_read {#max_partitions_to_read}

Type: Int64

Default value: -1

Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited.

## max_query_size {#max_query_size}

Type: UInt64

Default value: 262144

The maximum number of bytes of a query string parsed by the SQL parser.
Data in the VALUES clause of INSERT queries is processed by a separate stream parser (that consumes O(1) RAM) and not affected by this restriction.

:::note
`max_query_size` cannot be set within an SQL query (e.g., `SELECT now() SETTINGS max_query_size=10000`) because ClickHouse needs to allocate a buffer to parse the query, and this buffer size is determined by the `max_query_size` setting, which must be configured before the query is executed.
:::

## max_read_buffer_size {#max_read_buffer_size}

Type: UInt64

Default value: 1048576

The maximum size of the buffer to read from the filesystem.

## max_read_buffer_size_local_fs {#max_read_buffer_size_local_fs}

Type: UInt64

Default value: 131072

The maximum size of the buffer to read from local filesystem. If set to 0 then max_read_buffer_size will be used.

## max_read_buffer_size_remote_fs {#max_read_buffer_size_remote_fs}

Type: UInt64

Default value: 0

The maximum size of the buffer to read from remote filesystem. If set to 0 then max_read_buffer_size will be used.

## max_recursive_cte_evaluation_depth {#max_recursive_cte_evaluation_depth}

Type: UInt64

Default value: 1000

Maximum limit on recursive CTE evaluation depth

## max_remote_read_network_bandwidth {#max_remote_read_network_bandwidth}

Type: UInt64

Default value: 0

The maximum speed of data exchange over the network in bytes per second for read.

## max_remote_write_network_bandwidth {#max_remote_write_network_bandwidth}

Type: UInt64

Default value: 0

The maximum speed of data exchange over the network in bytes per second for write.

## max_replica_delay_for_distributed_queries {#max_replica_delay_for_distributed_queries}

Type: UInt64

Default value: 300

Disables lagging replicas for distributed queries. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

Sets the time in seconds. If a replica's lag is greater than or equal to the set value, this replica is not used.

Possible values:

- Positive integer.
- 0 — Replica lags are not checked.

To prevent the use of any replica with a non-zero lag, set this parameter to 1.

Used when performing `SELECT` from a distributed table that points to replicated tables.

## max_result_bytes {#max_result_bytes}

Type: UInt64

Default value: 0

Limit on result size in bytes (uncompressed).  The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold. Caveats: the result size in memory is taken into account for this threshold. Even if the result size is small, it can reference larger data structures in memory, representing dictionaries of LowCardinality columns, and Arenas of AggregateFunction columns, so the threshold can be exceeded despite the small result size. The setting is fairly low level and should be used with caution.

## max_result_rows {#max_result_rows}

Type: UInt64

Default value: 0

Limit on result size in rows. The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold.

## max_rows_in_distinct {#max_rows_in_distinct}

Type: UInt64

Default value: 0

Maximum number of elements during execution of DISTINCT.

## max_rows_in_join {#max_rows_in_join}

Type: UInt64

Default value: 0

Maximum size of the hash table for JOIN (in number of rows).

## max_rows_in_set {#max_rows_in_set}

Type: UInt64

Default value: 0

Maximum size of the set (in number of elements) resulting from the execution of the IN section.

## max_rows_in_set_to_optimize_join {#max_rows_in_set_to_optimize_join}

Type: UInt64

Default value: 0

Maximal size of the set to filter joined tables by each other's row sets before joining.

Possible values:

- 0 — Disable.
- Any positive integer.

## max_rows_to_group_by {#max_rows_to_group_by}

Type: UInt64

Default value: 0

If aggregation during GROUP BY is generating more than the specified number of rows (unique GROUP BY keys), the behavior will be determined by the 'group_by_overflow_mode' which by default is - throw an exception, but can be also switched to an approximate GROUP BY mode.

## max_rows_to_read {#max_rows_to_read}

Type: UInt64

Default value: 0

Limit on read rows from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.

## max_rows_to_read_leaf {#max_rows_to_read_leaf}

Type: UInt64

Default value: 0

Limit on read rows on the leaf nodes for distributed queries. Limit is applied for local reads only, excluding the final merge stage on the root node. Note, the setting is unstable with prefer_localhost_replica=1.

## max_rows_to_sort {#max_rows_to_sort}

Type: UInt64

Default value: 0

If more than the specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception

## max_rows_to_transfer {#max_rows_to_transfer}

Type: UInt64

Default value: 0

Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.

## max_sessions_for_user {#max_sessions_for_user}

Type: UInt64

Default value: 0

Maximum number of simultaneous sessions for a user.

## max_size_to_preallocate_for_aggregation {#max_size_to_preallocate_for_aggregation}

Type: UInt64

Default value: 100000000

For how many elements it is allowed to preallocate space in all hash tables in total before aggregation

## max_size_to_preallocate_for_joins {#max_size_to_preallocate_for_joins}

Type: UInt64

Default value: 100000000

For how many elements it is allowed to preallocate space in all hash tables in total before join

## max_streams_for_merge_tree_reading {#max_streams_for_merge_tree_reading}

Type: UInt64

Default value: 0

If is not zero, limit the number of reading streams for MergeTree table.

## max_streams_multiplier_for_merge_tables {#max_streams_multiplier_for_merge_tables}

Type: Float

Default value: 5

Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and is especially helpful when merged tables differ in size.

## max_streams_to_max_threads_ratio {#max_streams_to_max_threads_ratio}

Type: Float

Default value: 1

Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.

## max_subquery_depth {#max_subquery_depth}

Type: UInt64

Default value: 100

If a query has more than the specified number of nested subqueries, throw an exception. This allows you to have a sanity check to protect the users of your cluster from going insane with their queries.

## max_table_size_to_drop {#max_table_size_to_drop}

Type: UInt64

Default value: 50000000000

Restriction on deleting tables in query time. The value 0 means that you can delete all tables without any restrictions.

Cloud default value: 1 TB.

:::note
This query setting overwrites its server setting equivalent, see [max_table_size_to_drop](/docs/en/operations/server-configuration-parameters/settings.md/#max-table-size-to-drop)
:::

## max_temporary_columns {#max_temporary_columns}

Type: UInt64

Default value: 0

If a query generates more than the specified number of temporary columns in memory as a result of intermediate calculation, the exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.

## max_temporary_data_on_disk_size_for_query {#max_temporary_data_on_disk_size_for_query}

Type: UInt64

Default value: 0

The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries. Zero means unlimited.

## max_temporary_data_on_disk_size_for_user {#max_temporary_data_on_disk_size_for_user}

Type: UInt64

Default value: 0

The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries. Zero means unlimited.

## max_temporary_non_const_columns {#max_temporary_non_const_columns}

Type: UInt64

Default value: 0

Similar to the 'max_temporary_columns' setting but applies only to non-constant columns. This makes sense because constant columns are cheap and it is reasonable to allow more of them.

## max_threads {#max_threads}

Type: MaxThreads

Default value: 'auto(16)'

The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max_distributed_connections’ parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, when reading from a table, if it is possible to evaluate expressions with functions, filter with WHERE and pre-aggregate for GROUP BY in parallel using at least ‘max_threads’ number of threads, then ‘max_threads’ are used.

For queries that are completed quickly because of a LIMIT, you can set a lower ‘max_threads’. For example, if the necessary number of entries are located in every block and max_threads = 8, then 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.

## max_threads_for_indexes {#max_threads_for_indexes}

Type: UInt64

Default value: 0

The maximum number of threads process indices.

## max_untracked_memory {#max_untracked_memory}

Type: UInt64

Default value: 4194304

Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when an amount (in absolute value) becomes larger than the specified value. If the value is higher than 'memory_profiler_step' it will be effectively lowered to 'memory_profiler_step'.

## memory_overcommit_ratio_denominator {#memory_overcommit_ratio_denominator}

Type: UInt64

Default value: 1073741824

It represents the soft memory limit when the hard limit is reached on the global level.
This value is used to compute the overcommit ratio for the query.
Zero means skip the query.
Read more about [memory overcommit](memory-overcommit.md).

## memory_overcommit_ratio_denominator_for_user {#memory_overcommit_ratio_denominator_for_user}

Type: UInt64

Default value: 1073741824

It represents the soft memory limit when the hard limit is reached on the user level.
This value is used to compute the overcommit ratio for the query.
Zero means skip the query.
Read more about [memory overcommit](memory-overcommit.md).

## memory_profiler_sample_max_allocation_size {#memory_profiler_sample_max_allocation_size}

Type: UInt64

Default value: 0

Collect random allocations of size less or equal than the specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold work as expected.

## memory_profiler_sample_min_allocation_size {#memory_profiler_sample_min_allocation_size}

Type: UInt64

Default value: 0

Collect random allocations of size greater or equal than the specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold work as expected.

## memory_profiler_sample_probability {#memory_profiler_sample_probability}

Type: Float

Default value: 0

Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability is for every alloc/free regardless of the size of the allocation (can be changed with `memory_profiler_sample_min_allocation_size` and `memory_profiler_sample_max_allocation_size`). Note that sampling happens only when the amount of untracked memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine-grained sampling.

## memory_profiler_step {#memory_profiler_step}

Type: UInt64

Default value: 4194304

Sets the step of memory profiler. Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stacktrace and will write it into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- A positive integer number of bytes.

- 0 for turning off the memory profiler.

## memory_tracker_fault_probability {#memory_tracker_fault_probability}

Type: Float

Default value: 0

For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.

## memory_usage_overcommit_max_wait_microseconds {#memory_usage_overcommit_max_wait_microseconds}

Type: UInt64

Default value: 5000000

Maximum time thread will wait for memory to be freed in the case of memory overcommit on a user level.
If the timeout is reached and memory is not freed, an exception is thrown.
Read more about [memory overcommit](memory-overcommit.md).

## merge_tree_coarse_index_granularity {#merge_tree_coarse_index_granularity}

Type: UInt64

Default value: 8

When searching for data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range into `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively.

Possible values:

- Any positive even integer.

## merge_tree_compact_parts_min_granules_to_multibuffer_read {#merge_tree_compact_parts_min_granules_to_multibuffer_read}

Type: UInt64

Default value: 16

Only available in ClickHouse Cloud. Number of granules in stripe of compact part of MergeTree tables to use multibuffer reader, which supports parallel reading and prefetch. In case of reading from remote fs using of multibuffer reader increases number of read request.

## merge_tree_determine_task_size_by_prewhere_columns {#merge_tree_determine_task_size_by_prewhere_columns}

Type: Bool

Default value: 1

Whether to use only prewhere columns size to determine reading task size.

## merge_tree_max_bytes_to_use_cache {#merge_tree_max_bytes_to_use_cache}

Type: UInt64

Default value: 2013265920

If ClickHouse should read more than `merge_tree_max_bytes_to_use_cache` bytes in one query, it does not use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

- Any positive integer.

## merge_tree_max_rows_to_use_cache {#merge_tree_max_rows_to_use_cache}

Type: UInt64

Default value: 1048576

If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it does not use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

- Any positive integer.

## merge_tree_min_bytes_for_concurrent_read {#merge_tree_min_bytes_for_concurrent_read}

Type: UInt64

Default value: 251658240

If the number of bytes to read from one file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine table exceeds `merge_tree_min_bytes_for_concurrent_read`, then ClickHouse tries to concurrently read from this file in several threads.

Possible value:

- Positive integer.

## merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem {#merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem}

Type: UInt64

Default value: 251658240

The minimum number of bytes to read from one file before [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem.

Possible values:

- Positive integer.

## merge_tree_min_bytes_for_seek {#merge_tree_min_bytes_for_seek}

Type: UInt64

Default value: 0

If the distance between two data blocks to be read in one file is less than `merge_tree_min_bytes_for_seek` bytes, then ClickHouse sequentially reads a range of file that contains both blocks, thus avoiding extra seek.

Possible values:

- Any positive integer.

## merge_tree_min_bytes_per_task_for_remote_reading {#merge_tree_min_bytes_per_task_for_remote_reading}

Type: UInt64

Default value: 2097152

Min bytes to read per task.

## merge_tree_min_rows_for_concurrent_read {#merge_tree_min_rows_for_concurrent_read}

Type: UInt64

Default value: 163840

If the number of rows to be read from a file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file on several threads.

Possible values:

- Positive integer.

## merge_tree_min_rows_for_concurrent_read_for_remote_filesystem {#merge_tree_min_rows_for_concurrent_read_for_remote_filesystem}

Type: UInt64

Default value: 163840

The minimum number of lines to read from one file before the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem.

Possible values:

- Positive integer.

## merge_tree_min_rows_for_seek {#merge_tree_min_rows_for_seek}

Type: UInt64

Default value: 0

If the distance between two data blocks to be read in one file is less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file but reads the data sequentially.

Possible values:

- Any positive integer.

## merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability {#merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability}

Type: Float

Default value: 0

For testing of `PartsSplitter` - split read ranges into intersecting and non intersecting every time you read from MergeTree with the specified probability.

## merge_tree_use_const_size_tasks_for_remote_reading {#merge_tree_use_const_size_tasks_for_remote_reading}

Type: Bool

Default value: 1

Whether to use constant size tasks for reading from a remote table.

## metrics_perf_events_enabled {#metrics_perf_events_enabled}

Type: Bool

Default value: 0

If enabled, some of the perf events will be measured throughout queries' execution.

## metrics_perf_events_list {#metrics_perf_events_list}

Type: String

Default value: 

Comma separated list of perf metrics that will be measured throughout queries' execution. Empty means all events. See PerfEventInfo in sources for the available events.

## min_bytes_to_use_direct_io {#min_bytes_to_use_direct_io}

Type: UInt64

Default value: 0

The minimum data volume required for using direct I/O access to the storage disk.

ClickHouse uses this setting when reading data from tables. If the total storage volume of all the data to be read exceeds `min_bytes_to_use_direct_io` bytes, then ClickHouse reads the data from the storage disk with the `O_DIRECT` option.

Possible values:

- 0 — Direct I/O is disabled.
- Positive integer.

## min_bytes_to_use_mmap_io {#min_bytes_to_use_mmap_io}

Type: UInt64

Default value: 0

This is an experimental setting. Sets the minimum amount of memory for reading large files without copying data from the kernel to userspace. Recommended threshold is about 64 MB, because [mmap/munmap](https://en.wikipedia.org/wiki/Mmap) is slow. It makes sense only for large files and helps only if data reside in the page cache.

Possible values:

- Positive integer.
- 0 — Big files read with only copying data from kernel to userspace.

## min_chunk_bytes_for_parallel_parsing {#min_chunk_bytes_for_parallel_parsing}

Type: UInt64

Default value: 10485760

- Type: unsigned int
- Default value: 1 MiB

The minimum chunk size in bytes, which each thread will parse in parallel.

## min_compress_block_size {#min_compress_block_size}

Type: UInt64

Default value: 65536

For [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least `min_compress_block_size`. By default, 65,536.

The actual size of the block, if the uncompressed data is less than `max_compress_block_size`, is no less than this value and no less than the volume of data for one mark.

Let’s look at an example. Assume that `index_granularity` was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min_compress_block_size = 65,536, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won’t be decompressed.

:::note
This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse.
:::

## min_count_to_compile_aggregate_expression {#min_count_to_compile_aggregate_expression}

Type: UInt64

Default value: 3

The minimum number of identical aggregate expressions to start JIT-compilation. Works only if the [compile_aggregate_expressions](#compile_aggregate_expressions) setting is enabled.

Possible values:

- Positive integer.
- 0 — Identical aggregate expressions are always JIT-compiled.

## min_count_to_compile_expression {#min_count_to_compile_expression}

Type: UInt64

Default value: 3

Minimum count of executing same expression before it is get compiled.

## min_count_to_compile_sort_description {#min_count_to_compile_sort_description}

Type: UInt64

Default value: 3

The number of identical sort descriptions before they are JIT-compiled

## min_execution_speed {#min_execution_speed}

Type: UInt64

Default value: 0

Minimum number of execution rows per second.

## min_execution_speed_bytes {#min_execution_speed_bytes}

Type: UInt64

Default value: 0

Minimum number of execution bytes per second.

## min_external_table_block_size_bytes {#min_external_table_block_size_bytes}

Type: UInt64

Default value: 268402944

Squash blocks passed to the external table to a specified size in bytes, if blocks are not big enough.

## min_external_table_block_size_rows {#min_external_table_block_size_rows}

Type: UInt64

Default value: 1048449

Squash blocks passed to external table to specified size in rows, if blocks are not big enough.

## min_free_disk_bytes_to_perform_insert {#min_free_disk_bytes_to_perform_insert}

Type: UInt64

Default value: 0

Minimum free disk space bytes to perform an insert.

## min_free_disk_ratio_to_perform_insert {#min_free_disk_ratio_to_perform_insert}

Type: Float

Default value: 0

Minimum free disk space ratio to perform an insert.

## min_free_disk_space_for_temporary_data {#min_free_disk_space_for_temporary_data}

Type: UInt64

Default value: 0

The minimum disk space to keep while writing temporary data used in external sorting and aggregation.

## min_hit_rate_to_use_consecutive_keys_optimization {#min_hit_rate_to_use_consecutive_keys_optimization}

Type: Float

Default value: 0.5

Minimal hit rate of a cache which is used for consecutive keys optimization in aggregation to keep it enabled

## min_insert_block_size_bytes {#min_insert_block_size_bytes}

Type: UInt64

Default value: 268402944

Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.

## min_insert_block_size_bytes_for_materialized_views {#min_insert_block_size_bytes_for_materialized_views}

Type: UInt64

Default value: 0

Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

**See also**

- [min_insert_block_size_bytes](#min-insert-block-size-bytes)

## min_insert_block_size_rows {#min_insert_block_size_rows}

Type: UInt64

Default value: 1048449

Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.

## min_insert_block_size_rows_for_materialized_views {#min_insert_block_size_rows_for_materialized_views}

Type: UInt64

Default value: 0

Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

**See Also**

- [min_insert_block_size_rows](#min-insert-block-size-rows)

## mongodb_throw_on_unsupported_query {#mongodb_throw_on_unsupported_query}

Type: Bool

Default value: 1

If enabled, MongoDB tables will return an error when a MongoDB query cannot be built. Otherwise, ClickHouse reads the full table and processes it locally. This option does not apply to the legacy implementation or when 'allow_experimental_analyzer=0'.

## move_all_conditions_to_prewhere {#move_all_conditions_to_prewhere}

Type: Bool

Default value: 1

Move all viable conditions from WHERE to PREWHERE

## move_primary_key_columns_to_end_of_prewhere {#move_primary_key_columns_to_end_of_prewhere}

Type: Bool

Default value: 1

Move PREWHERE conditions containing primary key columns to the end of AND chain. It is likely that these conditions are taken into account during primary key analysis and thus will not contribute a lot to PREWHERE filtering.

## multiple_joins_try_to_keep_original_names {#multiple_joins_try_to_keep_original_names}

Type: Bool

Default value: 0

Do not add aliases to top level expression list on multiple joins rewrite

## mutations_execute_nondeterministic_on_initiator {#mutations_execute_nondeterministic_on_initiator}

Type: Bool

Default value: 0

If true constant nondeterministic functions (e.g. function `now()`) are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. It helps to keep data in sync on replicas while executing mutations with constant nondeterministic functions. Default value: `false`.

## mutations_execute_subqueries_on_initiator {#mutations_execute_subqueries_on_initiator}

Type: Bool

Default value: 0

If true scalar subqueries are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. Default value: `false`.

## mutations_max_literal_size_to_replace {#mutations_max_literal_size_to_replace}

Type: UInt64

Default value: 16384

The maximum size of serialized literal in bytes to replace in `UPDATE` and `DELETE` queries. Takes effect only if at least one the two settings above is enabled. Default value: 16384 (16 KiB).

## mutations_sync {#mutations_sync}

Type: UInt64

Default value: 0

Allows to execute `ALTER TABLE ... UPDATE|DELETE|MATERIALIZE INDEX|MATERIALIZE PROJECTION|MATERIALIZE COLUMN` queries ([mutations](../../sql-reference/statements/alter/index.md#mutations)) synchronously.

Possible values:

- 0 - Mutations execute asynchronously.
- 1 - The query waits for all mutations to complete on the current server.
- 2 - The query waits for all mutations to complete on all replicas (if they exist).

## mysql_datatypes_support_level {#mysql_datatypes_support_level}

Type: MySQLDataTypesSupport

Default value: 

Defines how MySQL types are converted to corresponding ClickHouse types. A comma separated list in any combination of `decimal`, `datetime64`, `date2Date32` or `date2String`.
- `decimal`: convert `NUMERIC` and `DECIMAL` types to `Decimal` when precision allows it.
- `datetime64`: convert `DATETIME` and `TIMESTAMP` types to `DateTime64` instead of `DateTime` when precision is not `0`.
- `date2Date32`: convert `DATE` to `Date32` instead of `Date`. Takes precedence over `date2String`.
- `date2String`: convert `DATE` to `String` instead of `Date`. Overridden by `datetime64`.

## mysql_map_fixed_string_to_text_in_show_columns {#mysql_map_fixed_string_to_text_in_show_columns}

Type: Bool

Default value: 1

When enabled, [FixedString](../../sql-reference/data-types/fixedstring.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md#show_columns).

Has an effect only when the connection is made through the MySQL wire protocol.

- 0 - Use `BLOB`.
- 1 - Use `TEXT`.

## mysql_map_string_to_text_in_show_columns {#mysql_map_string_to_text_in_show_columns}

Type: Bool

Default value: 1

When enabled, [String](../../sql-reference/data-types/string.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md#show_columns).

Has an effect only when the connection is made through the MySQL wire protocol.

- 0 - Use `BLOB`.
- 1 - Use `TEXT`.

## mysql_max_rows_to_insert {#mysql_max_rows_to_insert}

Type: UInt64

Default value: 65536

The maximum number of rows in MySQL batch insertion of the MySQL storage engine

## network_compression_method {#network_compression_method}

Type: String

Default value: LZ4

Sets the method of data compression that is used for communication between servers and between server and [clickhouse-client](../../interfaces/cli.md).

Possible values:

- `LZ4` — sets LZ4 compression method.
- `ZSTD` — sets ZSTD compression method.

**See Also**

- [network_zstd_compression_level](#network_zstd_compression_level)

## network_zstd_compression_level {#network_zstd_compression_level}

Type: Int64

Default value: 1

Adjusts the level of ZSTD compression. Used only when [network_compression_method](#network_compression_method) is set to `ZSTD`.

Possible values:

- Positive integer from 1 to 15.

## normalize_function_names {#normalize_function_names}

Type: Bool

Default value: 1

Normalize function names to their canonical names

## number_of_mutations_to_delay {#number_of_mutations_to_delay}

Type: UInt64

Default value: 0

If the mutated table contains at least that many unfinished mutations, artificially slow down mutations of table. 0 - disabled

## number_of_mutations_to_throw {#number_of_mutations_to_throw}

Type: UInt64

Default value: 0

If the mutated table contains at least that many unfinished mutations, throw 'Too many mutations ...' exception. 0 - disabled

## odbc_bridge_connection_pool_size {#odbc_bridge_connection_pool_size}

Type: UInt64

Default value: 16

Connection pool size for each connection settings string in ODBC bridge.

## odbc_bridge_use_connection_pooling {#odbc_bridge_use_connection_pooling}

Type: Bool

Default value: 1

Use connection pooling in ODBC bridge. If set to false, a new connection is created every time.

## offset {#offset}

Type: UInt64

Default value: 0

Sets the number of rows to skip before starting to return rows from the query. It adjusts the offset set by the [OFFSET](../../sql-reference/statements/select/offset.md/#offset-fetch) clause, so that these two values are summarized.

Possible values:

- 0 — No rows are skipped .
- Positive integer.

**Example**

Input table:

``` sql
CREATE TABLE test (i UInt64) ENGINE = MergeTree() ORDER BY i;
INSERT INTO test SELECT number FROM numbers(500);
```

Query:

``` sql
SET limit = 5;
SET offset = 7;
SELECT * FROM test LIMIT 10 OFFSET 100;
```
Result:

``` text
┌───i─┐
│ 107 │
│ 108 │
│ 109 │
└─────┘
```

## opentelemetry_start_trace_probability {#opentelemetry_start_trace_probability}

Type: Float

Default value: 0

Sets the probability that the ClickHouse can start a trace for executed queries (if no parent [trace context](https://www.w3.org/TR/trace-context/) is supplied).

Possible values:

- 0 — The trace for all executed queries is disabled (if no parent trace context is supplied).
- Positive floating-point number in the range [0..1]. For example, if the setting value is `0,5`, ClickHouse can start a trace on average for half of the queries.
- 1 — The trace for all executed queries is enabled.

## opentelemetry_trace_processors {#opentelemetry_trace_processors}

Type: Bool

Default value: 0

Collect OpenTelemetry spans for processors.

## optimize_aggregation_in_order {#optimize_aggregation_in_order}

Type: Bool

Default value: 0

Enables [GROUP BY](../../sql-reference/statements/select/group-by.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for aggregating data in corresponding order in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

- 0 — `GROUP BY` optimization is disabled.
- 1 — `GROUP BY` optimization is enabled.

**See Also**

- [GROUP BY optimization](../../sql-reference/statements/select/group-by.md/#aggregation-in-order)

## optimize_aggregators_of_group_by_keys {#optimize_aggregators_of_group_by_keys}

Type: Bool

Default value: 1

Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section

## optimize_append_index {#optimize_append_index}

Type: Bool

Default value: 0

Use [constraints](../../sql-reference/statements/create/table.md#constraints) in order to append index condition. The default is `false`.

Possible values:

- true, false

## optimize_arithmetic_operations_in_aggregate_functions {#optimize_arithmetic_operations_in_aggregate_functions}

Type: Bool

Default value: 1

Move arithmetic operations out of aggregation functions

## optimize_count_from_files {#optimize_count_from_files}

Type: Bool

Default value: 1

Enables or disables the optimization of counting number of rows from files in different input formats. It applies to table functions/engines `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`.

Possible values:

- 0 — Optimization disabled.
- 1 — Optimization enabled.

## optimize_distinct_in_order {#optimize_distinct_in_order}

Type: Bool

Default value: 1

Enable DISTINCT optimization if some columns in DISTINCT form a prefix of sorting. For example, prefix of sorting key in merge tree or ORDER BY statement

## optimize_distributed_group_by_sharding_key {#optimize_distributed_group_by_sharding_key}

Type: Bool

Default value: 1

Optimize `GROUP BY sharding_key` queries, by avoiding costly aggregation on the initiator server (which will reduce memory usage for the query on the initiator server).

The following types of queries are supported (and all combinations of them):

- `SELECT DISTINCT [..., ]sharding_key[, ...] FROM dist`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...]`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] ORDER BY x`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1 BY x`

The following types of queries are not supported (support for some of them may be added later):

- `SELECT ... GROUP BY sharding_key[, ...] WITH TOTALS`
- `SELECT ... GROUP BY sharding_key[, ...] WITH ROLLUP`
- `SELECT ... GROUP BY sharding_key[, ...] WITH CUBE`
- `SELECT ... GROUP BY sharding_key[, ...] SETTINGS extremes=1`

Possible values:

- 0 — Disabled.
- 1 — Enabled.

See also:

- [distributed_group_by_no_merge](#distributed-group-by-no-merge)
- [distributed_push_down_limit](#distributed-push-down-limit)
- [optimize_skip_unused_shards](#optimize-skip-unused-shards)

:::note
Right now it requires `optimize_skip_unused_shards` (the reason behind this is that one day it may be enabled by default, and it will work correctly only if data was inserted via Distributed table, i.e. data is distributed according to sharding_key).
:::

## optimize_functions_to_subcolumns {#optimize_functions_to_subcolumns}

Type: Bool

Default value: 1

Enables or disables optimization by transforming some functions to reading subcolumns. This reduces the amount of data to read.

These functions can be transformed:

- [length](../../sql-reference/functions/array-functions.md/#array_functions-length) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [empty](../../sql-reference/functions/array-functions.md/#function-empty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [notEmpty](../../sql-reference/functions/array-functions.md/#function-notempty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [isNull](../../sql-reference/operators/index.md#operator-is-null) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [isNotNull](../../sql-reference/operators/index.md#is-not-null) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [count](../../sql-reference/aggregate-functions/reference/count.md) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [mapKeys](../../sql-reference/functions/tuple-map-functions.md/#mapkeys) to read the [keys](../../sql-reference/data-types/map.md/#map-subcolumns) subcolumn.
- [mapValues](../../sql-reference/functions/tuple-map-functions.md/#mapvalues) to read the [values](../../sql-reference/data-types/map.md/#map-subcolumns) subcolumn.

Possible values:

- 0 — Optimization disabled.
- 1 — Optimization enabled.

## optimize_group_by_constant_keys {#optimize_group_by_constant_keys}

Type: Bool

Default value: 1

Optimize GROUP BY when all keys in block are constant

## optimize_group_by_function_keys {#optimize_group_by_function_keys}

Type: Bool

Default value: 1

Eliminates functions of other keys in GROUP BY section

## optimize_if_chain_to_multiif {#optimize_if_chain_to_multiif}

Type: Bool

Default value: 0

Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not beneficial for numeric types.

## optimize_if_transform_strings_to_enum {#optimize_if_transform_strings_to_enum}

Type: Bool

Default value: 0

Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in distributed query that would lead to its fail.

## optimize_injective_functions_in_group_by {#optimize_injective_functions_in_group_by}

Type: Bool

Default value: 1

Replaces injective functions by it's arguments in GROUP BY section

## optimize_injective_functions_inside_uniq {#optimize_injective_functions_inside_uniq}

Type: Bool

Default value: 1

Delete injective functions of one argument inside uniq*() functions.

## optimize_min_equality_disjunction_chain_length {#optimize_min_equality_disjunction_chain_length}

Type: UInt64

Default value: 3

The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization 

## optimize_min_inequality_conjunction_chain_length {#optimize_min_inequality_conjunction_chain_length}

Type: UInt64

Default value: 3

The minimum length of the expression `expr <> x1 AND ... expr <> xN` for optimization 

## optimize_move_to_prewhere {#optimize_move_to_prewhere}

Type: Bool

Default value: 1

Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries.

Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables.

Possible values:

- 0 — Automatic `PREWHERE` optimization is disabled.
- 1 — Automatic `PREWHERE` optimization is enabled.

## optimize_move_to_prewhere_if_final {#optimize_move_to_prewhere_if_final}

Type: Bool

Default value: 0

Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries with [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier.

Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables.

Possible values:

- 0 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is disabled.
- 1 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is enabled.

**See Also**

- [optimize_move_to_prewhere](#optimize_move_to_prewhere) setting

## optimize_multiif_to_if {#optimize_multiif_to_if}

Type: Bool

Default value: 1

Replace 'multiIf' with only one condition to 'if'.

## optimize_normalize_count_variants {#optimize_normalize_count_variants}

Type: Bool

Default value: 1

Rewrite aggregate functions that semantically equals to count() as count().

## optimize_on_insert {#optimize_on_insert}

Type: Bool

Default value: 1

Enables or disables data transformation before the insertion, as if merge was done on this block (according to table engine).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Example**

The difference between enabled and disabled:

Query:

```sql
SET optimize_on_insert = 1;

CREATE TABLE test1 (`FirstTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY FirstTable;

INSERT INTO test1 SELECT number % 2 FROM numbers(5);

SELECT * FROM test1;

SET optimize_on_insert = 0;

CREATE TABLE test2 (`SecondTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY SecondTable;

INSERT INTO test2 SELECT number % 2 FROM numbers(5);

SELECT * FROM test2;
```

Result:

``` text
┌─FirstTable─┐
│          0 │
│          1 │
└────────────┘

┌─SecondTable─┐
│           0 │
│           0 │
│           0 │
│           1 │
│           1 │
└─────────────┘
```

Note that this setting influences [Materialized view](../../sql-reference/statements/create/view.md/#materialized) and [MaterializedMySQL](../../engines/database-engines/materialized-mysql.md) behaviour.

## optimize_or_like_chain {#optimize_or_like_chain}

Type: Bool

Default value: 0

Optimize multiple OR LIKE into multiMatchAny. This optimization should not be enabled by default, because it defies index analysis in some cases.

## optimize_read_in_order {#optimize_read_in_order}

Type: Bool

Default value: 1

Enables [ORDER BY](../../sql-reference/statements/select/order-by.md/#optimize_read_in_order) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for reading data from [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

- 0 — `ORDER BY` optimization is disabled.
- 1 — `ORDER BY` optimization is enabled.

**See Also**

- [ORDER BY Clause](../../sql-reference/statements/select/order-by.md/#optimize_read_in_order)

## optimize_read_in_window_order {#optimize_read_in_window_order}

Type: Bool

Default value: 1

Enable ORDER BY optimization in window clause for reading data in corresponding order in MergeTree tables.

## optimize_redundant_functions_in_order_by {#optimize_redundant_functions_in_order_by}

Type: Bool

Default value: 1

Remove functions from ORDER BY if its argument is also in ORDER BY

## optimize_respect_aliases {#optimize_respect_aliases}

Type: Bool

Default value: 1

If it is set to true, it will respect aliases in WHERE/GROUP BY/ORDER BY, that will help with partition pruning/secondary indexes/optimize_aggregation_in_order/optimize_read_in_order/optimize_trivial_count

## optimize_rewrite_aggregate_function_with_if {#optimize_rewrite_aggregate_function_with_if}

Type: Bool

Default value: 1

Rewrite aggregate functions with if expression as argument when logically equivalent.
For example, `avg(if(cond, col, null))` can be rewritten to `avgOrNullIf(cond, col)`. It may improve performance.

:::note
Supported only with experimental analyzer (`enable_analyzer = 1`).
:::

## optimize_rewrite_array_exists_to_has {#optimize_rewrite_array_exists_to_has}

Type: Bool

Default value: 0

Rewrite arrayExists() functions to has() when logically equivalent. For example, arrayExists(x -> x = 1, arr) can be rewritten to has(arr, 1)

## optimize_rewrite_sum_if_to_count_if {#optimize_rewrite_sum_if_to_count_if}

Type: Bool

Default value: 1

Rewrite sumIf() and sum(if()) function countIf() function when logically equivalent

## optimize_skip_merged_partitions {#optimize_skip_merged_partitions}

Type: Bool

Default value: 0

Enables or disables optimization for [OPTIMIZE TABLE ... FINAL](../../sql-reference/statements/optimize.md) query if there is only one part with level > 0 and it doesn't have expired TTL.

- `OPTIMIZE TABLE ... FINAL SETTINGS optimize_skip_merged_partitions=1`

By default, `OPTIMIZE TABLE ... FINAL` query rewrites the one part even if there is only a single part.

Possible values:

- 1 - Enable optimization.
- 0 - Disable optimization.

## optimize_skip_unused_shards {#optimize_skip_unused_shards}

Type: Bool

Default value: 0

Enables or disables skipping of unused shards for [SELECT](../../sql-reference/statements/select/index.md) queries that have sharding key condition in `WHERE/PREWHERE` (assuming that the data is distributed by sharding key, otherwise a query yields incorrect result).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## optimize_skip_unused_shards_limit {#optimize_skip_unused_shards_limit}

Type: UInt64

Default value: 1000

Limit for number of sharding key values, turns off `optimize_skip_unused_shards` if the limit is reached.

Too many values may require significant amount for processing, while the benefit is doubtful, since if you have huge number of values in `IN (...)`, then most likely the query will be sent to all shards anyway.

## optimize_skip_unused_shards_nesting {#optimize_skip_unused_shards_nesting}

Type: UInt64

Default value: 0

Controls [`optimize_skip_unused_shards`](#optimize-skip-unused-shards) (hence still requires [`optimize_skip_unused_shards`](#optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

- 0 — Disabled, `optimize_skip_unused_shards` works always.
- 1 — Enables `optimize_skip_unused_shards` only for the first level.
- 2 — Enables `optimize_skip_unused_shards` up to the second level.

## optimize_skip_unused_shards_rewrite_in {#optimize_skip_unused_shards_rewrite_in}

Type: Bool

Default value: 1

Rewrite IN in query for remote shards to exclude values that does not belong to the shard (requires optimize_skip_unused_shards).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## optimize_sorting_by_input_stream_properties {#optimize_sorting_by_input_stream_properties}

Type: Bool

Default value: 1

Optimize sorting by sorting properties of input stream

## optimize_substitute_columns {#optimize_substitute_columns}

Type: Bool

Default value: 0

Use [constraints](../../sql-reference/statements/create/table.md#constraints) for column substitution. The default is `false`.

Possible values:

- true, false

## optimize_syntax_fuse_functions {#optimize_syntax_fuse_functions}

Type: Bool

Default value: 0

Enables to fuse aggregate functions with identical argument. It rewrites query contains at least two aggregate functions from [sum](../../sql-reference/aggregate-functions/reference/sum.md/#agg_function-sum), [count](../../sql-reference/aggregate-functions/reference/count.md/#agg_function-count) or [avg](../../sql-reference/aggregate-functions/reference/avg.md/#agg_function-avg) with identical argument to [sumCount](../../sql-reference/aggregate-functions/reference/sumcount.md/#agg_function-sumCount).

Possible values:

- 0 — Functions with identical argument are not fused.
- 1 — Functions with identical argument are fused.

**Example**

Query:

``` sql
CREATE TABLE fuse_tbl(a Int8, b Int8) Engine = Log;
SET optimize_syntax_fuse_functions = 1;
EXPLAIN SYNTAX SELECT sum(a), sum(b), count(b), avg(b) from fuse_tbl FORMAT TSV;
```

Result:

``` text
SELECT
    sum(a),
    sumCount(b).1,
    sumCount(b).2,
    (sumCount(b).1) / (sumCount(b).2)
FROM fuse_tbl
```

## optimize_throw_if_noop {#optimize_throw_if_noop}

Type: Bool

Default value: 0

Enables or disables throwing an exception if an [OPTIMIZE](../../sql-reference/statements/optimize.md) query didn’t perform a merge.

By default, `OPTIMIZE` returns successfully even if it didn’t do anything. This setting lets you differentiate these situations and get the reason in an exception message.

Possible values:

- 1 — Throwing an exception is enabled.
- 0 — Throwing an exception is disabled.

## optimize_time_filter_with_preimage {#optimize_time_filter_with_preimage}

Type: Bool

Default value: 1

Optimize Date and DateTime predicates by converting functions into equivalent comparisons without conversions (e.g. toYear(col) = 2023 -> col >= '2023-01-01' AND col <= '2023-12-31')

## optimize_trivial_approximate_count_query {#optimize_trivial_approximate_count_query}

Type: Bool

Default value: 0

Use an approximate value for trivial count optimization of storages that support such estimation, for example, EmbeddedRocksDB.

Possible values:

   - 0 — Optimization disabled.
   - 1 — Optimization enabled.

## optimize_trivial_count_query {#optimize_trivial_count_query}

Type: Bool

Default value: 1

Enables or disables the optimization to trivial query `SELECT count() FROM table` using metadata from MergeTree. If you need to use row-level security, disable this setting.

Possible values:

   - 0 — Optimization disabled.
   - 1 — Optimization enabled.

See also:

- [optimize_functions_to_subcolumns](#optimize-functions-to-subcolumns)

## optimize_trivial_insert_select {#optimize_trivial_insert_select}

Type: Bool

Default value: 0

Optimize trivial 'INSERT INTO table SELECT ... FROM TABLES' query

## optimize_uniq_to_count {#optimize_uniq_to_count}

Type: Bool

Default value: 1

Rewrite uniq and its variants(except uniqUpTo) to count if subquery has distinct or group by clause.

## optimize_use_implicit_projections {#optimize_use_implicit_projections}

Type: Bool

Default value: 1

Automatically choose implicit projections to perform SELECT query

## optimize_use_projections {#optimize_use_projections}

Type: Bool

Default value: 1

Enables or disables [projection](../../engines/table-engines/mergetree-family/mergetree.md/#projections) optimization when processing `SELECT` queries.

Possible values:

- 0 — Projection optimization disabled.
- 1 — Projection optimization enabled.

## optimize_using_constraints {#optimize_using_constraints}

Type: Bool

Default value: 0

Use [constraints](../../sql-reference/statements/create/table.md#constraints) for query optimization. The default is `false`.

Possible values:

- true, false

## os_thread_priority {#os_thread_priority}

Type: Int64

Default value: 0

Sets the priority ([nice](https://en.wikipedia.org/wiki/Nice_(Unix))) for threads that execute queries. The OS scheduler considers this priority when choosing the next thread to run on each available CPU core.

:::note
To use this setting, you need to set the `CAP_SYS_NICE` capability. The `clickhouse-server` package sets it up during installation. Some virtual environments do not allow you to set the `CAP_SYS_NICE` capability. In this case, `clickhouse-server` shows a message about it at the start.
:::

Possible values:

- You can set values in the range `[-20, 19]`.

Lower values mean higher priority. Threads with low `nice` priority values are executed more frequently than threads with high values. High values are preferable for long-running non-interactive queries because it allows them to quickly give up resources in favour of short interactive queries when they arrive.

## output_format_compression_level {#output_format_compression_level}

Type: UInt64

Default value: 3

Default compression level if query output is compressed. The setting is applied when `SELECT` query has `INTO OUTFILE` or when writing to table functions `file`, `url`, `hdfs`, `s3`, or `azureBlobStorage`.

Possible values: from `1` to `22`

## output_format_compression_zstd_window_log {#output_format_compression_zstd_window_log}

Type: UInt64

Default value: 0

Can be used when the output compression method is `zstd`. If greater than `0`, this setting explicitly sets compression window size (power of `2`) and enables a long-range mode for zstd compression. This can help to achieve a better compression ratio.

Possible values: non-negative numbers. Note that if the value is too small or too big, `zstdlib` will throw an exception. Typical values are from `20` (window size = `1MB`) to `30` (window size = `1GB`).

## output_format_parallel_formatting {#output_format_parallel_formatting}

Type: Bool

Default value: 1

Enables or disables parallel formatting of data formats. Supported only for [TSV](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [CSV](../../interfaces/formats.md/#csv) and [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) formats.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

## page_cache_inject_eviction {#page_cache_inject_eviction}

Type: Bool

Default value: 0

Userspace page cache will sometimes invalidate some pages at random. Intended for testing.

## parallel_distributed_insert_select {#parallel_distributed_insert_select}

Type: UInt64

Default value: 0

Enables parallel distributed `INSERT ... SELECT` query.

If we execute `INSERT INTO distributed_table_a SELECT ... FROM distributed_table_b` queries and both tables use the same cluster, and both tables are either [replicated](../../engines/table-engines/mergetree-family/replication.md) or non-replicated, then this query is processed locally on every shard.

Possible values:

- 0 — Disabled.
- 1 — `SELECT` will be executed on each shard from the underlying table of the distributed engine.
- 2 — `SELECT` and `INSERT` will be executed on each shard from/to the underlying table of the distributed engine.

## parallel_replica_offset {#parallel_replica_offset}

Type: UInt64

Default value: 0

This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the index of the replica participating in query processing among parallel replicas.

## parallel_replicas_allow_in_with_subquery {#parallel_replicas_allow_in_with_subquery}

Type: Bool

Default value: 1

If true, subquery for IN will be executed on every follower replica.

## parallel_replicas_count {#parallel_replicas_count}

Type: UInt64

Default value: 0

This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the number of parallel replicas participating in query processing.

## parallel_replicas_custom_key {#parallel_replicas_custom_key}

Type: String

Default value: 

An arbitrary integer expression that can be used to split work between replicas for a specific table.
The value can be any integer expression.

Simple expressions using primary keys are preferred.

If the setting is used on a cluster that consists of a single shard with multiple replicas, those replicas will be converted into virtual shards.
Otherwise, it will behave same as for `SAMPLE` key, it will use multiple replicas of each shard.

## parallel_replicas_custom_key_range_lower {#parallel_replicas_custom_key_range_lower}

Type: UInt64

Default value: 0

Allows the filter type `range` to split the work evenly between replicas based on the custom range `[parallel_replicas_custom_key_range_lower, INT_MAX]`.

When used in conjunction with [parallel_replicas_custom_key_range_upper](#parallel_replicas_custom_key_range_upper), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`.

Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing.

## parallel_replicas_custom_key_range_upper {#parallel_replicas_custom_key_range_upper}

Type: UInt64

Default value: 0

Allows the filter type `range` to split the work evenly between replicas based on the custom range `[0, parallel_replicas_custom_key_range_upper]`. A value of 0 disables the upper bound, setting it the max value of the custom key expression.

When used in conjunction with [parallel_replicas_custom_key_range_lower](#parallel_replicas_custom_key_range_lower), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`.

Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing

## parallel_replicas_for_non_replicated_merge_tree {#parallel_replicas_for_non_replicated_merge_tree}

Type: Bool

Default value: 0

If true, ClickHouse will use parallel replicas algorithm also for non-replicated MergeTree tables

## parallel_replicas_local_plan {#parallel_replicas_local_plan}

Type: Bool

Default value: 0

Build local plan for local replica

## parallel_replicas_mark_segment_size {#parallel_replicas_mark_segment_size}

Type: UInt64

Default value: 0

Parts virtually divided into segments to be distributed between replicas for parallel reading. This setting controls the size of these segments. Not recommended to change until you're absolutely sure in what you're doing. Value should be in range [128; 16384]

## parallel_replicas_min_number_of_rows_per_replica {#parallel_replicas_min_number_of_rows_per_replica}

Type: UInt64

Default value: 0

Limit the number of replicas used in a query to (estimated rows to read / min_number_of_rows_per_replica). The max is still limited by 'max_parallel_replicas'

## parallel_replicas_mode {#parallel_replicas_mode}

Type: ParallelReplicasMode

Default value: read_tasks

Type of filter to use with custom key for parallel replicas. default - use modulo operation on the custom key, range - use range filter on custom key using all possible values for the value type of custom key.

## parallel_replicas_prefer_local_join {#parallel_replicas_prefer_local_join}

Type: Bool

Default value: 1

If true, and JOIN can be executed with parallel replicas algorithm, and all storages of right JOIN part are *MergeTree, local JOIN will be used instead of GLOBAL JOIN.

## parallel_replicas_single_task_marks_count_multiplier {#parallel_replicas_single_task_marks_count_multiplier}

Type: Float

Default value: 2

A multiplier which will be added during calculation for minimal number of marks to retrieve from coordinator. This will be applied only for remote replicas.

## parallel_view_processing {#parallel_view_processing}

Type: Bool

Default value: 0

Enables pushing to attached views concurrently instead of sequentially.

## parallelize_output_from_storages {#parallelize_output_from_storages}

Type: Bool

Default value: 1

Parallelize output for reading step from storage. It allows parallelization of  query processing right after reading from storage if possible

## parsedatetime_parse_without_leading_zeros {#parsedatetime_parse_without_leading_zeros}

Type: Bool

Default value: 1

Formatters '%c', '%l' and '%k' in function 'parseDateTime()' parse months and hours without leading zeros.

## partial_merge_join_left_table_buffer_bytes {#partial_merge_join_left_table_buffer_bytes}

Type: UInt64

Default value: 0

If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread.

## partial_merge_join_rows_in_right_blocks {#partial_merge_join_rows_in_right_blocks}

Type: UInt64

Default value: 65536

Limits sizes of right-hand join data blocks in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.

ClickHouse server:

1.  Splits right-hand join data into blocks with up to the specified number of rows.
2.  Indexes each block with its minimum and maximum values.
3.  Unloads prepared blocks to disk if it is possible.

Possible values:

- Any positive integer. Recommended range of values: \[1000, 100000\].

## partial_result_on_first_cancel {#partial_result_on_first_cancel}

Type: Bool

Default value: 0

Allows query to return a partial result after cancel.

## parts_to_delay_insert {#parts_to_delay_insert}

Type: UInt64

Default value: 0

If the destination table contains at least that many active parts in a single partition, artificially slow down insert into table.

## parts_to_throw_insert {#parts_to_throw_insert}

Type: UInt64

Default value: 0

If more than this number active parts in a single partition of the destination table, throw 'Too many parts ...' exception.

## periodic_live_view_refresh {#periodic_live_view_refresh}

Type: Seconds

Default value: 60

Interval after which periodically refreshed live view is forced to refresh.

## poll_interval {#poll_interval}

Type: UInt64

Default value: 10

Block at the query wait loop on the server for the specified number of seconds.

## postgresql_connection_attempt_timeout {#postgresql_connection_attempt_timeout}

Type: UInt64

Default value: 2

Connection timeout in seconds of a single attempt to connect PostgreSQL end-point.
The value is passed as a `connect_timeout` parameter of the connection URL.

## postgresql_connection_pool_auto_close_connection {#postgresql_connection_pool_auto_close_connection}

Type: Bool

Default value: 0

Close connection before returning connection to the pool.

## postgresql_connection_pool_retries {#postgresql_connection_pool_retries}

Type: UInt64

Default value: 2

Connection pool push/pop retries number for PostgreSQL table engine and database engine.

## postgresql_connection_pool_size {#postgresql_connection_pool_size}

Type: UInt64

Default value: 16

Connection pool size for PostgreSQL table engine and database engine.

## postgresql_connection_pool_wait_timeout {#postgresql_connection_pool_wait_timeout}

Type: UInt64

Default value: 5000

Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty pool.

## prefer_column_name_to_alias {#prefer_column_name_to_alias}

Type: Bool

Default value: 0

Enables or disables using the original column names instead of aliases in query expressions and clauses. It especially matters when alias is the same as the column name, see [Expression Aliases](../../sql-reference/syntax.md/#notes-on-usage). Enable this setting to make aliases syntax rules in ClickHouse more compatible with most other database engines.

Possible values:

- 0 — The column name is substituted with the alias.
- 1 — The column name is not substituted with the alias.

**Example**

The difference between enabled and disabled:

Query:

```sql
SET prefer_column_name_to_alias = 0;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Result:

```text
Received exception from server (version 21.5.1):
Code: 184. DB::Exception: Received from localhost:9000. DB::Exception: Aggregate function avg(number) is found inside another aggregate function in query: While processing avg(number) AS number.
```

Query:

```sql
SET prefer_column_name_to_alias = 1;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Result:

```text
┌─number─┬─max(number)─┐
│    4.5 │           9 │
└────────┴─────────────┘
```

## prefer_external_sort_block_bytes {#prefer_external_sort_block_bytes}

Type: UInt64

Default value: 16744704

Prefer maximum block bytes for external sort, reduce the memory usage during merging.

## prefer_global_in_and_join {#prefer_global_in_and_join}

Type: Bool

Default value: 0

Enables the replacement of `IN`/`JOIN` operators with `GLOBAL IN`/`GLOBAL JOIN`.

Possible values:

- 0 — Disabled. `IN`/`JOIN` operators are not replaced with `GLOBAL IN`/`GLOBAL JOIN`.
- 1 — Enabled. `IN`/`JOIN` operators are replaced with `GLOBAL IN`/`GLOBAL JOIN`.

**Usage**

Although `SET distributed_product_mode=global` can change the queries behavior for the distributed tables, it's not suitable for local tables or tables from external resources. Here is when the `prefer_global_in_and_join` setting comes into play.

For example, we have query serving nodes that contain local tables, which are not suitable for distribution. We need to scatter their data on the fly during distributed processing with the `GLOBAL` keyword — `GLOBAL IN`/`GLOBAL JOIN`.

Another use case of `prefer_global_in_and_join` is accessing tables created by external engines. This setting helps to reduce the number of calls to external sources while joining such tables: only one call per query.

**See also:**

- [Distributed subqueries](../../sql-reference/operators/in.md/#select-distributed-subqueries) for more information on how to use `GLOBAL IN`/`GLOBAL JOIN`

## prefer_localhost_replica {#prefer_localhost_replica}

Type: Bool

Default value: 1

Enables/disables preferable using the localhost replica when processing distributed queries.

Possible values:

- 1 — ClickHouse always sends a query to the localhost replica if it exists.
- 0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#load_balancing) setting.

:::note
Disable this setting if you use [max_parallel_replicas](#max_parallel_replicas) without [parallel_replicas_custom_key](#parallel_replicas_custom_key).
If [parallel_replicas_custom_key](#parallel_replicas_custom_key) is set, disable this setting only if it's used on a cluster with multiple shards containing multiple replicas.
If it's used on a cluster with a single shard and multiple replicas, disabling this setting will have negative effects.
:::

## prefer_warmed_unmerged_parts_seconds {#prefer_warmed_unmerged_parts_seconds}

Type: Int64

Default value: 0

Only available in ClickHouse Cloud. If a merged part is less than this many seconds old and is not pre-warmed (see cache_populated_by_fetch), but all its source parts are available and pre-warmed, SELECT queries will read from those parts instead. Only for ReplicatedMergeTree. Note that this only checks whether CacheWarmer processed the part; if the part was fetched into cache by something else, it'll still be considered cold until CacheWarmer gets to it; if it was warmed, then evicted from cache, it'll still be considered warm.

## preferred_block_size_bytes {#preferred_block_size_bytes}

Type: UInt64

Default value: 1000000

This setting adjusts the data block size for query processing and represents additional fine-tuning to the more rough 'max_block_size' setting. If the columns are large and with 'max_block_size' rows the block size is likely to be larger than the specified amount of bytes, its size will be lowered for better CPU cache locality.

## preferred_max_column_in_block_size_bytes {#preferred_max_column_in_block_size_bytes}

Type: UInt64

Default value: 0

Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.

## preferred_optimize_projection_name {#preferred_optimize_projection_name}

Type: String

Default value: 

If it is set to a non-empty string, ClickHouse will try to apply specified projection in query.


Possible values:

- string: name of preferred projection

## prefetch_buffer_size {#prefetch_buffer_size}

Type: UInt64

Default value: 1048576

The maximum size of the prefetch buffer to read from the filesystem.

## print_pretty_type_names {#print_pretty_type_names}

Type: Bool

Default value: 1

Allows to print deep-nested type names in a pretty way with indents in `DESCRIBE` query and in `toTypeName()` function.

Example:

```sql
CREATE TABLE test (a Tuple(b String, c Tuple(d Nullable(UInt64), e Array(UInt32), f Array(Tuple(g String, h Map(String, Array(Tuple(i String, j UInt64))))), k Date), l Nullable(String))) ENGINE=Memory;
DESCRIBE TABLE test FORMAT TSVRaw SETTINGS print_pretty_type_names=1;
```

```
a	Tuple(
    b String,
    c Tuple(
        d Nullable(UInt64),
        e Array(UInt32),
        f Array(Tuple(
            g String,
            h Map(
                String,
                Array(Tuple(
                    i String,
                    j UInt64
                ))
            )
        )),
        k Date
    ),
    l Nullable(String)
)
```

## priority {#priority}

Type: UInt64

Default value: 0

Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.

## query_cache_compress_entries {#query_cache_compress_entries}

Type: Bool

Default value: 1

Compress entries in the [query cache](../query-cache.md). Lessens the memory consumption of the query cache at the cost of slower inserts into / reads from it.

Possible values:

- 0 - Disabled
- 1 - Enabled

## query_cache_max_entries {#query_cache_max_entries}

Type: UInt64

Default value: 0

The maximum number of query results the current user may store in the [query cache](../query-cache.md). 0 means unlimited.

Possible values:

- Positive integer >= 0.

## query_cache_max_size_in_bytes {#query_cache_max_size_in_bytes}

Type: UInt64

Default value: 0

The maximum amount of memory (in bytes) the current user may allocate in the [query cache](../query-cache.md). 0 means unlimited.

Possible values:

- Positive integer >= 0.

## query_cache_min_query_duration {#query_cache_min_query_duration}

Type: Milliseconds

Default value: 0

Minimum duration in milliseconds a query needs to run for its result to be stored in the [query cache](../query-cache.md).

Possible values:

- Positive integer >= 0.

## query_cache_min_query_runs {#query_cache_min_query_runs}

Type: UInt64

Default value: 0

Minimum number of times a `SELECT` query must run before its result is stored in the [query cache](../query-cache.md).

Possible values:

- Positive integer >= 0.

## query_cache_nondeterministic_function_handling {#query_cache_nondeterministic_function_handling}

Type: QueryCacheNondeterministicFunctionHandling

Default value: throw

Controls how the [query cache](../query-cache.md) handles `SELECT` queries with non-deterministic functions like `rand()` or `now()`.

Possible values:

- `'throw'` - Throw an exception and don't cache the query result.
- `'save'` - Cache the query result.
- `'ignore'` - Don't cache the query result and don't throw an exception.

## query_cache_share_between_users {#query_cache_share_between_users}

Type: Bool

Default value: 0

If turned on, the result of `SELECT` queries cached in the [query cache](../query-cache.md) can be read by other users.
It is not recommended to enable this setting due to security reasons.

Possible values:

- 0 - Disabled
- 1 - Enabled

## query_cache_squash_partial_results {#query_cache_squash_partial_results}

Type: Bool

Default value: 1

Squash partial result blocks to blocks of size [max_block_size](#setting-max_block_size). Reduces performance of inserts into the [query cache](../query-cache.md) but improves the compressability of cache entries (see [query_cache_compress-entries](#query-cache-compress-entries)).

Possible values:

- 0 - Disabled
- 1 - Enabled

## query_cache_system_table_handling {#query_cache_system_table_handling}

Type: QueryCacheSystemTableHandling

Default value: throw

Controls how the [query cache](../query-cache.md) handles `SELECT` queries against system tables, i.e. tables in databases `system.*` and `information_schema.*`.

Possible values:

- `'throw'` - Throw an exception and don't cache the query result.
- `'save'` - Cache the query result.
- `'ignore'` - Don't cache the query result and don't throw an exception.

## query_cache_tag {#query_cache_tag}

Type: String

Default value: 

A string which acts as a label for [query cache](../query-cache.md) entries.
The same queries with different tags are considered different by the query cache.

Possible values:

- Any string

## query_cache_ttl {#query_cache_ttl}

Type: Seconds

Default value: 60

After this time in seconds entries in the [query cache](../query-cache.md) become stale.

Possible values:

- Positive integer >= 0.

## query_plan_aggregation_in_order {#query_plan_aggregation_in_order}

Type: Bool

Default value: 1

Toggles the aggregation in-order query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_convert_outer_join_to_inner_join {#query_plan_convert_outer_join_to_inner_join}

Type: Bool

Default value: 1

Allow to convert OUTER JOIN to INNER JOIN if filter after JOIN always filters default values

## query_plan_enable_multithreading_after_window_functions {#query_plan_enable_multithreading_after_window_functions}

Type: Bool

Default value: 1

Enable multithreading after evaluating window functions to allow parallel stream processing

## query_plan_enable_optimizations {#query_plan_enable_optimizations}

Type: Bool

Default value: 1

Toggles query optimization at the query plan level.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable all optimizations at the query plan level
- 1 - Enable optimizations at the query plan level (but individual optimizations may still be disabled via their individual settings)

## query_plan_execute_functions_after_sorting {#query_plan_execute_functions_after_sorting}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which moves expressions after sorting steps.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_filter_push_down {#query_plan_filter_push_down}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which moves filters down in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_lift_up_array_join {#query_plan_lift_up_array_join}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which moves ARRAY JOINs up in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_lift_up_union {#query_plan_lift_up_union}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which moves larger subtrees of the query plan into union to enable further optimizations.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_max_optimizations_to_apply {#query_plan_max_optimizations_to_apply}

Type: UInt64

Default value: 10000

Limits the total number of optimizations applied to query plan, see setting [query_plan_enable_optimizations](#query_plan_enable_optimizations).
Useful to avoid long optimization times for complex queries.
If the actual number of optimizations exceeds this setting, an exception is thrown.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

## query_plan_merge_expressions {#query_plan_merge_expressions}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which merges consecutive filters.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_merge_filters {#query_plan_merge_filters}

Type: Bool

Default value: 0

Allow to merge filters in the query plan

## query_plan_optimize_prewhere {#query_plan_optimize_prewhere}

Type: Bool

Default value: 1

Allow to push down filter to PREWHERE expression for supported storages

## query_plan_push_down_limit {#query_plan_push_down_limit}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which moves LIMITs down in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_read_in_order {#query_plan_read_in_order}

Type: Bool

Default value: 1

Toggles the read in-order optimization query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_remove_redundant_distinct {#query_plan_remove_redundant_distinct}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which removes redundant DISTINCT steps.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_remove_redundant_sorting {#query_plan_remove_redundant_sorting}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which removes redundant sorting steps, e.g. in subqueries.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_reuse_storage_ordering_for_window_functions {#query_plan_reuse_storage_ordering_for_window_functions}

Type: Bool

Default value: 1

Toggles a query-plan-level optimization which uses storage sorting when sorting for window functions.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

## query_plan_split_filter {#query_plan_split_filter}

Type: Bool

Default value: 1

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Toggles a query-plan-level optimization which splits filters into expressions.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

Possible values:

- 0 - Disable
- 1 - Enable

## query_profiler_cpu_time_period_ns {#query_profiler_cpu_time_period_ns}

Type: UInt64

Default value: 1000000000

Sets the period for a CPU clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). This timer counts only CPU time.

Possible values:

- A positive integer number of nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

- 0 for turning off the timer.

**Temporarily disabled in ClickHouse Cloud.**

See also:

- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)

## query_profiler_real_time_period_ns {#query_profiler_real_time_period_ns}

Type: UInt64

Default value: 1000000000

Sets the period for a real clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Real clock timer counts wall-clock time.

Possible values:

- Positive integer number, in nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

- 0 for turning off the timer.

**Temporarily disabled in ClickHouse Cloud.**

See also:

- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)

## queue_max_wait_ms {#queue_max_wait_ms}

Type: Milliseconds

Default value: 0

The wait time in the request queue, if the number of concurrent requests exceeds the maximum.

## rabbitmq_max_wait_ms {#rabbitmq_max_wait_ms}

Type: Milliseconds

Default value: 5000

The wait time for reading from RabbitMQ before retry.

## read_backoff_max_throughput {#read_backoff_max_throughput}

Type: UInt64

Default value: 1048576

Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.

## read_backoff_min_concurrency {#read_backoff_min_concurrency}

Type: UInt64

Default value: 1

Settings to try keeping the minimal number of threads in case of slow reads.

## read_backoff_min_events {#read_backoff_min_events}

Type: UInt64

Default value: 2

Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.

## read_backoff_min_interval_between_events_ms {#read_backoff_min_interval_between_events_ms}

Type: Milliseconds

Default value: 1000

Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.

## read_backoff_min_latency_ms {#read_backoff_min_latency_ms}

Type: Milliseconds

Default value: 1000

Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.

## read_from_filesystem_cache_if_exists_otherwise_bypass_cache {#read_from_filesystem_cache_if_exists_otherwise_bypass_cache}

Type: Bool

Default value: 0

Allow to use the filesystem cache in passive mode - benefit from the existing cache entries, but don't put more entries into the cache. If you set this setting for heavy ad-hoc queries and leave it disabled for short real-time queries, this will allows to avoid cache threshing by too heavy queries and to improve the overall system efficiency.

## read_from_page_cache_if_exists_otherwise_bypass_cache {#read_from_page_cache_if_exists_otherwise_bypass_cache}

Type: Bool

Default value: 0

Use userspace page cache in passive mode, similar to read_from_filesystem_cache_if_exists_otherwise_bypass_cache.

## read_in_order_two_level_merge_threshold {#read_in_order_two_level_merge_threshold}

Type: UInt64

Default value: 100

Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.

## read_in_order_use_buffering {#read_in_order_use_buffering}

Type: Bool

Default value: 1

Use buffering before merging while reading in order of primary key. It increases the parallelism of query execution

## read_overflow_mode {#read_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## read_overflow_mode_leaf {#read_overflow_mode_leaf}

Type: OverflowMode

Default value: throw

What to do when the leaf limit is exceeded.

## read_priority {#read_priority}

Type: Int64

Default value: 0

Priority to read data from local filesystem or remote filesystem. Only supported for 'pread_threadpool' method for local filesystem and for `threadpool` method for remote filesystem.

## read_through_distributed_cache {#read_through_distributed_cache}

Type: Bool

Default value: 0

Only in ClickHouse Cloud. Allow reading from distributed cache

## readonly {#readonly}

Type: UInt64

Default value: 0

0 - no read-only restrictions. 1 - only read requests, as well as changing explicitly allowed settings. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.

## receive_data_timeout_ms {#receive_data_timeout_ms}

Type: Milliseconds

Default value: 2000

Connection timeout for receiving first packet of data or packet with positive progress from replica

## receive_timeout {#receive_timeout}

Type: Seconds

Default value: 300

Timeout for receiving data from the network, in seconds. If no bytes were received in this interval, the exception is thrown. If you set this setting on the client, the 'send_timeout' for the socket will also be set on the corresponding connection end on the server.

## regexp_max_matches_per_row {#regexp_max_matches_per_row}

Type: UInt64

Default value: 1000

Sets the maximum number of matches for a single regular expression per row. Use it to protect against memory overload when using greedy regular expression in the [extractAllGroupsHorizontal](../../sql-reference/functions/string-search-functions.md/#extractallgroups-horizontal) function.

Possible values:

- Positive integer.

## reject_expensive_hyperscan_regexps {#reject_expensive_hyperscan_regexps}

Type: Bool

Default value: 1

Reject patterns which will likely be expensive to evaluate with hyperscan (due to NFA state explosion)

## remerge_sort_lowered_memory_bytes_ratio {#remerge_sort_lowered_memory_bytes_ratio}

Type: Float

Default value: 2

If memory usage after remerge does not reduced by this ratio, remerge will be disabled.

## remote_filesystem_read_method {#remote_filesystem_read_method}

Type: String

Default value: threadpool

Method of reading data from remote filesystem, one of: read, threadpool.

## remote_filesystem_read_prefetch {#remote_filesystem_read_prefetch}

Type: Bool

Default value: 1

Should use prefetching when reading data from remote filesystem.

## remote_fs_read_backoff_max_tries {#remote_fs_read_backoff_max_tries}

Type: UInt64

Default value: 5

Max attempts to read with backoff

## remote_fs_read_max_backoff_ms {#remote_fs_read_max_backoff_ms}

Type: UInt64

Default value: 10000

Max wait time when trying to read data for remote disk

## remote_read_min_bytes_for_seek {#remote_read_min_bytes_for_seek}

Type: UInt64

Default value: 4194304

Min bytes required for remote read (url, s3) to do seek, instead of read with ignore.

## rename_files_after_processing {#rename_files_after_processing}

Type: String

Default value: 

- **Type:** String

- **Default value:** Empty string

This setting allows to specify renaming pattern for files processed by `file` table function. When option is set, all files read by `file` table function will be renamed according to specified pattern with placeholders, only if files processing was successful.

### Placeholders

- `%a` — Full original filename (e.g., "sample.csv").
- `%f` — Original filename without extension (e.g., "sample").
- `%e` — Original file extension with dot (e.g., ".csv").
- `%t` — Timestamp (in microseconds).
- `%%` — Percentage sign ("%").

### Example
- Option: `--rename_files_after_processing="processed_%f_%t%e"`

- Query: `SELECT * FROM file('sample.csv')`


If reading `sample.csv` is successful, file will be renamed to `processed_sample_1683473210851438.csv`

## replace_running_query {#replace_running_query}

Type: Bool

Default value: 0

When using the HTTP interface, the ‘query_id’ parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same ‘query_id’ already exists at this time, the behaviour depends on the ‘replace_running_query’ parameter.

`0` (default) – Throw an exception (do not allow the query to run if a query with the same ‘query_id’ is already running).

`1` – Cancel the old query and start running the new one.

Set this parameter to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn’t finished yet, it should be cancelled.

## replace_running_query_max_wait_ms {#replace_running_query_max_wait_ms}

Type: Milliseconds

Default value: 5000

The wait time for running the query with the same `query_id` to finish, when the [replace_running_query](#replace-running-query) setting is active.

Possible values:

- Positive integer.
- 0 — Throwing an exception that does not allow to run a new query if the server already executes a query with the same `query_id`.

## replication_wait_for_inactive_replica_timeout {#replication_wait_for_inactive_replica_timeout}

Type: Int64

Default value: 120

Specifies how long (in seconds) to wait for inactive replicas to execute [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries.

Possible values:

- 0 — Do not wait.
- Negative integer — Wait for unlimited time.
- Positive integer — The number of seconds to wait.

## restore_replace_external_dictionary_source_to_null {#restore_replace_external_dictionary_source_to_null}

Type: Bool

Default value: 0

Replace external dictionary sources to Null on restore. Useful for testing purposes

## restore_replace_external_engines_to_null {#restore_replace_external_engines_to_null}

Type: Bool

Default value: 0

For testing purposes. Replaces all external engines to Null to not initiate external connections.

## restore_replace_external_table_functions_to_null {#restore_replace_external_table_functions_to_null}

Type: Bool

Default value: 0

For testing purposes. Replaces all external table functions to Null to not initiate external connections.

## result_overflow_mode {#result_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## rewrite_count_distinct_if_with_count_distinct_implementation {#rewrite_count_distinct_if_with_count_distinct_implementation}

Type: Bool

Default value: 0

Allows you to rewrite `countDistcintIf` with [count_distinct_implementation](#count_distinct_implementation) setting.

Possible values:

- true — Allow.
- false — Disallow.

## s3_allow_parallel_part_upload {#s3_allow_parallel_part_upload}

Type: Bool

Default value: 1

Use multiple threads for s3 multipart upload. It may lead to slightly higher memory usage

## s3_check_objects_after_upload {#s3_check_objects_after_upload}

Type: Bool

Default value: 0

Check each uploaded object to s3 with head request to be sure that upload was successful

## s3_connect_timeout_ms {#s3_connect_timeout_ms}

Type: UInt64

Default value: 1000

Connection timeout for host from s3 disks.

## s3_create_new_file_on_insert {#s3_create_new_file_on_insert}

Type: Bool

Default value: 0

Enables or disables creating a new file on each insert in s3 engine tables. If enabled, on each insert a new S3 object will be created with the key, similar to this pattern:

initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.

## s3_disable_checksum {#s3_disable_checksum}

Type: Bool

Default value: 0

Do not calculate a checksum when sending a file to S3. This speeds up writes by avoiding excessive processing passes on a file. It is mostly safe as the data of MergeTree tables is checksummed by ClickHouse anyway, and when S3 is accessed with HTTPS, the TLS layer already provides integrity while transferring through the network. While additional checksums on S3 give defense in depth.

## s3_ignore_file_doesnt_exist {#s3_ignore_file_doesnt_exist}

Type: Bool

Default value: 0

Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.

## s3_list_object_keys_size {#s3_list_object_keys_size}

Type: UInt64

Default value: 1000

Maximum number of files that could be returned in batch by ListObject request

## s3_max_connections {#s3_max_connections}

Type: UInt64

Default value: 1024

The maximum number of connections per server.

## s3_max_get_burst {#s3_max_get_burst}

Type: UInt64

Default value: 0

Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_get_rps`

## s3_max_get_rps {#s3_max_get_rps}

Type: UInt64

Default value: 0

Limit on S3 GET request per second rate before throttling. Zero means unlimited.

## s3_max_inflight_parts_for_one_file {#s3_max_inflight_parts_for_one_file}

Type: UInt64

Default value: 20

The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited.

## s3_max_part_number {#s3_max_part_number}

Type: UInt64

Default value: 10000

Maximum part number number for s3 upload part.

## s3_max_put_burst {#s3_max_put_burst}

Type: UInt64

Default value: 0

Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_put_rps`

## s3_max_put_rps {#s3_max_put_rps}

Type: UInt64

Default value: 0

Limit on S3 PUT request per second rate before throttling. Zero means unlimited.

## s3_max_redirects {#s3_max_redirects}

Type: UInt64

Default value: 10

Max number of S3 redirects hops allowed.

## s3_max_single_operation_copy_size {#s3_max_single_operation_copy_size}

Type: UInt64

Default value: 33554432

Maximum size for a single copy operation in s3

## s3_max_single_part_upload_size {#s3_max_single_part_upload_size}

Type: UInt64

Default value: 33554432

The maximum size of object to upload using singlepart upload to S3.

## s3_max_single_read_retries {#s3_max_single_read_retries}

Type: UInt64

Default value: 4

The maximum number of retries during single S3 read.

## s3_max_unexpected_write_error_retries {#s3_max_unexpected_write_error_retries}

Type: UInt64

Default value: 4

The maximum number of retries in case of unexpected errors during S3 write.

## s3_max_upload_part_size {#s3_max_upload_part_size}

Type: UInt64

Default value: 5368709120

The maximum size of part to upload during multipart upload to S3.

## s3_min_upload_part_size {#s3_min_upload_part_size}

Type: UInt64

Default value: 16777216

The minimum size of part to upload during multipart upload to S3.

## s3_request_timeout_ms {#s3_request_timeout_ms}

Type: UInt64

Default value: 30000

Idleness timeout for sending and receiving data to/from S3. Fail if a single TCP read or write call blocks for this long.

## s3_retry_attempts {#s3_retry_attempts}

Type: UInt64

Default value: 100

Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries

## s3_skip_empty_files {#s3_skip_empty_files}

Type: Bool

Default value: 0

Enables or disables skipping empty files in [S3](../../engines/table-engines/integrations/s3.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

## s3_strict_upload_part_size {#s3_strict_upload_part_size}

Type: UInt64

Default value: 0

The exact size of part to upload during multipart upload to S3 (some implementations does not supports variable size parts).

## s3_throw_on_zero_files_match {#s3_throw_on_zero_files_match}

Type: Bool

Default value: 0

Throw an error, when ListObjects request cannot match any files

## s3_truncate_on_insert {#s3_truncate_on_insert}

Type: Bool

Default value: 0

Enables or disables truncate before inserts in s3 engine tables. If disabled, an exception will be thrown on insert attempts if an S3 object already exists.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.

## s3_upload_part_size_multiply_factor {#s3_upload_part_size_multiply_factor}

Type: UInt64

Default value: 2

Multiply s3_min_upload_part_size by this factor each time s3_multiply_parts_count_threshold parts were uploaded from a single write to S3.

## s3_upload_part_size_multiply_parts_count_threshold {#s3_upload_part_size_multiply_parts_count_threshold}

Type: UInt64

Default value: 500

Each time this number of parts was uploaded to S3, s3_min_upload_part_size is multiplied by s3_upload_part_size_multiply_factor.

## s3_use_adaptive_timeouts {#s3_use_adaptive_timeouts}

Type: Bool

Default value: 1

When set to `true` than for all s3 requests first two attempts are made with low send and receive timeouts.
When set to `false` than all attempts are made with identical timeouts.

## s3_validate_request_settings {#s3_validate_request_settings}

Type: Bool

Default value: 1

Enables s3 request settings validation.

Possible values:
- 1 — validate settings.
- 0 — do not validate settings.

## s3queue_default_zookeeper_path {#s3queue_default_zookeeper_path}

Type: String

Default value: /clickhouse/s3queue/

Default zookeeper path prefix for S3Queue engine

## s3queue_enable_logging_to_s3queue_log {#s3queue_enable_logging_to_s3queue_log}

Type: Bool

Default value: 0

Enable writing to system.s3queue_log. The value can be overwritten per table with table settings

## schema_inference_cache_require_modification_time_for_url {#schema_inference_cache_require_modification_time_for_url}

Type: Bool

Default value: 1

Use schema from cache for URL with last modification time validation (for URLs with Last-Modified header)

## schema_inference_use_cache_for_azure {#schema_inference_use_cache_for_azure}

Type: Bool

Default value: 1

Use cache in schema inference while using azure table function

## schema_inference_use_cache_for_file {#schema_inference_use_cache_for_file}

Type: Bool

Default value: 1

Use cache in schema inference while using file table function

## schema_inference_use_cache_for_hdfs {#schema_inference_use_cache_for_hdfs}

Type: Bool

Default value: 1

Use cache in schema inference while using hdfs table function

## schema_inference_use_cache_for_s3 {#schema_inference_use_cache_for_s3}

Type: Bool

Default value: 1

Use cache in schema inference while using s3 table function

## schema_inference_use_cache_for_url {#schema_inference_use_cache_for_url}

Type: Bool

Default value: 1

Use cache in schema inference while using url table function

## select_sequential_consistency {#select_sequential_consistency}

Type: UInt64

Default value: 0

:::note
This setting differ in behavior between SharedMergeTree and ReplicatedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information about the behavior of `select_sequential_consistency` in SharedMergeTree.
:::

Enables or disables sequential consistency for `SELECT` queries. Requires `insert_quorum_parallel` to be disabled (enabled by default).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Usage

When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas.

When `insert_quorum_parallel` is enabled (the default), then `select_sequential_consistency` does not work. This is because parallel `INSERT` queries can be written to different sets of quorum replicas so there is no guarantee a single replica will have received all writes.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_timeout](#insert_quorum_timeout)
- [insert_quorum_parallel](#insert_quorum_parallel)

## send_logs_level {#send_logs_level}

Type: LogsLevel

Default value: fatal

Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'

## send_logs_source_regexp {#send_logs_source_regexp}

Type: String

Default value: 

Send server text logs with specified regexp to match log source name. Empty means all sources.

## send_progress_in_http_headers {#send_progress_in_http_headers}

Type: Bool

Default value: 0

Enables or disables `X-ClickHouse-Progress` HTTP response headers in `clickhouse-server` responses.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## send_timeout {#send_timeout}

Type: Seconds

Default value: 300

Timeout for sending data to the network, in seconds. If a client needs to send some data but is not able to send any bytes in this interval, the exception is thrown. If you set this setting on the client, the 'receive_timeout' for the socket will also be set on the corresponding connection end on the server.

## session_timezone {#session_timezone}

Type: Timezone

Default value: 

Sets the implicit time zone of the current session or query.
The implicit time zone is the time zone applied to values of type DateTime/DateTime64 which have no explicitly specified time zone.
The setting takes precedence over the globally configured (server-level) implicit time zone.
A value of '' (empty string) means that the implicit time zone of the current session or query is equal to the [server time zone](../server-configuration-parameters/settings.md#timezone).

You can use functions `timeZone()` and `serverTimeZone()` to get the session time zone and server time zone.

Possible values:

-    Any time zone name from `system.time_zones`, e.g. `Europe/Berlin`, `UTC` or `Zulu`

Examples:

```sql
SELECT timeZone(), serverTimeZone() FORMAT TSV

Europe/Berlin	Europe/Berlin
```

```sql
SELECT timeZone(), serverTimeZone() SETTINGS session_timezone = 'Asia/Novosibirsk' FORMAT TSV

Asia/Novosibirsk	Europe/Berlin
```

Assign session time zone 'America/Denver' to the inner DateTime without explicitly specified time zone:

```sql
SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS session_timezone = 'America/Denver' FORMAT TSV

1999-12-13 07:23:23.123
```

:::warning
Not all functions that parse DateTime/DateTime64 respect `session_timezone`. This can lead to subtle errors.
See the following example and explanation.
:::

```sql
CREATE TABLE test_tz (`d` DateTime('UTC')) ENGINE = Memory AS SELECT toDateTime('2000-01-01 00:00:00', 'UTC');

SELECT *, timeZone() FROM test_tz WHERE d = toDateTime('2000-01-01 00:00:00') SETTINGS session_timezone = 'Asia/Novosibirsk'
0 rows in set.

SELECT *, timeZone() FROM test_tz WHERE d = '2000-01-01 00:00:00' SETTINGS session_timezone = 'Asia/Novosibirsk'
┌───────────────────d─┬─timeZone()───────┐
│ 2000-01-01 00:00:00 │ Asia/Novosibirsk │
└─────────────────────┴──────────────────┘
```

This happens due to different parsing pipelines:

- `toDateTime()` without explicitly given time zone used in the first `SELECT` query honors setting `session_timezone` and the global time zone.
- In the second query, a DateTime is parsed from a String, and inherits the type and time zone of the existing column`d`. Thus, setting `session_timezone` and the global time zone are not honored.

**See also**

- [timezone](../server-configuration-parameters/settings.md#timezone)

## set_overflow_mode {#set_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## short_circuit_function_evaluation {#short_circuit_function_evaluation}

Type: ShortCircuitFunctionEvaluation

Default value: enable

Allows calculating the [if](../../sql-reference/functions/conditional-functions.md/#if), [multiIf](../../sql-reference/functions/conditional-functions.md/#multiif), [and](../../sql-reference/functions/logical-functions.md/#logical-and-function), and [or](../../sql-reference/functions/logical-functions.md/#logical-or-function) functions according to a [short scheme](https://en.wikipedia.org/wiki/Short-circuit_evaluation). This helps optimize the execution of complex expressions in these functions and prevent possible exceptions (such as division by zero when it is not expected).

Possible values:

- `enable` — Enables short-circuit function evaluation for functions that are suitable for it (can throw an exception or computationally heavy).
- `force_enable` — Enables short-circuit function evaluation for all functions.
- `disable` — Disables short-circuit function evaluation.

## show_table_uuid_in_table_create_query_if_not_nil {#show_table_uuid_in_table_create_query_if_not_nil}

Type: Bool

Default value: 0

Sets the `SHOW TABLE` query display.

Possible values:

- 0 — The query will be displayed without table UUID.
- 1 — The query will be displayed with table UUID.

## single_join_prefer_left_table {#single_join_prefer_left_table}

Type: Bool

Default value: 1

For single JOIN in case of identifier ambiguity prefer left table

## skip_download_if_exceeds_query_cache {#skip_download_if_exceeds_query_cache}

Type: Bool

Default value: 1

Skip download from remote filesystem if exceeds query cache size

## skip_unavailable_shards {#skip_unavailable_shards}

Type: Bool

Default value: 0

Enables or disables silently skipping of unavailable shards.

Shard is considered unavailable if all its replicas are unavailable. A replica is unavailable in the following cases:

- ClickHouse can’t connect to replica for any reason.

    When connecting to a replica, ClickHouse performs several attempts. If all these attempts fail, the replica is considered unavailable.

- Replica can’t be resolved through DNS.

    If replica’s hostname can’t be resolved through DNS, it can indicate the following situations:

    - Replica’s host has no DNS record. It can occur in systems with dynamic DNS, for example, [Kubernetes](https://kubernetes.io), where nodes can be unresolvable during downtime, and this is not an error.

    - Configuration error. ClickHouse configuration file contains a wrong hostname.

Possible values:

- 1 — skipping enabled.

    If a shard is unavailable, ClickHouse returns a result based on partial data and does not report node availability issues.

- 0 — skipping disabled.

    If a shard is unavailable, ClickHouse throws an exception.

## sleep_after_receiving_query_ms {#sleep_after_receiving_query_ms}

Type: Milliseconds

Default value: 0

Time to sleep after receiving query in TCPHandler

## sleep_in_send_data_ms {#sleep_in_send_data_ms}

Type: Milliseconds

Default value: 0

Time to sleep in sending data in TCPHandler

## sleep_in_send_tables_status_ms {#sleep_in_send_tables_status_ms}

Type: Milliseconds

Default value: 0

Time to sleep in sending tables status response in TCPHandler

## sort_overflow_mode {#sort_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## split_intersecting_parts_ranges_into_layers_final {#split_intersecting_parts_ranges_into_layers_final}

Type: Bool

Default value: 1

Split intersecting parts ranges into layers during FINAL optimization

## split_parts_ranges_into_intersecting_and_non_intersecting_final {#split_parts_ranges_into_intersecting_and_non_intersecting_final}

Type: Bool

Default value: 1

Split parts ranges into intersecting and non intersecting during FINAL optimization

## splitby_max_substrings_includes_remaining_string {#splitby_max_substrings_includes_remaining_string}

Type: Bool

Default value: 0

Controls whether function [splitBy*()](../../sql-reference/functions/splitting-merging-functions.md) with argument `max_substrings` > 0 will include the remaining string in the last element of the result array.

Possible values:

- `0` - The remaining string will not be included in the last element of the result array.
- `1` - The remaining string will be included in the last element of the result array. This is the behavior of Spark's [`split()`](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.split.html) function and Python's ['string.split()'](https://docs.python.org/3/library/stdtypes.html#str.split) method.

## stop_refreshable_materialized_views_on_startup {#stop_refreshable_materialized_views_on_startup}

Type: Bool

Default value: 0

On server startup, prevent scheduling of refreshable materialized views, as if with SYSTEM STOP VIEWS. You can manually start them with SYSTEM START VIEWS or SYSTEM START VIEW \\<name\\> afterwards. Also applies to newly created views. Has no effect on non-refreshable materialized views.

## storage_file_read_method {#storage_file_read_method}

Type: LocalFSReadMethod

Default value: pread

Method of reading data from storage file, one of: `read`, `pread`, `mmap`. The mmap method does not apply to clickhouse-server (it's intended for clickhouse-local).

## storage_system_stack_trace_pipe_read_timeout_ms {#storage_system_stack_trace_pipe_read_timeout_ms}

Type: Milliseconds

Default value: 100

Maximum time to read from a pipe for receiving information from the threads when querying the `system.stack_trace` table. This setting is used for testing purposes and not meant to be changed by users.

## stream_flush_interval_ms {#stream_flush_interval_ms}

Type: Milliseconds

Default value: 7500

Works for tables with streaming in the case of a timeout, or when a thread generates [max_insert_block_size](#max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.

## stream_like_engine_allow_direct_select {#stream_like_engine_allow_direct_select}

Type: Bool

Default value: 0

Allow direct SELECT query for Kafka, RabbitMQ, FileLog, Redis Streams, and NATS engines. In case there are attached materialized views, SELECT query is not allowed even if this setting is enabled.

## stream_like_engine_insert_queue {#stream_like_engine_insert_queue}

Type: String

Default value: 

When stream-like engine reads from multiple queues, the user will need to select one queue to insert into when writing. Used by Redis Streams and NATS.

## stream_poll_timeout_ms {#stream_poll_timeout_ms}

Type: Milliseconds

Default value: 500

Timeout for polling data from/to streaming storages.

## system_events_show_zero_values {#system_events_show_zero_values}

Type: Bool

Default value: 0

Allows to select zero-valued events from [`system.events`](../../operations/system-tables/events.md).

Some monitoring systems require passing all the metrics values to them for each checkpoint, even if the metric value is zero.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

**Examples**

Query

```sql
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Result

```text
Ok.
```

Query
```sql
SET system_events_show_zero_values = 1;
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Result

```text
┌─event────────────────────┬─value─┬─description───────────────────────────────────────────┐
│ QueryMemoryLimitExceeded │     0 │ Number of times when memory limit exceeded for query. │
└──────────────────────────┴───────┴───────────────────────────────────────────────────────┘
```

## table_function_remote_max_addresses {#table_function_remote_max_addresses}

Type: UInt64

Default value: 1000

Sets the maximum number of addresses generated from patterns for the [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

- Positive integer.

## tcp_keep_alive_timeout {#tcp_keep_alive_timeout}

Type: Seconds

Default value: 290

The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes

## temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds {#temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds}

Type: UInt64

Default value: 600000

Wait time to lock cache for space reservation for temporary data in filesystem cache

## temporary_files_codec {#temporary_files_codec}

Type: String

Default value: LZ4

Sets compression codec for temporary files used in sorting and joining operations on disk.

Possible values:

- LZ4 — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression is applied.
- NONE — No compression is applied.

## throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert {#throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert}

Type: Bool

Default value: 1

Throw exception on INSERT query when the setting `deduplicate_blocks_in_dependent_materialized_views` is enabled along with `async_insert`. It guarantees correctness, because these features can't work together.

## throw_if_no_data_to_insert {#throw_if_no_data_to_insert}

Type: Bool

Default value: 1

Allows or forbids empty INSERTs, enabled by default (throws an error on an empty insert)

## throw_on_error_from_cache_on_write_operations {#throw_on_error_from_cache_on_write_operations}

Type: Bool

Default value: 0

Ignore error from cache when caching on write operations (INSERT, merges)

## throw_on_max_partitions_per_insert_block {#throw_on_max_partitions_per_insert_block}

Type: Bool

Default value: 1

Used with max_partitions_per_insert_block. If true (default), an exception will be thrown when max_partitions_per_insert_block is reached. If false, details of the insert query reaching this limit with the number of partitions will be logged. This can be useful if you're trying to understand the impact on users when changing max_partitions_per_insert_block.

## throw_on_unsupported_query_inside_transaction {#throw_on_unsupported_query_inside_transaction}

Type: Bool

Default value: 1

Throw exception if unsupported query is used inside transaction

## timeout_before_checking_execution_speed {#timeout_before_checking_execution_speed}

Type: Seconds

Default value: 10

Check that the speed is not too low after the specified time has elapsed.

## timeout_overflow_mode {#timeout_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## timeout_overflow_mode_leaf {#timeout_overflow_mode_leaf}

Type: OverflowMode

Default value: throw

What to do when the leaf limit is exceeded.

## totals_auto_threshold {#totals_auto_threshold}

Type: Float

Default value: 0.5

The threshold for `totals_mode = 'auto'`.
See the section “WITH TOTALS modifier”.

## totals_mode {#totals_mode}

Type: TotalsMode

Default value: after_having_exclusive

How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.
See the section “WITH TOTALS modifier”.

## trace_profile_events {#trace_profile_events}

Type: Bool

Default value: 0

Enables or disables collecting stacktraces on each update of profile events along with the name of profile event and the value of increment and sending them into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- 1 — Tracing of profile events enabled.
- 0 — Tracing of profile events disabled.

## transfer_overflow_mode {#transfer_overflow_mode}

Type: OverflowMode

Default value: throw

What to do when the limit is exceeded.

## transform_null_in {#transform_null_in}

Type: Bool

Default value: 0

Enables equality of [NULL](../../sql-reference/syntax.md/#null-literal) values for [IN](../../sql-reference/operators/in.md) operator.

By default, `NULL` values can’t be compared because `NULL` means undefined value. Thus, comparison `expr = NULL` must always return `false`. With this setting `NULL = NULL` returns `true` for `IN` operator.

Possible values:

- 0 — Comparison of `NULL` values in `IN` operator returns `false`.
- 1 — Comparison of `NULL` values in `IN` operator returns `true`.

**Example**

Consider the `null_in` table:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
│    3 │     3 │
└──────┴───────┘
```

Query:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 0;
```

Result:

``` text
┌──idx─┬────i─┐
│    1 │    1 │
└──────┴──────┘
```

Query:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 1;
```

Result:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
└──────┴───────┘
```

**See Also**

- [NULL Processing in IN Operators](../../sql-reference/operators/in.md/#in-null-processing)

## traverse_shadow_remote_data_paths {#traverse_shadow_remote_data_paths}

Type: Bool

Default value: 0

Traverse shadow directory when query system.remote_data_paths

## union_default_mode {#union_default_mode}

Type: SetOperationMode

Default value: 

Sets a mode for combining `SELECT` query results. The setting is only used when shared with [UNION](../../sql-reference/statements/select/union.md) without explicitly specifying the `UNION ALL` or `UNION DISTINCT`.

Possible values:

- `'DISTINCT'` — ClickHouse outputs rows as a result of combining queries removing duplicate rows.
- `'ALL'` — ClickHouse outputs all rows as a result of combining queries including duplicate rows.
- `''` — ClickHouse generates an exception when used with `UNION`.

See examples in [UNION](../../sql-reference/statements/select/union.md).

## unknown_packet_in_send_data {#unknown_packet_in_send_data}

Type: UInt64

Default value: 0

Send unknown packet instead of data Nth data packet

## use_cache_for_count_from_files {#use_cache_for_count_from_files}

Type: Bool

Default value: 1

Enables caching of rows number during count from files in table functions `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`.

Enabled by default.

## use_client_time_zone {#use_client_time_zone}

Type: Bool

Default value: 0

Use client timezone for interpreting DateTime string values, instead of adopting server timezone.

## use_compact_format_in_distributed_parts_names {#use_compact_format_in_distributed_parts_names}

Type: Bool

Default value: 1

Uses compact format for storing blocks for background (`distributed_foreground_insert`) INSERT into tables with `Distributed` engine.

Possible values:

- 0 — Uses `user[:password]@host:port#default_database` directory format.
- 1 — Uses `[shard{shard_index}[_replica{replica_index}]]` directory format.

:::note
- with `use_compact_format_in_distributed_parts_names=0` changes from cluster definition will not be applied for background INSERT.
- with `use_compact_format_in_distributed_parts_names=1` changing the order of the nodes in the cluster definition, will change the `shard_index`/`replica_index` so be aware.
:::

## use_concurrency_control {#use_concurrency_control}

Type: Bool

Default value: 1

Respect the server's concurrency control (see the `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores` global server settings). If disabled, it allows using a larger number of threads even if the server is overloaded (not recommended for normal usage, and needed mostly for tests).

## use_hedged_requests {#use_hedged_requests}

Type: Bool

Default value: 1

Enables hedged requests logic for remote queries. It allows to establish many connections with different replicas for query.
New connection is enabled in case existent connection(s) with replica(s) were not established within `hedged_connection_timeout`
or no data was received within `receive_data_timeout`. Query uses the first connection which send non empty progress packet (or data packet, if `allow_changing_replica_until_first_data_packet`);
other connections are cancelled. Queries with `max_parallel_replicas > 1` are supported.

Enabled by default.

Disabled by default on Cloud.

## use_hive_partitioning {#use_hive_partitioning}

Type: Bool

Default value: 0

When enabled, ClickHouse will detect Hive-style partitioning in path (`/name=value/`) in file-like table engines [File](../../engines/table-engines/special/file.md#hive-style-partitioning)/[S3](../../engines/table-engines/integrations/s3.md#hive-style-partitioning)/[URL](../../engines/table-engines/special/url.md#hive-style-partitioning)/[HDFS](../../engines/table-engines/integrations/hdfs.md#hive-style-partitioning)/[AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md#hive-style-partitioning) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.

## use_index_for_in_with_subqueries {#use_index_for_in_with_subqueries}

Type: Bool

Default value: 1

Try using an index if there is a subquery or a table expression on the right side of the IN operator.

## use_index_for_in_with_subqueries_max_values {#use_index_for_in_with_subqueries_max_values}

Type: UInt64

Default value: 0

The maximum size of the set in the right-hand side of the IN operator to use table index for filtering. It allows to avoid performance degradation and higher memory usage due to the preparation of additional data structures for large queries. Zero means no limit.

## use_json_alias_for_old_object_type {#use_json_alias_for_old_object_type}

Type: Bool

Default value: 0

When enabled, `JSON` data type alias will be used to create an old [Object('json')](../../sql-reference/data-types/json.md) type instead of the new [JSON](../../sql-reference/data-types/newjson.md) type.

## use_local_cache_for_remote_storage {#use_local_cache_for_remote_storage}

Type: Bool

Default value: 1

Use local cache for remote storage like HDFS or S3, it's used for remote table engine only

## use_page_cache_for_disks_without_file_cache {#use_page_cache_for_disks_without_file_cache}

Type: Bool

Default value: 0

Use userspace page cache for remote disks that don't have filesystem cache enabled.

## use_query_cache {#use_query_cache}

Type: Bool

Default value: 0

If turned on, `SELECT` queries may utilize the [query cache](../query-cache.md). Parameters [enable_reads_from_query_cache](#enable-reads-from-query-cache)
and [enable_writes_to_query_cache](#enable-writes-to-query-cache) control in more detail how the cache is used.

Possible values:

- 0 - Disabled
- 1 - Enabled

## use_skip_indexes {#use_skip_indexes}

Type: Bool

Default value: 1

Use data skipping indexes during query execution.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## use_skip_indexes_if_final {#use_skip_indexes_if_final}

Type: Bool

Default value: 0

Controls whether skipping indexes are used when executing a query with the FINAL modifier.

By default, this setting is disabled because skip indexes may exclude rows (granules) containing the latest data, which could lead to incorrect results. When enabled, skipping indexes are applied even with the FINAL modifier, potentially improving performance but with the risk of missing recent updates.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

## use_structure_from_insertion_table_in_table_functions {#use_structure_from_insertion_table_in_table_functions}

Type: UInt64

Default value: 2

Use structure from insertion table instead of schema inference from data. Possible values: 0 - disabled, 1 - enabled, 2 - auto

## use_uncompressed_cache {#use_uncompressed_cache}

Type: Bool

Default value: 0

Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
Using the uncompressed cache (only for tables in the MergeTree family) can significantly reduce latency and increase throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically to save space for truly small queries. This means that you can keep the ‘use_uncompressed_cache’ setting always set to 1.

## use_variant_as_common_type {#use_variant_as_common_type}

Type: Bool

Default value: 0

Allows to use `Variant` type as a result type for [if](../../sql-reference/functions/conditional-functions.md/#if)/[multiIf](../../sql-reference/functions/conditional-functions.md/#multiif)/[array](../../sql-reference/functions/array-functions.md)/[map](../../sql-reference/functions/tuple-map-functions.md) functions when there is no common type for argument types.

Example:

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(if(number % 2, number, range(number))) as variant_type FROM numbers(1);
SELECT if(number % 2, number, range(number)) as variant FROM numbers(5);
```

```text
┌─variant_type───────────────────┐
│ Variant(Array(UInt64), UInt64) │
└────────────────────────────────┘
┌─variant───┐
│ []        │
│ 1         │
│ [0,1]     │
│ 3         │
│ [0,1,2,3] │
└───────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL)) AS variant_type FROM numbers(1);
SELECT multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL) AS variant FROM numbers(4);
```

```text
─variant_type─────────────────────────┐
│ Variant(Array(UInt8), String, UInt8) │
└──────────────────────────────────────┘

┌─variant───────┐
│ 42            │
│ [1,2,3]       │
│ Hello, World! │
│ ᴺᵁᴸᴸ          │
└───────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(array(range(number), number, 'str_' || toString(number))) as array_of_variants_type from numbers(1);
SELECT array(range(number), number, 'str_' || toString(number)) as array_of_variants FROM numbers(3);
```

```text
┌─array_of_variants_type────────────────────────┐
│ Array(Variant(Array(UInt64), String, UInt64)) │
└───────────────────────────────────────────────┘

┌─array_of_variants─┐
│ [[],0,'str_0']    │
│ [[0],1,'str_1']   │
│ [[0,1],2,'str_2'] │
└───────────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(map('a', range(number), 'b', number, 'c', 'str_' || toString(number))) as map_of_variants_type from numbers(1);
SELECT map('a', range(number), 'b', number, 'c', 'str_' || toString(number)) as map_of_variants FROM numbers(3);
```

```text
┌─map_of_variants_type────────────────────────────────┐
│ Map(String, Variant(Array(UInt64), String, UInt64)) │
└─────────────────────────────────────────────────────┘

┌─map_of_variants───────────────┐
│ {'a':[],'b':0,'c':'str_0'}    │
│ {'a':[0],'b':1,'c':'str_1'}   │
│ {'a':[0,1],'b':2,'c':'str_2'} │
└───────────────────────────────┘
```

## use_with_fill_by_sorting_prefix {#use_with_fill_by_sorting_prefix}

Type: Bool

Default value: 1

Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently

## validate_polygons {#validate_polygons}

Type: Bool

Default value: 1

Enables or disables throwing an exception in the [pointInPolygon](../../sql-reference/functions/geo/index.md#pointinpolygon) function, if the polygon is self-intersecting or self-tangent.

Possible values:

- 0 — Throwing an exception is disabled. `pointInPolygon` accepts invalid polygons and returns possibly incorrect results for them.
- 1 — Throwing an exception is enabled.

## wait_changes_become_visible_after_commit_mode {#wait_changes_become_visible_after_commit_mode}

Type: TransactionsWaitCSNMode

Default value: wait_unknown

Wait for committed changes to become actually visible in the latest snapshot

## wait_for_async_insert {#wait_for_async_insert}

Type: Bool

Default value: 1

If true wait for processing of asynchronous insertion

## wait_for_async_insert_timeout {#wait_for_async_insert_timeout}

Type: Seconds

Default value: 120

Timeout for waiting for processing asynchronous insertion

## wait_for_window_view_fire_signal_timeout {#wait_for_window_view_fire_signal_timeout}

Type: Seconds

Default value: 10

Timeout for waiting for window view fire signal in event time processing

## window_view_clean_interval {#window_view_clean_interval}

Type: Seconds

Default value: 60

The clean interval of window view in seconds to free outdated data.

## window_view_heartbeat_interval {#window_view_heartbeat_interval}

Type: Seconds

Default value: 15

The heartbeat interval in seconds to indicate watch query is alive.

## workload {#workload}

Type: String

Default value: default

Name of workload to be used to access resources

## write_through_distributed_cache {#write_through_distributed_cache}

Type: Bool

Default value: 0

Only in ClickHouse Cloud. Allow writing to distributed cache (writing to s3 will also be done by distributed cache)

## zstd_window_log_max {#zstd_window_log_max}

Type: Int64

Default value: 0

Allows you to select the max window log of ZSTD (it will not be used for MergeTree family)
