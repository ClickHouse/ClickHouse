---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# 设置 {#settings}

## 分布_产品_模式 {#distributed-product-mode}

改变的行为 [分布式子查询](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

限制:

-   仅适用于IN和JOIN子查询。
-   仅当FROM部分使用包含多个分片的分布式表时。
-   如果子查询涉及包含多个分片的分布式表。
-   不用于表值 [远程](../../sql-reference/table-functions/remote.md) 功能。

可能的值:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” 例外）。
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` 查询与 `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable_optimize_predicate_expression {#enable-optimize-predicate-expression}

打开谓词下推 `SELECT` 查询。

谓词下推可以显着减少分布式查询的网络流量。

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：1。

用途

请考虑以下查询:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

如果 `enable_optimize_predicate_expression = 1`，则这些查询的执行时间相等，因为ClickHouse应用 `WHERE` 对子查询进行处理。

如果 `enable_optimize_predicate_expression = 0`，那么第二个查询的执行时间要长得多，因为 `WHERE` 子句适用于子查询完成后的所有数据。

## fallback_to_stale_replicas_for_distributed_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

如果更新的数据不可用，则强制对过期副本进行查询。 看 [复制](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse从表的过时副本中选择最相关的副本。

执行时使用 `SELECT` 从指向复制表的分布式表。

默认情况下，1（已启用）。

## force_index_by_date {#settings-force_index_by_date}

如果索引不能按日期使用，则禁用查询执行。

适用于MergeTree系列中的表。

如果 `force_index_by_date=1`，ClickHouse检查查询是否具有可用于限制数据范围的date键条件。 如果没有合适的条件，则会引发异常。 但是，它不检查条件是否减少了要读取的数据量。 例如，条件 `Date != ' 2000-01-01 '` 即使它与表中的所有数据匹配（即运行查询需要完全扫描），也是可以接受的。 有关MergeTree表中数据范围的详细信息，请参阅 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_primary_key {#force-primary-key}

如果无法按主键编制索引，则禁用查询执行。

适用于MergeTree系列中的表。

如果 `force_primary_key=1`，ClickHouse检查查询是否具有可用于限制数据范围的主键条件。 如果没有合适的条件，则会引发异常。 但是，它不检查条件是否减少了要读取的数据量。 有关MergeTree表中数据范围的详细信息，请参阅 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## format_schema {#format-schema}

当您使用需要架构定义的格式时，此参数非常有用，例如 [普罗托船长](https://capnproto.org/) 或 [Protobuf](https://developers.google.com/protocol-buffers/). 该值取决于格式。

## fsync_metadata {#fsync-metadata}

启用或禁用 [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) 写作时 `.sql` 文件 默认情况下启用。

如果服务器有数百万个不断创建和销毁的小表，那么禁用它是有意义的。

## enable_http_compression {#settings-enable_http_compression}

在对HTTP请求的响应中启用或禁用数据压缩。

欲了解更多信息，请阅读 [HTTP接口描述](../../interfaces/http.md).

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

## http_zlib_compression_level {#settings-http_zlib_compression_level}

在以下情况下，设置对HTTP请求的响应中的数据压缩级别 [enable_http_compression=1](#settings-enable_http_compression).

可能的值：数字从1到9。

默认值：3。

## http_native_compression_disable_checksumming_on_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

在从客户端解压缩HTTP POST数据时启用或禁用校验和验证。 仅用于ClickHouse原生压缩格式（不用于 `gzip` 或 `deflate`).

欲了解更多信息，请阅读 [HTTP接口描述](../../interfaces/http.md).

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

## send_progress_in_http_headers {#settings-send_progress_in_http_headers}

启用或禁用 `X-ClickHouse-Progress` Http响应头 `clickhouse-server` 答复。

欲了解更多信息，请阅读 [HTTP接口描述](../../interfaces/http.md).

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

## max_http_get_redirects {#setting-max_http_get_redirects}

限制HTTP GET重定向跳数的最大数量 [URL](../../engines/table-engines/special/url.md)-发动机表。 该设置适用于两种类型的表：由 [CREATE TABLE](../../sql-reference/statements/create.md#create-table-query) 查询和由 [url](../../sql-reference/table-functions/url.md) 表功能。

可能的值:

-   跳数的任何正整数。
-   0 — No hops allowed.

默认值：0。

## input_format_allow_errors_num {#settings-input_format_allow_errors_num}

设置从文本格式（CSV，TSV等）读取时可接受的错误的最大数量。).

默认值为0。

总是与它配对 `input_format_allow_errors_ratio`.

如果在读取行时发生错误，但错误计数器仍小于 `input_format_allow_errors_num`，ClickHouse忽略该行并移动到下一个。

如果两者 `input_format_allow_errors_num` 和 `input_format_allow_errors_ratio` 超出时，ClickHouse引发异常。

## input_format_allow_errors_ratio {#settings-input_format_allow_errors_ratio}

设置从文本格式（CSV，TSV等）读取时允许的最大错误百分比。).
错误百分比设置为介于0和1之间的浮点数。

默认值为0。

总是与它配对 `input_format_allow_errors_num`.

如果在读取行时发生错误，但错误计数器仍小于 `input_format_allow_errors_ratio`，ClickHouse忽略该行并移动到下一个。

如果两者 `input_format_allow_errors_num` 和 `input_format_allow_errors_ratio` 超出时，ClickHouse引发异常。

## input_format_values_interpret_expressions {#settings-input_format_values_interpret_expressions}

如果快速流解析器无法解析数据，则启用或禁用完整SQL解析器。 此设置仅用于 [值](../../interfaces/formats.md#data-format-values) 格式在数据插入。 有关语法分析的详细信息，请参阅 [语法](../../sql-reference/syntax.md) 科。

可能的值:

-   0 — Disabled.

    在这种情况下，您必须提供格式化的数据。 见 [格式](../../interfaces/formats.md) 科。

-   1 — Enabled.

    在这种情况下，您可以使用SQL表达式作为值，但数据插入速度要慢得多。 如果仅插入格式化的数据，则ClickHouse的行为就好像设置值为0。

默认值：1。

使用示例

插入 [日期时间](../../sql-reference/data-types/datetime.md) 使用不同的设置键入值。

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

最后一个查询等效于以下内容:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input_format_values_deduce_templates_of_expressions {#settings-input_format_values_deduce_templates_of_expressions}

启用或禁用以下内容中的SQL表达式的模板扣除 [值](../../interfaces/formats.md#data-format-values) 格式。 它允许解析和解释表达式 `Values` 如果连续行中的表达式具有相同的结构，速度要快得多。 ClickHouse尝试推导表达式的模板，使用此模板解析以下行，并在一批成功解析的行上评估表达式。

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：1。

对于以下查询:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   如果 `input_format_values_interpret_expressions=1` 和 `format_values_deduce_templates_of_expressions=0`，表达式为每行分别解释（对于大量行来说，这非常慢）。
-   如果 `input_format_values_interpret_expressions=0` 和 `format_values_deduce_templates_of_expressions=1`，第一，第二和第三行中的表达式使用template解析 `lower(String)` 并一起解释，第四行中的表达式用另一个模板解析 (`upper(String)`).
-   如果 `input_format_values_interpret_expressions=1` 和 `format_values_deduce_templates_of_expressions=1`，与前面的情况相同，但如果不可能推导出模板，也允许回退到单独解释表达式。

## input_format_values_accurate_types_of_literals {#settings-input-format-values-accurate-types-of-literals}

此设置仅在以下情况下使用 `input_format_values_deduce_templates_of_expressions = 1`. 它可能发生，某些列的表达式具有相同的结构，但包含不同类型的数字文字，例如

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

可能的值:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` 或 `Int64` 而不是 `UInt64` 为 `42`），但它可能会导致溢出和精度问题。

-   1 — Enabled.

    在这种情况下，ClickHouse会检查文本的实际类型，并使用相应类型的表达式模板。 在某些情况下，可能会显着减慢表达式评估 `Values`.

默认值：1。

## input_format_defaults_for_omitted_fields {#session_settings-input_format_defaults_for_omitted_fields}

执行时 `INSERT` 查询时，将省略的输入列值替换为相应列的默认值。 此选项仅适用于 [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) 和 [TabSeparated](../../interfaces/formats.md#tabseparated) 格式。

!!! note "注"
    启用此选项后，扩展表元数据将从服务器发送到客户端。 它会消耗服务器上的额外计算资源，并可能降低性能。

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：1。

## input_format_tsv_empty_as_default {#settings-input-format-tsv-empty-as-default}

启用后，将TSV中的空输入字段替换为默认值。 对于复杂的默认表达式 `input_format_defaults_for_omitted_fields` 必须启用了。

默认情况下禁用。

## input_format_null_as_default {#settings-input-format-null-as-default}

如果输入数据包含 `NULL`，但相应列的数据类型不 `Nullable(T)` （对于文本输入格式）。

## input_format_skip_unknown_fields {#settings-input-format-skip-unknown-fields}

启用或禁用跳过额外数据的插入。

写入数据时，如果输入数据包含目标表中不存在的列，ClickHouse将引发异常。 如果启用了跳过，ClickHouse不会插入额外的数据，也不会引发异常。

支持的格式:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

## input_format_import_nested_json {#settings-input_format_import_nested_json}

启用或禁用具有嵌套对象的JSON数据的插入。

支持的格式:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

另请参阅:

-   [嵌套结构的使用](../../interfaces/formats.md#jsoneachrow-nested) 与 `JSONEachRow` 格式。

## input_format_with_names_use_header {#settings-input-format-with-names-use-header}

启用或禁用插入数据时检查列顺序。

为了提高插入性能，如果您确定输入数据的列顺序与目标表中的列顺序相同，建议禁用此检查。

支持的格式:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：1。

## date_time_input_format {#settings-date_time_input_format}

允许选择日期和时间的文本表示的解析器。

该设置不适用于 [日期和时间功能](../../sql-reference/functions/date-time-functions.md).

可能的值:

-   `'best_effort'` — Enables extended parsing.

    ClickHouse可以解析基本 `YYYY-MM-DD HH:MM:SS` 格式和所有 [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) 日期和时间格式。 例如, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouse只能解析基本的 `YYYY-MM-DD HH:MM:SS` 格式。 例如, `'2019-08-20 10:18:56'`.

默认值: `'basic'`.

另请参阅:

-   [日期时间数据类型。](../../sql-reference/data-types/datetime.md)
-   [用于处理日期和时间的函数。](../../sql-reference/functions/date-time-functions.md)

## join_default_strictness {#settings-join_default_strictness}

设置默认严格性 [加入子句](../../sql-reference/statements/select/join.md#select-join).

可能的值:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [笛卡尔积](https://en.wikipedia.org/wiki/Cartesian_product) 从匹配的行。 这是正常的 `JOIN` 来自标准SQL的行为。
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` 和 `ALL` 都是一样的
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` 或 `ANY` 如果未在查询中指定，ClickHouse将引发异常。

默认值: `ALL`.

## join_any_take_last_row {#settings-join_any_take_last_row}

更改联接操作的行为 `ANY` 严格。

!!! warning "注意"
    此设置仅适用于 `JOIN` 操作与 [加入我们](../../engines/table-engines/special/join.md) 发动机表.

可能的值:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

默认值：0。

另请参阅:

-   [JOIN子句](../../sql-reference/statements/select/join.md#select-join)
-   [联接表引擎](../../engines/table-engines/special/join.md)
-   [join_default_strictness](#settings-join_default_strictness)

## join_use_nulls {#join_use_nulls}

设置类型 [JOIN](../../sql-reference/statements/select/join.md) 行为 合并表时，可能会出现空单元格。 ClickHouse根据此设置以不同的方式填充它们。

可能的值:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` 其行为方式与标准SQL中的行为方式相同。 相应字段的类型将转换为 [可为空](../../sql-reference/data-types/nullable.md#data_type-nullable)，和空单元格填充 [NULL](../../sql-reference/syntax.md).

默认值：0。

## max_block_size {#setting-max_block_size}

在ClickHouse中，数据由块（列部分集）处理。 单个块的内部处理周期足够高效，但每个块都有明显的支出。 该 `max_block_size` 设置是建议从表中加载块的大小（行数）。 块大小不应该太小，以便每个块上的支出仍然明显，但不能太大，以便在第一个块处理完成后快速完成限制查询。 目标是避免在多个线程中提取大量列时占用太多内存，并且至少保留一些缓存局部性。

默认值：65,536。

块的大小 `max_block_size` 并不总是从表中加载。 如果显然需要检索的数据较少，则处理较小的块。

## preferred_block_size_bytes {#preferred-block-size-bytes}

用于相同的目的 `max_block_size`，但它通过使其适应块中的行数来设置推荐的块大小（以字节为单位）。
但是，块大小不能超过 `max_block_size` 行。
默认情况下：1,000,000。 它只有在从MergeTree引擎读取时才有效。

## merge_tree_min_rows_for_concurrent_read {#setting-merge-tree-min-rows-for-concurrent-read}

如果从a的文件中读取的行数 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表超过 `merge_tree_min_rows_for_concurrent_read` 然后ClickHouse尝试在多个线程上从该文件执行并发读取。

可能的值:

-   任何正整数。

默认值:163840.

## merge_tree_min_bytes_for_concurrent_read {#setting-merge-tree-min-bytes-for-concurrent-read}

如果从一个文件中读取的字节数 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-发动机表超过 `merge_tree_min_bytes_for_concurrent_read`，然后ClickHouse尝试在多个线程中并发读取此文件。

可能的值:

-   任何正整数。

默认值:251658240.

## merge_tree_min_rows_for_seek {#setting-merge-tree-min-rows-for-seek}

如果要在一个文件中读取的两个数据块之间的距离小于 `merge_tree_min_rows_for_seek` 行，然后ClickHouse不查找文件，而是按顺序读取数据。

可能的值:

-   任何正整数。

默认值：0。

## merge_tree_min_bytes_for_seek {#setting-merge-tree-min-bytes-for-seek}

如果要在一个文件中读取的两个数据块之间的距离小于 `merge_tree_min_bytes_for_seek` 字节数，然后ClickHouse依次读取包含两个块的文件范围，从而避免了额外的寻道。

可能的值:

-   任何正整数。

默认值：0。

## merge_tree_coarse_index_granularity {#setting-merge-tree-coarse-index-granularity}

搜索数据时，ClickHouse会检查索引文件中的数据标记。 如果ClickHouse发现所需的键在某个范围内，它将此范围划分为 `merge_tree_coarse_index_granularity` 子范围和递归地搜索所需的键。

可能的值:

-   任何正偶数整数。

默认值：8。

## merge_tree_max_rows_to_use_cache {#setting-merge-tree-max-rows-to-use-cache}

如果克里克豪斯应该阅读更多 `merge_tree_max_rows_to_use_cache` 在一个查询中的行，它不使用未压缩块的缓存。

未压缩块的缓存存储为查询提取的数据。 ClickHouse使用此缓存来加快对重复的小查询的响应。 此设置可保护缓存免受读取大量数据的查询的破坏。 该 [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) 服务器设置定义未压缩块的高速缓存的大小。

可能的值:

-   任何正整数。

Default value: 128 ✕ 8192.

## merge_tree_max_bytes_to_use_cache {#setting-merge-tree-max-bytes-to-use-cache}

如果克里克豪斯应该阅读更多 `merge_tree_max_bytes_to_use_cache` 在一个查询中的字节，它不使用未压缩块的缓存。

未压缩块的缓存存储为查询提取的数据。 ClickHouse使用此缓存来加快对重复的小查询的响应。 此设置可保护缓存免受读取大量数据的查询的破坏。 该 [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) 服务器设置定义未压缩块的高速缓存的大小。

可能的值:

-   任何正整数。

默认值:2013265920.

## min_bytes_to_use_direct_io {#settings-min-bytes-to-use-direct-io}

使用直接I/O访问存储磁盘所需的最小数据量。

ClickHouse在从表中读取数据时使用此设置。 如果要读取的所有数据的总存储量超过 `min_bytes_to_use_direct_io` 字节，然后ClickHouse读取从存储磁盘的数据 `O_DIRECT` 选项。

可能的值:

-   0 — Direct I/O is disabled.
-   整数。

默认值：0。

## log_queries {#settings-log-queries}

设置查询日志记录。

使用此设置发送到ClickHouse的查询将根据以下内容中的规则记录 [query_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 服务器配置参数。

示例:

``` text
log_queries=1
```

## log_queries_min_type {#settings-log-queries-min-type}

`query_log` 要记录的最小类型。

可能的值:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

默认值: `QUERY_START`.

可以用来限制哪些entiries将去 `query_log`，说你只有在错误中才感兴趣，那么你可以使用 `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log_query_threads {#settings-log-query-threads}

设置查询线程日志记录。

ClickHouse使用此设置运行的查询线程将根据以下命令中的规则记录 [query_thread_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) 服务器配置参数。

示例:

``` text
log_query_threads=1
```

## max_insert_block_size {#settings-max_insert_block_size}

要插入到表中的块的大小。
此设置仅适用于服务器形成块的情况。
例如，对于通过HTTP接口进行的插入，服务器会分析数据格式并形成指定大小的块。
但是当使用clickhouse-client时，客户端解析数据本身，并且 ‘max_insert_block_size’ 服务器上的设置不会影响插入的块的大小。
使用INSERT SELECT时，该设置也没有目的，因为数据是使用在SELECT之后形成的相同块插入的。

默认值：1,048,576。

默认值略高于 `max_block_size`. 这样做的原因是因为某些表引擎 (`*MergeTree`）在磁盘上为每个插入的块形成一个数据部分，这是一个相当大的实体。 同样, `*MergeTree` 表在插入过程中对数据进行排序，并且足够大的块大小允许在RAM中对更多数据进行排序。

## min_insert_block_size_rows {#min-insert-block-size-rows}

设置块中可以通过以下方式插入到表中的最小行数 `INSERT` 查询。 较小尺寸的块被压扁成较大的块。

可能的值:

-   整数。
-   0 — Squashing disabled.

默认值：1048576。

## min_insert_block_size_bytes {#min-insert-block-size-bytes}

设置块中的最小字节数，可以通过以下方式插入到表中 `INSERT` 查询。 较小尺寸的块被压扁成较大的块。

可能的值:

-   整数。
-   0 — Squashing disabled.

默认值:268435456.

## max_replica_delay_for_distributed_queries {#settings-max_replica_delay_for_distributed_queries}

禁用分布式查询的滞后副本。 看 [复制](../../engines/table-engines/mergetree-family/replication.md).

以秒为单位设置时间。 如果副本滞后超过设定值，则不使用此副本。

默认值：300。

执行时使用 `SELECT` 从指向复制表的分布式表。

## max_threads {#settings-max_threads}

查询处理线程的最大数量，不包括用于从远程服务器检索数据的线程（请参阅 ‘max_distributed_connections’ 参数）。

此参数适用于并行执行查询处理管道的相同阶段的线程。
例如，当从表中读取时，如果可以使用函数来评估表达式，请使用WHERE进行过滤，并且至少使用并行方式对GROUP BY进行预聚合 ‘max_threads’ 线程数，然后 ‘max_threads’ 被使用。

默认值：物理CPU内核数。

如果一次在服务器上运行的SELECT查询通常少于一个，请将此参数设置为略小于实际处理器内核数的值。

对于由于限制而快速完成的查询，可以设置较低的 ‘max_threads’. 例如，如果必要数量的条目位于每个块中，并且max_threads=8，则会检索8个块，尽管只读取一个块就足够了。

越小 `max_threads` 值，较少的内存被消耗。

## max_insert_threads {#settings-max-insert-threads}

要执行的最大线程数 `INSERT SELECT` 查询。

可能的值:

-   0 (or 1) — `INSERT SELECT` 没有并行执行。
-   整数。 大于1。

默认值：0。

平行 `INSERT SELECT` 只有在 `SELECT` 部分并行执行，请参阅 [max_threads](#settings-max_threads) 设置。
更高的值将导致更高的内存使用率。

## max_compress_block_size {#max-compress-block-size}

在压缩写入表之前，未压缩数据块的最大大小。 默认情况下，1,048,576（1MiB）。 如果大小减小，则压缩率显着降低，压缩和解压缩速度由于高速缓存局部性而略微增加，并且内存消耗减少。 通常没有任何理由更改此设置。

不要将用于压缩的块（由字节组成的内存块）与用于查询处理的块（表中的一组行）混淆。

## min_compress_block_size {#min-compress-block-size}

为 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)"表。 为了减少处理查询时的延迟，在写入下一个标记时，如果块的大小至少为 ‘min_compress_block_size’. 默认情况下，65,536。

块的实际大小，如果未压缩的数据小于 ‘max_compress_block_size’，是不小于该值且不小于一个标记的数据量。

让我们来看看一个例子。 假设 ‘index_granularity’ 在表创建期间设置为8192。

我们正在编写一个UInt32类型的列（每个值4个字节）。 当写入8192行时，总数将是32KB的数据。 由于min_compress_block_size=65,536，将为每两个标记形成一个压缩块。

我们正在编写一个字符串类型的URL列（每个值的平均大小60字节）。 当写入8192行时，平均数据将略少于500KB。 由于这超过65,536，将为每个标记形成一个压缩块。 在这种情况下，当从单个标记范围内的磁盘读取数据时，额外的数据不会被解压缩。

通常没有任何理由更改此设置。

## max_query_size {#settings-max_query_size}

查询的最大部分，可以被带到RAM用于使用SQL解析器进行解析。
插入查询还包含由单独的流解析器（消耗O(1)RAM）处理的插入数据，这些数据不包含在此限制中。

默认值：256KiB。

## interactive_delay {#interactive-delay}

以微秒为单位的间隔，用于检查请求执行是否已被取消并发送进度。

默认值：100,000（检查取消并每秒发送十次进度）。

## connect_timeout,receive_timeout,send_timeout {#connect-timeout-receive-timeout-send-timeout}

用于与客户端通信的套接字上的超时以秒为单位。

默认值：10，300，300。

## cancel_http_readonly_queries_on_client_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

默认值：0

## poll_interval {#poll-interval}

锁定在指定秒数的等待循环。

默认值：10。

## max_distributed_connections {#max-distributed-connections}

与远程服务器同时连接的最大数量，用于分布式处理对单个分布式表的单个查询。 我们建议设置不小于群集中服务器数量的值。

默认值：1024。

以下参数仅在创建分布式表（以及启动服务器时）时使用，因此没有理由在运行时更改它们。

## distributed_connections_pool_size {#distributed-connections-pool-size}

与远程服务器同时连接的最大数量，用于分布式处理对单个分布式表的所有查询。 我们建议设置不小于群集中服务器数量的值。

默认值：1024。

## connect_timeout_with_failover_ms {#connect-timeout-with-failover-ms}

以毫秒为单位连接到分布式表引擎的远程服务器的超时，如果 ‘shard’ 和 ‘replica’ 部分用于群集定义。
如果不成功，将尝试多次连接到各种副本。

默认值：50。

## connections_with_failover_max_tries {#connections-with-failover-max-tries}

分布式表引擎的每个副本的最大连接尝试次数。

默认值：3。

## 极端 {#extremes}

是否计算极值（查询结果列中的最小值和最大值）。 接受0或1。 默认情况下，0（禁用）。
有关详细信息，请参阅部分 “Extreme values”.

## use_uncompressed_cache {#setting-use_uncompressed_cache}

是否使用未压缩块的缓存。 接受0或1。 默认情况下，0（禁用）。
使用未压缩缓存（仅适用于MergeTree系列中的表）可以在处理大量短查询时显着减少延迟并提高吞吐量。 为频繁发送短请求的用户启用此设置。 还要注意 [uncompressed_cache_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

对于至少读取大量数据（一百万行或更多行）的查询，将自动禁用未压缩缓存，以节省真正小型查询的空间。 这意味着你可以保持 ‘use_uncompressed_cache’ 设置始终设置为1。

## replace_running_query {#replace-running-query}

当使用HTTP接口时， ‘query_id’ 参数可以传递。 这是用作查询标识符的任何字符串。
如果来自同一用户的查询具有相同的 ‘query_id’ 已经存在在这个时候，行为取决于 ‘replace_running_query’ 参数。

`0` (default) – Throw an exception (don't allow the query to run if a query with the same ‘query_id’ 已经运行）。

`1` – Cancel the old query and start running the new one.

YandexMetrica使用此参数设置为1来实现分段条件的建议。 输入下一个字符后，如果旧的查询还没有完成，应该取消。

## stream_flush_interval_ms {#stream-flush-interval-ms}

适用于在超时的情况下或线程生成流式传输的表 [max_insert_block_size](#settings-max_insert_block_size) 行。

默认值为7500。

值越小，数据被刷新到表中的频率就越高。 将该值设置得太低会导致性能较差。

## load_balancing {#settings-load_balancing}

指定用于分布式查询处理的副本选择算法。

ClickHouse支持以下选择副本的算法:

-   [随机](#load_balancing-random) （默认情况下)
-   [最近的主机名](#load_balancing-nearest_hostname)
-   [按顺序](#load_balancing-in_order)
-   [第一次或随机](#load_balancing-first_or_random)

### 随机（默认情况下) {#load_balancing-random}

``` sql
load_balancing = random
```

对每个副本计算错误数。 查询发送到错误最少的副本，如果存在其中几个错误，则发送给其中任何一个。
缺点：不考虑服务器邻近度；如果副本具有不同的数据，则也会获得不同的数据。

### 最近的主机名 {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

例如，例如01-01-1和example01-01-2.yandex.ru 在一个位置是不同的，而example01-01-1和example01-02-2在两个地方不同。
这种方法可能看起来很原始，但它不需要有关网络拓扑的外部数据，也不比较IP地址，这对于我们的IPv6地址来说会很复杂。

因此，如果存在等效副本，则首选按名称最接近的副本。
我们还可以假设，当向同一台服务器发送查询时，在没有失败的情况下，分布式查询也将转到同一台服务器。 因此，即使在副本上放置了不同的数据，查询也会返回大多相同的结果。

### 按顺序 {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

具有相同错误数的副本的访问顺序与配置中指定的顺序相同。
当您确切知道哪个副本是可取的时，此方法是适当的。

### 第一次或随机 {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

此算法选择集合中的第一个副本，如果第一个副本不可用，则选择随机副本。 它在跨复制拓扑设置中有效，但在其他配置中无用。

该 `first_or_random` 算法解决的问题 `in_order` 算法。 与 `in_order`，如果一个副本出现故障，下一个副本将获得双重负载，而其余副本将处理通常的流量。 使用时 `first_or_random` 算法中，负载均匀分布在仍然可用的副本之间。

## prefer_localhost_replica {#settings-prefer-localhost-replica}

在处理分布式查询时，最好使用localhost副本启用/禁用该副本。

可能的值:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#settings-load_balancing) 设置。

默认值：1。

!!! warning "警告"
    如果使用此设置，请禁用此设置 [max_parallel_replicas](#settings-max_parallel_replicas).

## totals_mode {#totals-mode}

如何计算总计时有存在，以及当max_rows_to_group_by和group_by_overflow_mode= ‘any’ 都在场。
请参阅部分 “WITH TOTALS modifier”.

## totals_auto_threshold {#totals-auto-threshold}

阈值 `totals_mode = 'auto'`.
请参阅部分 “WITH TOTALS modifier”.

## max_parallel_replicas {#settings-max_parallel_replicas}

执行查询时每个分片的最大副本数。
为了保持一致性（以获取相同数据拆分的不同部分），此选项仅在设置了采样键时有效。
副本滞后不受控制。

## 编译 {#compile}

启用查询的编译。 默认情况下，0（禁用）。

编译仅用于查询处理管道的一部分：用于聚合的第一阶段（GROUP BY）。
如果编译了管道的这一部分，则由于部署周期较短和内联聚合函数调用，查询可能运行得更快。 对于具有多个简单聚合函数的查询，可以看到最大的性能改进（在极少数情况下可快四倍）。 通常，性能增益是微不足道的。 在极少数情况下，它可能会减慢查询执行速度。

## min_count_to_compile {#min-count-to-compile}

在运行编译之前可能使用已编译代码块的次数。 默认情况下，3。
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
如果该值为1或更大，则编译在单独的线程中异步进行。 结果将在准备就绪后立即使用，包括当前正在运行的查询。

对于查询中使用的聚合函数的每个不同组合以及GROUP BY子句中的键类型，都需要编译代码。
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don't use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output_format_json_quote_64bit_integers {#session_settings-output_format_json_quote_64bit_integers}

如果该值为true，则在使用JSON\*Int64和UInt64格式时，整数将显示在引号中（为了与大多数JavaScript实现兼容）；否则，整数将不带引号输出。

## format_csv_delimiter {#settings-format_csv_delimiter}

将字符解释为CSV数据中的分隔符。 默认情况下，分隔符为 `,`.

## input_format_csv_unquoted_null_literal_as_null {#settings-input_format_csv_unquoted_null_literal_as_null}

对于CSV输入格式，启用或禁用未引用的解析 `NULL` 作为文字（同义词 `\N`).

## output_format_csv_crlf_end_of_line {#settings-output-format-csv-crlf-end-of-line}

在CSV中使用DOS/Windows样式的行分隔符(CRLF)而不是Unix样式(LF)。

## output_format_tsv_crlf_end_of_line {#settings-output-format-tsv-crlf-end-of-line}

在TSV中使用DOC/Windows样式的行分隔符（CRLF）而不是Unix样式（LF）。

## insert_quorum {#settings-insert_quorum}

启用仲裁写入。

-   如果 `insert_quorum < 2`，仲裁写入被禁用。
-   如果 `insert_quorum >= 2`，仲裁写入已启用。

默认值：0。

仲裁写入

`INSERT` 只有当ClickHouse设法正确地将数据写入成功 `insert_quorum` 在复制品的 `insert_quorum_timeout`. 如果由于任何原因，成功写入的副本数量没有达到 `insert_quorum`，写入被认为失败，ClickHouse将从已经写入数据的所有副本中删除插入的块。

仲裁中的所有副本都是一致的，即它们包含来自所有以前的数据 `INSERT` 查询。 该 `INSERT` 序列线性化。

当读取从写入的数据 `insert_quorum`，您可以使用 [select_sequential_consistency](#settings-select_sequential_consistency) 选项。

ClickHouse生成异常

-   如果查询时可用副本的数量小于 `insert_quorum`.
-   在尝试写入数据时，以前的块尚未被插入 `insert_quorum` 的复制品。 如果用户尝试执行 `INSERT` 前一个与 `insert_quorum` 完成。

另请参阅:

-   [insert_quorum_timeout](#settings-insert_quorum_timeout)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## insert_quorum_timeout {#settings-insert_quorum_timeout}

写入仲裁超时以秒为单位。 如果超时已经过去，并且还没有发生写入，ClickHouse将生成异常，客户端必须重复查询以将相同的块写入相同的副本或任何其他副本。

默认值：60秒。

另请参阅:

-   [insert_quorum](#settings-insert_quorum)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## select_sequential_consistency {#settings-select_sequential_consistency}

启用或禁用顺序一致性 `SELECT` 查询:

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：0。

用途

当启用顺序一致性时，ClickHouse允许客户端执行 `SELECT` 仅查询那些包含来自所有先前数据的副本 `INSERT` 查询执行 `insert_quorum`. 如果客户端引用了部分副本，ClickHouse将生成异常。 SELECT查询将不包括尚未写入副本仲裁的数据。

另请参阅:

-   [insert_quorum](#settings-insert_quorum)
-   [insert_quorum_timeout](#settings-insert_quorum_timeout)

## insert_deduplicate {#settings-insert-deduplicate}

启用或禁用块重复数据删除 `INSERT` （对于复制的\*表）。

可能的值:

-   0 — Disabled.
-   1 — Enabled.

默认值：1。

默认情况下，块插入到复制的表 `INSERT` 语句重复数据删除（见 [数据复制](../../engines/table-engines/mergetree-family/replication.md)).

## deduplicate_blocks_in_dependent_materialized_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

启用或禁用从已复制\*表接收数据的实例化视图的重复数据删除检查。

可能的值:

      0 — Disabled.
      1 — Enabled.

默认值：0。

用途

默认情况下，重复数据删除不对实例化视图执行，而是在源表的上游执行。
如果由于源表中的重复数据删除而跳过了插入的块，则不会插入附加的实例化视图。 这种行为的存在是为了允许将高度聚合的数据插入到实例化视图中，对于在实例化视图聚合之后插入的块相同，但是从源表中的不同插入派生的情况。
与此同时，这种行为 “breaks” `INSERT` 幂等性 如果一个 `INSERT` 进入主表是成功的， `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won't receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` 允许改变这种行为。 重试时，实例化视图将收到重复插入，并自行执行重复数据删除检查,
忽略源表的检查结果，并将插入由于第一次失败而丢失的行。

## max_network_bytes {#settings-max-network-bytes}

限制在执行查询时通过网络接收或传输的数据量（以字节为单位）。 此设置适用于每个单独的查询。

可能的值:

-   整数。
-   0 — Data volume control is disabled.

默认值：0。

## max_network_bandwidth {#settings-max-network-bandwidth}

限制通过网络进行数据交换的速度，以每秒字节为单位。 此设置适用于每个查询。

可能的值:

-   整数。
-   0 — Bandwidth control is disabled.

默认值：0。

## max_network_bandwidth_for_user {#settings-max-network-bandwidth-for-user}

限制通过网络进行数据交换的速度，以每秒字节为单位。 此设置适用于单个用户执行的所有并发运行的查询。

可能的值:

-   整数。
-   0 — Control of the data speed is disabled.

默认值：0。

## max_network_bandwidth_for_all_users {#settings-max-network-bandwidth-for-all-users}

限制通过网络交换数据的速度，以每秒字节为单位。 此设置适用于服务器上同时运行的所有查询。

可能的值:

-   整数。
-   0 — Control of the data speed is disabled.

默认值：0。

## count_distinct_implementation {#settings-count_distinct_implementation}

指定其中的 `uniq*` 函数应用于执行 [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) 建筑。

可能的值:

-   [uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

默认值: `uniqExact`.

## skip_unavailable_shards {#settings-skip_unavailable_shards}

启用或禁用静默跳过不可用分片。

如果分片的所有副本都不可用，则视为不可用。 副本在以下情况下不可用:

-   ClickHouse出于任何原因无法连接到副本。

    连接到副本时，ClickHouse会执行多次尝试。 如果所有这些尝试都失败，则认为副本不可用。

-   副本无法通过DNS解析。

    如果无法通过DNS解析副本的主机名，则可能指示以下情况:

    -   副本的主机没有DNS记录。 它可以发生在具有动态DNS的系统中，例如, [Kubernetes](https://kubernetes.io)，其中节点在停机期间可能无法解决问题，这不是错误。

    -   配置错误。 ClickHouse配置文件包含错误的主机名。

可能的值:

-   1 — skipping enabled.

    如果分片不可用，ClickHouse将基于部分数据返回结果，并且不报告节点可用性问题。

-   0 — skipping disabled.

    如果分片不可用，ClickHouse将引发异常。

默认值：0。

## optimize_skip_unused_shards {#settings-optimize_skip_unused_shards}

对于在PREWHERE/WHERE中具有分片键条件的SELECT查询，启用或禁用跳过未使用的分片（假定数据是通过分片键分发的，否则不执行任何操作）。

默认值：0

## force_optimize_skip_unused_shards {#settings-force_optimize_skip_unused_shards}

在以下情况下启用或禁用查询执行 [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) 无法启用和跳过未使用的分片。 如果跳过是不可能的，并且设置为启用异常将被抛出。

可能的值:

-   0-禁用（不抛出)
-   1-仅当表具有分片键时禁用查询执行
-   2-无论为表定义了分片键，都禁用查询执行

默认值：0

## optimize_throw_if_noop {#setting-optimize_throw_if_noop}

启用或禁用抛出异常，如果 [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) 查询未执行合并。

默认情况下, `OPTIMIZE` 即使它没有做任何事情，也会成功返回。 此设置允许您区分这些情况并在异常消息中获取原因。

可能的值:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

默认值：0。

## distributed_replica_error_half_life {#settings-distributed_replica_error_half_life}

-   类型：秒
-   默认值：60秒

控制清零分布式表中的错误的速度。 如果某个副本在一段时间内不可用，累计出现5个错误，并且distributed_replica_error_half_life设置为1秒，则该副本在上一个错误发生3秒后视为正常。

另请参阅:

-   [表引擎分布式](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap](#settings-distributed_replica_error_cap)

## distributed_replica_error_cap {#settings-distributed_replica_error_cap}

-   类型：无符号int
-   默认值：1000

每个副本的错误计数上限为此值，从而防止单个副本累积太多错误。

另请参阅:

-   [表引擎分布式](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_half_life](#settings-distributed_replica_error_half_life)

## distributed_directory_monitor_sleep_time_ms {#distributed_directory_monitor_sleep_time_ms}

对于基本间隔 [分布](../../engines/table-engines/special/distributed.md) 表引擎发送数据。 在发生错误时，实际间隔呈指数级增长。

可能的值:

-   毫秒的正整数。

默认值：100毫秒。

## distributed_directory_monitor_max_sleep_time_ms {#distributed_directory_monitor_max_sleep_time_ms}

的最大间隔 [分布](../../engines/table-engines/special/distributed.md) 表引擎发送数据。 限制在设置的区间的指数增长 [distributed_directory_monitor_sleep_time_ms](#distributed_directory_monitor_sleep_time_ms) 设置。

可能的值:

-   毫秒的正整数。

默认值：30000毫秒（30秒）。

## distributed_directory_monitor_batch_inserts {#distributed_directory_monitor_batch_inserts}

启用/禁用批量发送插入的数据。

当批量发送被启用时， [分布](../../engines/table-engines/special/distributed.md) 表引擎尝试在一个操作中发送插入数据的多个文件，而不是单独发送它们。 批量发送通过更好地利用服务器和网络资源来提高集群性能。

可能的值:

-   1 — Enabled.
-   0 — Disabled.

默认值：0。

## os_thread_priority {#setting-os-thread-priority}

设置优先级 ([不错](https://en.wikipedia.org/wiki/Nice_(Unix))）对于执行查询的线程。 当选择要在每个可用CPU内核上运行的下一个线程时，操作系统调度程序会考虑此优先级。

!!! warning "警告"
    要使用此设置，您需要设置 `CAP_SYS_NICE` 能力。 该 `clickhouse-server` 软件包在安装过程中设置它。 某些虚拟环境不允许您设置 `CAP_SYS_NICE` 能力。 在这种情况下, `clickhouse-server` 在开始时显示关于它的消息。

可能的值:

-   您可以在范围内设置值 `[-20, 19]`.

值越低意味着优先级越高。 低螺纹 `nice` 与具有高值的线程相比，优先级值的执行频率更高。 高值对于长时间运行的非交互式查询更为可取，因为这使得它们可以在到达时快速放弃资源，转而使用短交互式查询。

默认值：0。

## query_profiler_real_time_period_ns {#query_profiler_real_time_period_ns}

设置周期的实时时钟定时器 [查询探查器](../../operations/optimizing-performance/sampling-query-profiler.md). 真正的时钟计时器计数挂钟时间。

可能的值:

-   正整数，以纳秒为单位。

    推荐值:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0用于关闭计时器。

类型: [UInt64](../../sql-reference/data-types/int-uint.md).

默认值：1000000000纳秒（每秒一次）。

另请参阅:

-   系统表 [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## query_profiler_cpu_time_period_ns {#query_profiler_cpu_time_period_ns}

设置周期的CPU时钟定时器 [查询探查器](../../operations/optimizing-performance/sampling-query-profiler.md). 此计时器仅计算CPU时间。

可能的值:

-   纳秒的正整数。

    推荐值:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0用于关闭计时器。

类型: [UInt64](../../sql-reference/data-types/int-uint.md).

默认值：1000000000纳秒。

另请参阅:

-   系统表 [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## allow_introspection_functions {#settings-allow_introspection_functions}

启用禁用 [反省函数](../../sql-reference/functions/introspection.md) 用于查询分析。

可能的值:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

默认值：0。

**另请参阅**

-   [采样查询探查器](../optimizing-performance/sampling-query-profiler.md)
-   系统表 [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## input_format_parallel_parsing {#input-format-parallel-parsing}

-   类型：布尔
-   默认值：True

启用数据格式的保序并行分析。 仅支持TSV，TKSV，CSV和JSONEachRow格式。

## min_chunk_bytes_for_parallel_parsing {#min-chunk-bytes-for-parallel-parsing}

-   类型：无符号int
-   默认值：1MiB

以字节为单位的最小块大小，每个线程将并行解析。

## output_format_avro_codec {#settings-output_format_avro_codec}

设置用于输出Avro文件的压缩编解ec。

类型：字符串

可能的值:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [活泼的](https://google.github.io/snappy/)

默认值: `snappy` （如果可用）或 `deflate`.

## output_format_avro_sync_interval {#settings-output_format_avro_sync_interval}

设置输出Avro文件的同步标记之间的最小数据大小（以字节为单位）。

类型：无符号int

可能的值：32（32字节）-1073741824（1GiB)

默认值：32768（32KiB)

## format_avro_schema_registry_url {#settings-format_avro_schema_registry_url}

设置要与之一起使用的汇合架构注册表URL [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) 格式

类型：网址

默认值：空

## background_pool_size {#background_pool_size}

设置在表引擎中执行后台操作的线程数（例如，合并 [MergeTree引擎](../../engines/table-engines/mergetree-family/index.md) 表）。 此设置在ClickHouse服务器启动时应用，不能在用户会话中更改。 通过调整此设置，您可以管理CPU和磁盘负载。 较小的池大小使用较少的CPU和磁盘资源，但后台进程推进速度较慢，最终可能会影响查询性能。

可能的值:

-   任何正整数。

默认值：16。

[原始文章](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->

## transform_null_in {#transform_null_in}

为[IN](../../sql-reference/operators/in.md) 运算符启用[NULL](../../sql-reference/syntax.md#null-literal) 值的相等性。

默认情况下，无法比较 `NULL` 值，因为 `NULL` 表示未定义的值。 因此，比较 `expr = NULL` 必须始终返回 `false`。 在此设置下，`NULL = NULL` 为IN运算符返回 `true`.

可能的值：

-   0 — 比较 `IN` 运算符中 `NULL` 值将返回 `false`。
-   1 — 比较 `IN` 运算符中 `NULL` 值将返回 `true`。

默认值：0。

**例**

考虑`null_in`表：

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
│    3 │     3 │
└──────┴───────┘
```

查询:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 0;
```

结果:

``` text
┌──idx─┬────i─┐
│    1 │    1 │
└──────┴──────┘
```

查询:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 1;
```

结果:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
└──────┴───────┘
```

**另请参阅**

-   [IN 运算符中的 NULL 处理](../../sql-reference/operators/in.md#in-null-processing)
