---
description: 'CHECK TABLE 文档'
sidebar_label: 'CHECK TABLE'
sidebar_position: 41
slug: /sql-reference/statements/check-table
title: 'CHECK TABLE 语句'
doc_type: 'reference'
---

ClickHouse 中的 `CHECK TABLE` 查询用于对特定表或其分区执行校验。它通过验证校验和及其他内部数据结构来确保数据完整性。

具体而言，它会将实际文件大小与 server 上存储的预期值进行比较。如果文件大小与存储值不一致，则表示数据已损坏。例如，这种情况可能是由查询执行期间发生的系统崩溃引起的。

:::warning
`CHECK TABLE` 查询可能会读取表中的全部数据并占用一定资源，因此资源开销较大。
执行此查询前，请务必考虑其对性能和资源利用率的潜在影响。
此查询不会提升系统性能；如果你不确定自己在做什么，则不应执行它。
:::

<div id="syntax">
  ## 语法
</div>

查询的基本语法如下：

```sql
CHECK TABLE table_name [PARTITION partition_expression | PART part_name] [FORMAT format] [SETTINGS check_query_single_value_result = (0|1) [, other_settings]]
```

* `table_name`：指定要检查的表名。
* `partition_expression`： (可选) 如果要检查表的特定分区，可以使用此表达式指定分区。
* `part_name`： (可选) 如果要检查表中的特定数据分区分片，可以添加字符串字面量来指定分片名称。
* `FORMAT format`： (可选) 允许指定结果的输出格式。
* `SETTINGS`： (可选) 允许设置其他参数。
  *  (可选) ：[check&#95;query&#95;single&#95;value&#95;result](../../operations/settings/settings#check_query_single_value_result)：此设置控制输出为详细信息 (`0`) 还是摘要结果 (`1`) 。
  * 也可以应用其他设置。如果不要求结果顺序具有确定性，可以将 max&#95;threads 设置为大于 1 的值，以加快查询速度。

查询响应取决于 `check_query_single_value_result` 设置的值。
当 `check_query_single_value_result = 1` 时，只会返回一个仅包含单行的 `result` 列。如果完整性检查通过，则该行中的值为 `1`；如果数据已损坏，则为 `0`。

当 `check_query_single_value_result = 0` 时，查询会返回以下列：

* `part_path`：表示数据分区分片的路径或文件名。
  * `is_passed`：如果此分片的检查成功，则返回 1，否则返回 0。
  * `message`：与检查相关的其他消息，例如错误信息或成功消息。

`CHECK TABLE` 查询支持以下表引擎：

* [Log](../../engines/table-engines/log-family/log.md)
* [TinyLog](../../engines/table-engines/log-family/tinylog.md)
* [StripeLog](../../engines/table-engines/log-family/stripelog.md)
* [MergeTree 家族](../../engines/table-engines/mergetree-family/mergetree.md)

对使用其他表引擎的表执行该查询时，会抛出 `NOT_IMPLEMENTED` 异常。

`*Log` 家族的引擎在发生故障时不提供自动数据恢复。请使用 `CHECK TABLE` 查询及时发现数据丢失。

<div id="examples">
  ## 示例
</div>

默认情况下，`CHECK TABLE` 查询会显示表检查的整体状态：

```sql title="Query"
CHECK TABLE test_table;
```

```text title="Response"
┌─result─┐
│      1 │
└────────┘
```

如果你想查看每个数据分区片段各自的检查状态，可以使用 `check_query_single_value_result` 设置。

此外，如需检查表中的特定分区，可以使用 `PARTITION` 关键字。

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
│ 201003_3_3_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

类似地，你也可以使用 `PART` 关键字来检查表中的特定分片。

```sql title="Query"
CHECK TABLE t0 PART '201003_7_7_0'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message─┐
│ 201003_7_7_0 │         1 │         │
└──────────────┴───────────┴─────────┘
```

请注意，当分片不存在时，查询会返回错误：

```sql title="Query"
CHECK TABLE t0 PART '201003_111_222_0'
```

```text title="Response"
DB::Exception: No such data part '201003_111_222_0' to check in table 'default.t0'. (NO_SUCH_DATA_PART)
```

<div id="receiving-a-corrupted-result">
  ### 返回 &#39;损坏&#39; 结果时
</div>

:::warning
免责声明：此处描述的操作流程，包括直接在数据目录中手动操作或删除文件，仅适用于实验或开发环境。**请勿**在生产服务器上尝试这样做，否则可能导致数据丢失或其他意外后果。
:::

删除现有的校验和文件：

```bash
rm /var/lib/clickhouse-server/data/default/t0/201003_3_3_0/checksums.txt
```

```sql title="Query"
CHECK TABLE t0 PARTITION ID '201003'
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text title="Response"
┌─part_path────┬─is_passed─┬─message──────────────────────────────────┐
│ 201003_7_7_0 │         1 │                                          │
│ 201003_3_3_0 │         1 │ Checksums recounted and written to disk. │
└──────────────┴───────────┴──────────────────────────────────────────┘
```

如果 `checksums.txt` 文件丢失，可以恢复。对特定分区执行 `CHECK TABLE` 命令时，系统会重新计算并重写该文件，其状态仍会显示为 &#39;is&#95;passed = 1&#39;。

你可以使用 `CHECK ALL TABLES` 查询，一次性检查所有现有的 `(Replicated)MergeTree` 表。

```sql
CHECK ALL TABLES
FORMAT PrettyCompactMonoBlock
SETTINGS check_query_single_value_result = 0
```

```text
┌─database─┬─table────┬─part_path───┬─is_passed─┬─message─┐
│ default  │ t2       │ all_1_95_3  │         1 │         │
│ db1      │ table_01 │ all_39_39_0 │         1 │         │
│ default  │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ t1       │ all_39_39_0 │         1 │         │
│ db1      │ table_01 │ all_1_6_1   │         1 │         │
│ default  │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ t1       │ all_1_6_1   │         1 │         │
│ db1      │ table_01 │ all_7_38_2  │         1 │         │
│ db1      │ t1       │ all_7_38_2  │         1 │         │
│ default  │ t1       │ all_7_38_2  │         1 │         │
└──────────┴──────────┴─────────────┴───────────┴─────────┘
```

<div id="if-the-data-is-corrupted">
  ## 如果数据损坏
</div>

如果表已损坏，您可以将未损坏的数据复制到另一张表。为此，请按以下步骤操作：

1. 创建一张与损坏表结构相同的新表。为此，请执行查询 `CREATE TABLE <new_table_name> AS <damaged_table_name>`。
2. 将 `max_threads` 的值设为 1，以便后续查询在单线程中处理。为此，请运行查询 `SET max_threads = 1`。
3. 执行查询 `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`。此请求会将损坏表中未损坏的数据复制到另一张表。只有损坏部分之前的数据会被复制。
4. 重启 `clickhouse-client` 以重置 `max_threads` 的值。