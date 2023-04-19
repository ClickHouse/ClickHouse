# system.data_skipping_indices {#system-data-skipping-indices}

包含有关所有表中现有数据跳过索引的信息.

列信息:

-   `database` ([String](../../sql-reference/data-types/string.md)) — 数据库名称.
-   `table` ([String](../../sql-reference/data-types/string.md)) — 数据表名称.
-   `name` ([String](../../sql-reference/data-types/string.md)) — 索引名称.
-   `type` ([String](../../sql-reference/data-types/string.md)) — 索引类型.
-   `expr` ([String](../../sql-reference/data-types/string.md)) — 索引计算表达式.
-   `granularity` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 块中颗粒的数量.
-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 压缩数据的大小, 以字节为单位.
-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 解压缩数据的大小, 以字节为单位.
-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 标记的大小, 以字节为单位.

**示例**

```sql
SELECT * FROM system.data_skipping_indices LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       user_actions
name:        clicks_idx
type:        minmax
expr:        clicks
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks:                   48

Row 2:
──────
database:    default
table:       users
name:        contacts_null_idx
type:        minmax
expr:        assumeNotNull(contacts_null)
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks:                   48
```
