---
description: 'ClickHouse 中 UUID 数据类型的文档'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

通用唯一标识符 (UUID) 是一个 16 字节的值，用于标识记录。有关 UUID 的详细信息，请参见 [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier)。

虽然 UUID 有不同的变体，例如 UUIDv4 和 UUIDv7 (参见[这里](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)) ，但 ClickHouse 不会验证插入的 UUID 是否符合某种特定变体。
在 SQL 层面，UUID 在内部被视为由 16 个随机字节组成的序列，并采用 [8-4-4-4-12 表示形式](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation)。

UUID 值示例：

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

默认 UUID 为全零。例如，在插入新记录时，如果未为 UUID 列指定值，就会使用它：

```text
00000000-0000-0000-0000-000000000000
```

:::warning
由于历史原因，UUIDs 是按后半部分排序的。

这对于 UUIDv4 值没有问题，但如果在主索引定义中使用 UUIDv7 列，可能会导致性能下降 (用于排序键或分区键则没有问题) 。
更具体地说，UUIDv7 值的前半部分是时间戳，后半部分是计数器。
因此，在稀疏主键索引中对 UUIDv7 排序时 (即每个索引粒度的第一个值) ，实际会按照计数器字段排序。
如果 UUIDs 是按前半部分 (时间戳) 排序，那么在查询开始阶段进行主键索引分析时，理论上可以剪枝掉除一个分片外所有分片中的全部标记。
但是，如果按后半部分 (计数器) 排序，则预计所有分片都会至少返回一个标记，从而导致不必要的磁盘访问。
:::

示例：

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

作为一种权宜之计，可以将 UUID 转换为从后半部分提取的时间戳：

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

结果 (假设插入了相同的数据) ：

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## 生成 UUID
</div>

ClickHouse 提供 [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) 函数，用于生成随机 UUIDv4 值。

<div id="usage-example">
  ## 用法示例
</div>

**示例 1**

本示例演示了如何创建一个包含 UUID 列的表，并向该表插入一个值。

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**示例 2**

在此示例中，插入记录时未指定 UUID 列的值，也就是说，会插入默认的 UUID 值：

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## 限制
</div>

UUID 数据类型仅支持 [String](../../sql-reference/data-types/string.md) 数据类型同样支持的函数 (例如，[min](/zh/sql-reference/aggregate-functions/reference/min)、[max](/zh/sql-reference/aggregate-functions/reference/max) 和 [count](/zh/sql-reference/aggregate-functions/reference/count)) 。

UUID 数据类型不支持算术运算 (例如，[abs](/zh/sql-reference/functions/arithmetic-functions#abs)) ，也不支持聚合函数，例如 [sum](/zh/sql-reference/aggregate-functions/reference/sum) 和 [avg](/zh/sql-reference/aggregate-functions/reference/avg)。