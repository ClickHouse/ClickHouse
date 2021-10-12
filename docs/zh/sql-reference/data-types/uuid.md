---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

通用唯一标识符(UUID)是用于标识记录的16字节数。 有关UUID的详细信息，请参阅 [维基百科](https://en.wikipedia.org/wiki/Universally_unique_identifier).

UUID类型值的示例如下所示:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

如果在插入新记录时未指定UUID列值，则UUID值将用零填充:

``` text
00000000-0000-0000-0000-000000000000
```

## 如何生成 {#how-to-generate}

要生成UUID值，ClickHouse提供了 [generateuidv4](../../sql-reference/functions/uuid-functions.md) 功能。

## 用法示例 {#usage-example}

**示例1**

此示例演示如何创建具有UUID类型列的表并将值插入到表中。

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**示例2**

在此示例中，插入新记录时未指定UUID列值。

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## 限制 {#restrictions}

UUID数据类型仅支持以下功能 [字符串](string.md) 数据类型也支持（例如, [min](../../sql-reference/aggregate-functions/reference.md#agg_function-min), [max](../../sql-reference/aggregate-functions/reference.md#agg_function-max)，和 [计数](../../sql-reference/aggregate-functions/reference.md#agg_function-count)).

算术运算不支持UUID数据类型（例如, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)）或聚合函数，例如 [sum](../../sql-reference/aggregate-functions/reference.md#agg_function-sum) 和 [avg](../../sql-reference/aggregate-functions/reference.md#agg_function-avg).

[原始文章](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
