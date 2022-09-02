---
sidebar_position: 46
sidebar_label: UUID
---

# UUID {#uuid-data-type}

通用唯一标识符(UUID)是一个16字节的数字，用于标识记录。有关UUID的详细信息, 参见[维基百科](https://en.wikipedia.org/wiki/Universally_unique_identifier)。

UUID类型值的示例如下:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

如果在插入新记录时未指定UUID列的值，则UUID值将用零填充:

``` text
00000000-0000-0000-0000-000000000000
```

## 如何生成 {#how-to-generate}

要生成UUID值，ClickHouse提供了 [generateuidv4](../../sql-reference/functions/uuid-functions.md) 函数。

## 用法示例 {#usage-example}

**示例1**

这个例子演示了创建一个具有UUID类型列的表，并在表中插入一个值。

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

在这个示例中，插入新记录时未指定UUID列的值。

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

UUID数据类型只支持 [字符串](../../sql-reference/data-types/string.md) 数据类型也支持的函数(比如, [min](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min), [max](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max), 和 [count](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count))。

算术运算不支持UUID数据类型（例如, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)）或聚合函数，例如 [sum](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum) 和 [avg](../../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg).

