---
description: 'ClickHouse 中 Date32 数据类型的文档，该类型用于存储日期，且相比 Date
  具有更大的取值范围'
sidebar_label: 'Date32'
sidebar_position: 14
slug: /sql-reference/data-types/date32
title: 'Date32'
doc_type: 'reference'
---

一种日期类型。支持与 [DateTime64](../../sql-reference/data-types/datetime64.md) 相同的日期范围。以原生字节序存储为有符号 32 位整数，其值表示自 `1900-01-01` 以来的天数。**重要！** 0 表示 `1970-01-01`，负值表示 `1970-01-01` 之前的天数。

**示例**

创建一个包含 `Date32` 类型列的表，并向其中插入数据：

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**另请参见**

* [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
* [toDate32OrZero](/zh/sql-reference/functions/type-conversion-functions#toDate32OrZero)
* [toDate32OrNull](/zh/sql-reference/functions/type-conversion-functions#toDate32OrNull)