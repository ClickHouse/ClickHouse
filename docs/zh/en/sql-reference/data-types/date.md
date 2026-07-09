---
description: 'ClickHouse 中 Date 数据类型的文档'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
doc_type: 'reference'
---

日期。以自 1970-01-01 以来的天数 (无符号) 形式存储，占用两个字节。可存储从 Unix 纪元开始之后到编译阶段由常量定义的上限之间的值 (当前截至 2149 年，但最终完全支持的年份为 2148 年) 。

支持的值范围：[1970-01-01, 2149-06-06]。

日期值存储时不包含时区信息。

**示例**

创建一个包含 `Date` 类型列的表，并向其中插入数据：

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**另请参阅**

* [日期和时间函数](../../sql-reference/functions/date-time-functions.md)
* [日期和时间运算符](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [`DateTime` 数据类型](../../sql-reference/data-types/datetime.md)