---
description: 'ClickHouse 中 Time 数据类型的文档，该类型以秒精度存储时间'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

`Time` 数据类型表示由小时、分钟和秒组成的时间。
它独立于任何日历日期，适用于不需要日、月、年组成部分的值。

语法：

```sql
Time
```

文本表示范围：[-999:59:59, 999:59:59]。

分辨率：1 秒。

<div id="implementation-details">
  ## 实现细节
</div>

**表示与性能**。
数据类型 `Time` 在内部以带符号的 32 位整数存储，用于编码秒数。
`Time` 和 `DateTime` 类型的值具有相同的字节大小，因此性能也相当。

**归一化**。
将字符串解析为 `Time` 时，时间各组成部分会被归一化，而不会进行校验。
例如，`25:70:70` 会被解释为 `26:11:10`。

**负值**。
支持并保留前导负号。
负值通常来自对 `Time` 值执行算术运算。
对于 `Time` 类型，无论是文本输入 (例如 `'-01:02:03'`) 还是数值输入 (例如 `-3723`) ，负值都会被保留。

**饱和**。
时刻部分会被限制在 [-999:59:59, 999:59:59] 范围内。
小时数超过 999 (或低于 -999) 的值，在通过文本表示及往返转换时，会表示为 `999:59:59` (或 `-999:59:59`) 。

**时区**。
`Time` 不支持时区，也就是说，`Time` 值的解释不依赖任何区域上下文。
为 `Time` 指定时区作为类型参数，或在创建值时指定时区，都会抛出错误。
同样，尝试对 `Time` 列应用或更改时区也是不受支持的，并会导致错误。
`Time` 值不会在不同的时区下被静默重新解释。

<div id="examples">
  ## 示例
</div>

**1.** 创建一个包含 `Time` 类型列的表，并向其中插入数据：

```sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

```sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** 按 `Time` 值筛选

```sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

`Time` 列的值可以在 `WHERE` 条件中使用字符串值进行过滤。它会自动转换为 `Time`：

```sql
SELECT * FROM tab WHERE time = '14:30:25'
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** 查看结果类型：

```sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

```text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

<div id="addition-with-date">
  ## Date 加法
</div>

可以将 [Time](time.md) 值加到 [Date](date.md) 或 [Date32](date32.md) 值上，得到 [DateTime](datetime.md) 或 [DateTime64](datetime64.md)：

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

有关所有受支持的组合及结果类型的详细信息，请参阅[日期和时间加法](../operators/index.md#date-time-addition)。

<div id="see-also">
  ## 另请参阅
</div>

* [类型转换函数](../functions/type-conversion-functions.md)
* [处理日期和时间的函数](../functions/date-time-functions.md)
* [处理数组的函数](../functions/array-functions.md)
* [`date_time_input_format` 设置](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 设置](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` 服务器配置参数](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 设置](../../operations/settings/settings.md#session_timezone)
* [`DateTime` 数据类型](datetime.md)
* [`Date` 数据类型](date.md)