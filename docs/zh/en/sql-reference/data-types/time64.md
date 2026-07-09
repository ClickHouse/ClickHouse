---
description: 'ClickHouse 中 Time64 数据类型的文档，该类型以子秒级精度存储
  时刻'
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: 'Time64'
title: 'Time64'
doc_type: 'reference'
---

数据类型 `Time64` 表示带有小数秒的时刻。
它不包含任何日历日期组成部分 (日、月、年) 。
`precision` 参数定义小数位数，因此也决定了时间粒度。

时间粒度 (precision) ：10<sup>-precision</sup> 秒。有效范围：0..9。常见取值为 3 (毫秒) 、6 (微秒) 和 9 (纳秒) 。

**语法：**

```sql
Time64(precision)
```

在内部，`Time64` 将小数秒存储为有符号的 64 位十进制数 (Decimal64) 。
tick 的分辨率由 `precision` 参数决定。
不支持时区：为 `Time64` 指定时区会抛出错误。

与 `DateTime64` 不同，`Time64` 不存储日期部分。
另请参见 [`Time`](../../sql-reference/data-types/time.md)。

文本表示范围：当 `precision = 3` 时为 [-999:59:59.000, 999:59:59.999]。一般情况下，最小值为 `-999:59:59`，最大值为 `999:59:59`，最多可有 `precision` 位小数 (对于 `precision = 9`，最小值为 `-999:59:59.999999999`) 。

<div id="implementation-details">
  ## 实现细节
</div>

**表示形式**。
带符号的 `Decimal64` 值，用于表示秒的小数部分，小数位数由 `precision` 指定。

**归一化**。
将字符串解析为 `Time64` 时，其中的时、分、秒等组成部分会被归一化，但不会做合法性校验。
例如，`25:70:70` 会被解释为 `26:11:10`。

**负值**。
支持并保留前导负号。
负值通常来自对 `Time64` 值进行算术运算。
对于 `Time64`，无论是文本输入 (例如 `'-01:02:03.123'`) 还是数值输入 (例如 `-3723.123`) ，都会保留负值。

**饱和处理**。
在转换为各组成部分或序列化为文本时，时刻部分会被限制在 [-999:59:59.xxx, 999:59:59.xxx] 范围内。
存储的数值可能超出此范围；但是，任何组成部分提取 (小时、分钟、秒) 以及文本表示都会使用饱和后的值。

**时区**。
`Time64` 不支持时区。
创建 `Time64` 类型或值时，如果指定时区，会抛出错误。
同样，对 `Time64` 列应用或修改时区也不受支持，并会导致错误。

<div id="examples">
  ## 示例
</div>

1. 创建一个带有 `Time64` 类型列的表，并向其中插入数据：

```sql
CREATE TABLE tab64
(
    `event_id` UInt8,
    `time` Time64(3)
)
ENGINE = TinyLog;
```

```sql
-- Parse Time64
-- - from string,
-- - from a number of seconds since 00:00:00 (fractional part according to precision).
INSERT INTO tab64 VALUES (1, '14:30:25'), (2, 52225.123), (3, '14:30:25');

SELECT * FROM tab64 ORDER BY event_id;
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        2 │ 14:30:25.123 │
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

2. 按 `Time64` 值筛选

```sql
SELECT * FROM tab64 WHERE time = toTime64('14:30:25', 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

```sql
SELECT * FROM tab64 WHERE time = toTime64(52225.123, 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        2 │ 14:30:25.123 │
   └──────────┴──────────────┘
```

注：`toTime64` 会按指定精度将数字字面量解析为带小数部分的秒数，因此请显式写出所需的小数位数。

3. 检查结果类型：

```sql
SELECT CAST('14:30:25.250' AS Time64(3)) AS column, toTypeName(column) AS type;
```

```text
   ┌────────column─┬─type──────┐
1. │ 14:30:25.250 │ Time64(3) │
   └───────────────┴───────────┘
```

<div id="addition-with-date">
  ## 与 Date 相加
</div>

可将 [Time64](time64.md) 值与 [Date](date.md) 或 [Date32](date32.md) 值相加，得到一个与 `Time64` 具有相同标度的 [DateTime64](datetime64.md)：

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
   ┌─────────────────────────dt─┬─toTypeName(dt)─┐
1. │ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
   └────────────────────────────┴────────────────┘
```

有关所有支持的组合和结果类型的详细信息，请参阅[日期和时间加法](../operators/index.md#date-time-addition)。

**另请参阅**

* [类型转换函数](../../sql-reference/functions/type-conversion-functions.md)
* [日期和时间函数](../../sql-reference/functions/date-time-functions.md)
* [`date_time_input_format` 设置](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 设置](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` server 配置参数](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 设置](../../operations/settings/settings.md#session_timezone)
* [日期和时间运算符](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [`Date` 数据类型](../../sql-reference/data-types/date.md)
* [`Time` 数据类型](../../sql-reference/data-types/time.md)
* [`DateTime` 数据类型](../../sql-reference/data-types/datetime.md)