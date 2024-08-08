---
slug: /zh/sql-reference/data-types/special-data-types/interval
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 61
sidebar_label: "\u95F4\u9694"
---

# Interval类型 {#data-type-interval}

表示时间和日期间隔的数据类型家族。  [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) 运算的结果类型。

!!! warning "警告"
    `Interval` 数据类型值不能存储在表中。

结构:

-   时间间隔作为无符号整数值。
-   时间间隔的类型。

支持的时间间隔类型:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

对于每个时间间隔类型，都有一个单独的数据类型。 例如， `DAY` 间隔对应于 `IntervalDay` 数据类型:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## 使用说明 {#data-type-interval-usage-remarks}

您可以在与 [日期](../../../sql-reference/data-types/date.md) 和 [日期时间](../../../sql-reference/data-types/datetime.md) 类型值的算术运算中使用 `Interval` 类型值。 例如，您可以将4天添加到当前时间:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

也可以同時使用多個間隔：

``` sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

並比較不同直數的值：

``` sql
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
```

``` text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

## 另请参阅 {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) 操作
-   [toInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) 类型转换函数
