---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u95F4\u9694"
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

不同类型的间隔不能合并。 你不能使用诸如 `4 DAY 1 HOUR` 的时间间隔. 以小于或等于时间间隔最小单位的单位来指定间隔，例如，时间间隔 `1 day and an hour` 可以表示为 `25 HOUR` 或 `90000 SECOND`.

你不能对 `Interval` 类型的值执行算术运算，但你可以向 `Date` 或 `DateTime` 数据类型的值添加不同类型的时间间隔，例如:
 
``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

以下查询将导致异常:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## 另请参阅 {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) 操作
-   [toInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) 类型转换函数
