---
description: 'Documentation for Operators'
sidebar_label: 'Operators'
sidebar_position: 38
slug: /sql-reference/operators/
title: 'Operators'
doc_type: 'reference'
---

# Operators

ClickHouse transforms operators to their corresponding functions at the query parsing stage according to their priority, precedence, and associativity.

## Access Operators {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` function.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` function.

## Numeric Negation Operator {#numeric-negation-operator}

`-a` – The `negate (a)` function.

For tuple negation: [tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate).

## Multiplication and Division Operators {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` function.

For multiplying tuple by number: [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber), for scalar product: [dotProduct](/sql-reference/functions/array-functions#arrayDotProduct).

`a / b` – The `divide(a, b)` function.

For dividing tuple by number: [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber).

`a % b` – The `modulo(a, b)` function.

## Addition and Subtraction Operators {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` function.

For tuple addiction: [tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus).

`a - b` – The `minus(a, b)` function.

For tuple subtraction: [tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus).

## Comparison Operators {#comparison-operators}

### equals function {#equals-function}
`a = b` – The `equals(a, b)` function.

`a == b` – The `equals(a, b)` function.

### notEquals function {#notequals-function}
`a != b` – The `notEquals(a, b)` function.

`a <> b` – The `notEquals(a, b)` function.

### lessOrEquals function {#lessorequals-function}
`a <= b` – The `lessOrEquals(a, b)` function.

### greaterOrEquals function {#greaterorequals-function}
`a >= b` – The `greaterOrEquals(a, b)` function.

### less function {#less-function}
`a < b` – The `less(a, b)` function.

### greater function {#greater-function}
`a > b` – The `greater(a, b)` function.

### like function {#like-function}
`a LIKE b` – The `like(a, b)` function.

### notLike function {#notlike-function}
`a NOT LIKE b` – The `notLike(a, b)` function.

### ilike function {#ilike-function}
`a ILIKE b` – The `ilike(a, b)` function.

### BETWEEN function {#between-function}
`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

### is not distinct from operator (`<=>`) {#is-not-distinct-from}

:::note
From 25.10 you can use `<=>` in the same way as any other operator.
Before 25.10 it could only be used in JOIN expressions, for example:

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```
:::

The `<=>` operator is the `NULL`-safe equality operator, equivalent to `IS NOT DISTINCT FROM`.
It works like the regular equality operator (`=`), but it treats `NULL` values as comparable.
Two `NULL` values are considered equal, and a `NULL` compared to any non-`NULL` value returns 0 (false) rather than `NULL`.

```sql
SELECT
  'ClickHouse' <=> NULL,
  NULL <=> NULL
```

```response
┌─isNotDistinc⋯use', NULL)─┬─isNotDistinc⋯NULL, NULL)─┐
│                        0 │                        1 │
└──────────────────────────┴──────────────────────────┘
```

## Operators for Working with Strings {#operators-for-working-with-strings}

### OVERLAY {#overlay}

- `OVERLAY(string PLACING replacement FROM offset)` - The `overlay(string, replacement, offset)` function.
- `OVERLAY(string PLACING replacement FROM offset FOR length)` - The `overlay(string, replacement, offset, length)` function.
- `OVERLAYUTF8(string PLACING replacement FROM offset)` - The `overlayUTF8(string, replacement, offset)` function.
- `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` - The `overlayUTF8(string, replacement, offset, length)` function.

## Operators for Working with Data Sets {#operators-for-working-with-data-sets}

See [IN operators](../../sql-reference/operators/in.md) and [EXISTS](../../sql-reference/operators/exists.md) operator.

### in function {#in-function}
`a IN ...` – The `in(a, b)` function.

### notIn function {#notin-function}
`a NOT IN ...` – The `notIn(a, b)` function.

### globalIn function {#globalin-function}
`a GLOBAL IN ...` – The `globalIn(a, b)` function.

### globalNotIn function {#globalnotin-function}
`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` function.

### in subquery function {#in-subquery-function}
`a = ANY (subquery)` – The `in(a, subquery)` function.

### notIn subquery function {#notin-subquery-function}
`a != ANY (subquery)` – The same as `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`.

### in subquery function {#in-subquery-function-1}
`a = ALL (subquery)` – The same as `a IN (SELECT singleValueOrNull(*) FROM subquery)`.

### notIn subquery function {#notin-subquery-function-1}
`a != ALL (subquery)` – The `notIn(a, subquery)` function.

**Examples**

Query with ALL:

```sql
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

Result:

```text
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

Query with ANY:

```sql
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

Result:

```text
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

## Operators for Working with Dates and Times {#operators-for-working-with-dates-and-times}

### EXTRACT {#extract}

```sql
EXTRACT(part FROM date);
```

Extract parts from a given date. For example, you can retrieve a month from a given date, or a second from a time.

The `part` parameter specifies which part of the date to retrieve. The following values are available:

- `SECOND` — The second. Possible values: 0–59.
- `MINUTE` — The minute. Possible values: 0–59.
- `HOUR` — The hour. Possible values: 0–23.
- `DAY` — The day of the month. Possible values: 1–31.
- `WEEK` — The ISO 8601 week number. Possible values: 1–53.
- `MONTH` — The number of a month. Possible values: 1–12.
- `QUARTER` — The quarter. Possible values: 1–4.
- `YEAR` — The year.
- `EPOCH` — The Unix timestamp (seconds since 1970-01-01 00:00:00 UTC). Note: for `DateTime64`, the subsecond part is truncated.
- `DOW` — The day of the week (PostgreSQL-compatible). 0 = Sunday, 6 = Saturday.
- `DOY` — The day of the year. Possible values: 1–366.
- `ISODOW` — The ISO day of the week. 1 = Monday, 7 = Sunday.
- `ISOYEAR` — The ISO 8601 week-numbering year.
- `CENTURY` — The century. For example, the year 2024 is in the 21st century.
- `DECADE` — The decade (year divided by 10). For example, the year 2024 has decade 202.
- `MILLENNIUM` — The millennium. For example, the year 2024 is in the 3rd millennium.

The `part` parameter is case-insensitive.

The `date` parameter specifies the date or the time to process. The [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), and [DateTime64](../../sql-reference/data-types/datetime64.md) types are supported.

Examples:

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
SELECT EXTRACT(EPOCH FROM toDateTime('2024-01-15 12:30:45', 'UTC'));
SELECT EXTRACT(DOW FROM toDate('2024-01-15'));
SELECT EXTRACT(CENTURY FROM toDate('2024-01-01'));
```

In the following example we create a table and insert into it a value with the `DateTime` type.

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
) ENGINE = MergeTree
ORDER BY ();
```

```sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

```sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

```text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

You can see more examples in [tests](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#interval}

Creates an [Interval](../../sql-reference/data-types/special-data-types/interval.md)-type value that should be used in arithmetical operations with [Date](../../sql-reference/data-types/date.md) and [DateTime](../../sql-reference/data-types/datetime.md)-type values.

Types of intervals:
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

You can also use a string literal when setting the `INTERVAL` value. For example, `INTERVAL 1 HOUR` is identical to the `INTERVAL '1 hour'` or `INTERVAL '1' hour`.

:::tip
Intervals with different types can't be combined. You can't use expressions like `INTERVAL 4 DAY 1 HOUR`. Specify intervals in units that are smaller or equal to the smallest unit of the interval, for example, `INTERVAL 25 HOUR`. You can use consecutive operations, like in the example below.
:::

Examples:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

:::note
The `INTERVAL` syntax or `addDays` function are always preferred. Simple addition or subtraction (syntax like `now() + ...`) doesn't consider time settings. For example, daylight saving time.
:::

Examples:

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**See Also**

- [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type
- [toInterval](/sql-reference/functions/type-conversion-functions#toIntervalYear) type conversion functions

### Date and Time Addition {#date-time-addition}

A [Date](../../sql-reference/data-types/date.md) or [Date32](../../sql-reference/data-types/date32.md) value can be added to a [Time](../../sql-reference/data-types/time.md) or [Time64](../../sql-reference/data-types/time64.md) value using the `+` operator. The result is a [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md) representing the date at the given time of day. The operation is commutative.

The result type depends on the operand types:

| Left operand | Right operand | Result type |
|---|---|---|
| `Date` | `Time` | `DateTime` |
| `Date` | `Time64(s)` | `DateTime64(s)` |
| `Date32` | `Time` | `DateTime64(0)` |
| `Date32` | `Time64(s)` | `DateTime64(s)` |

:::note
The result uses the [session timezone](../../operations/settings/settings.md#session_timezone) (or server default timezone if no session timezone is set). The [`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) setting controls what happens when the result is outside the representable range.
:::

Examples:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') AS dt, toTypeName(dt);
```

```text
┌──────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25 │ DateTime       │
└─────────────────────┴────────────────┘
```

```sql
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
┌─────────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
└────────────────────────────┴────────────────┘
```

```sql
SELECT toTime64('23:59:59.999', 3) + toDate32('2024-07-15') AS dt, toTypeName(dt);
```

```text
┌──────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 23:59:59.999 │ DateTime64(3)  │
└─────────────────────────┴────────────────┘
```

## Logical AND Operator {#logical-and-operator}

Syntax `SELECT a AND b` — calculates logical conjunction of `a` and `b` with the function [and](/sql-reference/functions/logical-functions#and).

## Logical OR Operator {#logical-or-operator}

Syntax `SELECT a OR b` — calculates logical disjunction of `a` and `b` with the function [or](/sql-reference/functions/logical-functions#or).

## Logical Negation Operator {#logical-negation-operator}

Syntax `SELECT NOT a` — calculates logical negation of `a` with the function [not](/sql-reference/functions/logical-functions#not).

## Conditional Operator {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` function.

Note:

The conditional operator calculates the values of b and c, then checks whether condition a is met, and then returns the corresponding value. If `b` or `C` is an [arrayJoin()](/sql-reference/functions/array-join) function, each row will be replicated regardless of the "a" condition.

## Conditional Expression {#conditional-expression}

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

If `x` is specified, then `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

If there is no `ELSE c` clause in the expression, the default value is `NULL`.

The `transform` function does not work with `NULL`.

## Concatenation Operator {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## Lambda Creation Operator {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

The following operators do not have a priority since they are brackets:

## Array Creation Operator {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## Tuple Creation Operator {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## Associativity {#associativity}

All binary operators have left associativity. For example, `1 + 2 + 3` is transformed to `plus(plus(1, 2), 3)`.
Sometimes this does not work the way you expect. For example, `SELECT 4 > 2 > 3` will result in 0.

For efficiency, the `and` and `or` functions accept any number of arguments. The corresponding chains of `AND` and `OR` operators are transformed into a single call of these functions.

## Checking for `NULL` {#checking-for-null}

ClickHouse supports the `IS NULL` and `IS NOT NULL` operators.

### IS NULL {#is_null}

- For [Nullable](../../sql-reference/data-types/nullable.md) type values, the `IS NULL` operator returns:
  - `1`, if the value is `NULL`.
  - `0` otherwise.
- For other values, the `IS NULL` operator always returns `0`.

Can be optimized by enabling the [optimize_functions_to_subcolumns](/operations/settings/settings#optimize_functions_to_subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [null](../../sql-reference/data-types/nullable.md#finding-null) subcolumn instead of reading and processing the whole column data. The query `SELECT n IS NULL FROM table` transforms to `SELECT n.null FROM TABLE`.

<!-- -->

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is_not_null}

- For [Nullable](../../sql-reference/data-types/nullable.md) type values, the `IS NOT NULL` operator returns:
  - `0`, if the value is `NULL`.
  - `1` otherwise.
- For other values, the `IS NOT NULL` operator always returns `1`.

<!-- -->

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

Can be optimized by enabling the [optimize_functions_to_subcolumns](/operations/settings/settings#optimize_functions_to_subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [null](../../sql-reference/data-types/nullable.md#finding-null) subcolumn instead of reading and processing the whole column data. The query `SELECT n IS NOT NULL FROM table` transforms to `SELECT NOT n.null FROM TABLE`.
