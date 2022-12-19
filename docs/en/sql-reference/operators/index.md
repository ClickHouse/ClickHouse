---
toc_priority: 38
toc_title: Operators
---

# Operators {#operators}

ClickHouse transforms operators to their corresponding functions at the query parsing stage according to their priority, precedence, and associativity.

## Access Operators {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` function.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` function.

## Numeric Negation Operator {#numeric-negation-operator}

`-a` – The `negate (a)` function.

For tuple negation: [tupleNegate](../../sql-reference/functions/tuple-functions.md#tuplenegate).

## Multiplication and Division Operators {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` function.

For multiplying tuple by number: [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tuplemultiplybynumber), for scalar profuct: [dotProduct](../../sql-reference/functions/tuple-functions.md#dotproduct).

`a / b` – The `divide(a, b)` function.

For dividing tuple by number: [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupledividebynumber).

`a % b` – The `modulo(a, b)` function.

## Addition and Subtraction Operators {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` function.

For tuple addiction: [tuplePlus](../../sql-reference/functions/tuple-functions.md#tupleplus).

`a - b` – The `minus(a, b)` function.

For tuple subtraction: [tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleminus).

## Comparison Operators {#comparison-operators}

`a = b` – The `equals(a, b)` function.

`a == b` – The `equals(a, b)` function.

`a != b` – The `notEquals(a, b)` function.

`a <> b` – The `notEquals(a, b)` function.

`a <= b` – The `lessOrEquals(a, b)` function.

`a >= b` – The `greaterOrEquals(a, b)` function.

`a < b` – The `less(a, b)` function.

`a > b` – The `greater(a, b)` function.

`a LIKE s` – The `like(a, b)` function.

`a NOT LIKE s` – The `notLike(a, b)` function.

`a ILIKE s` – The `ilike(a, b)` function.

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## Operators for Working with Data Sets {#operators-for-working-with-data-sets}

See [IN operators](../../sql-reference/operators/in.md) and [EXISTS](../../sql-reference/operators/exists.md) operator.

`a IN ...` – The `in(a, b)` function.

`a NOT IN ...` – The `notIn(a, b)` function.

`a GLOBAL IN ...` – The `globalIn(a, b)` function.

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` function.

`a = ANY (subquery)` – The `in(a, subquery)` function.  

`a != ANY (subquery)` – The same as `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`.

`a = ALL (subquery)` – The same as `a IN (SELECT singleValueOrNull(*) FROM subquery)`.

`a != ALL (subquery)` – The `notIn(a, subquery)` function. 


**Examples**

Query with ALL:

``` sql
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

Result:

``` text
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

Query with ANY:

``` sql
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

Result:

``` text
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

## Operators for Working with Dates and Times {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

Extract parts from a given date. For example, you can retrieve a month from a given date, or a second from a time.

The `part` parameter specifies which part of the date to retrieve. The following values are available:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

The `part` parameter is case-insensitive.

The `date` parameter specifies the date or the time to process. Either [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md) type is supported.

Examples:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

In the following example we create a table and insert into it a value with the `DateTime` type.

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

``` sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

``` text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

You can see more examples in [tests](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

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

!!! warning "Warning"
    Intervals with different types can’t be combined. You can’t use expressions like `INTERVAL 4 DAY 1 HOUR`. Specify intervals in units that are smaller or equal to the smallest unit of the interval, for example, `INTERVAL 25 HOUR`. You can use consecutive operations, like in the example below.

Examples:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

You can work with dates without using `INTERVAL`, just by adding or subtracting seconds, minutes, and hours. For example, an interval of one day can be set by adding `60*60*24`.

!!! note "Note"
    The `INTERVAL` syntax or `addDays` function are always preferred. Simple addition or subtraction (syntax like `now() + ...`) doesn't consider time settings. For example, daylight saving time.


Examples:

``` sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

``` text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**See Also**

-   [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type
-   [toInterval](../../sql-reference/functions/type-conversion-functions.md#function-tointerval) type conversion functions

## Logical AND Operator {#logical-and-operator}

Syntax `SELECT a AND b` — calculates logical conjunction of `a` and `b` with the function [and](../../sql-reference/functions/logical-functions.md#logical-and-function).

## Logical OR Operator {#logical-or-operator}

Syntax `SELECT a OR b` — calculates logical disjunction of `a` and `b` with the function [or](../../sql-reference/functions/logical-functions.md#logical-or-function).

## Logical Negation Operator {#logical-negation-operator}

Syntax `SELECT NOT a` — calculates logical negation of `a` with the function [not](../../sql-reference/functions/logical-functions.md#logical-not-function).

## Conditional Operator {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` function.

Note:

The conditional operator calculates the values of b and c, then checks whether condition a is met, and then returns the corresponding value. If `b` or `C` is an [arrayJoin()](../../sql-reference/functions/array-join.md#functions_arrayjoin) function, each row will be replicated regardless of the “a” condition.

## Conditional Expression {#operator_case}

``` sql
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

### IS NULL {#operator-is-null}

-   For [Nullable](../../sql-reference/data-types/nullable.md) type values, the `IS NULL` operator returns:
    -   `1`, if the value is `NULL`.
    -   `0` otherwise.
-   For other values, the `IS NULL` operator always returns `0`.

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [null](../../sql-reference/data-types/nullable.md#finding-null) subcolumn instead of reading and processing the whole column data. The query `SELECT n IS NULL FROM table` transforms to `SELECT n.null FROM TABLE`.

<!-- -->

``` sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

``` text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is-not-null}

-   For [Nullable](../../sql-reference/data-types/nullable.md) type values, the `IS NOT NULL` operator returns:
    -   `0`, if the value is `NULL`.
    -   `1` otherwise.
-   For other values, the `IS NOT NULL` operator always returns `1`.

<!-- -->

``` sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

``` text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [null](../../sql-reference/data-types/nullable.md#finding-null) subcolumn instead of reading and processing the whole column data. The query `SELECT n IS NOT NULL FROM table` transforms to `SELECT NOT n.null FROM TABLE`.
