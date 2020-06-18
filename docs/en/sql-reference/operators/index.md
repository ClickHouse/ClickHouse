---
toc_priority: 37
toc_title: Operators
---

# Operators {#operators}

ClickHouse transforms operators to their corresponding functions at the query parsing stage according to their priority, precedence, and associativity.

## Access Operators {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` function.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` function.

## Numeric Negation Operator {#numeric-negation-operator}

`-a` – The `negate (a)` function.

## Multiplication and Division Operators {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` function.

`a / b` – The `divide(a, b)` function.

`a % b` – The `modulo(a, b)` function.

## Addition and Subtraction Operators {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` function.

`a - b` – The `minus(a, b)` function.

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

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## Operators for Working with Data Sets {#operators-for-working-with-data-sets}

*See [IN operators](../../sql-reference/operators/in.md).*

`a IN ...` – The `in(a, b)` function.

`a NOT IN ...` – The `notIn(a, b)` function.

`a GLOBAL IN ...` – The `globalIn(a, b)` function.

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` function.

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

!!! warning "Warning"
    Intervals with different types can’t be combined. You can’t use expressions like `INTERVAL 4 DAY 1 HOUR`. Specify intervals in units that are smaller or equal to the smallest unit of the interval, for example, `INTERVAL 25 HOUR`. You can use consecutive operations, like in the example below.

Example:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**See Also**

-   [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type
-   [toInterval](../../sql-reference/functions/type-conversion-functions.md#function-tointerval) type convertion functions

## Logical Negation Operator {#logical-negation-operator}

`NOT a` – The `not(a)` function.

## Logical AND Operator {#logical-and-operator}

`a AND b` – The`and(a, b)` function.

## Logical OR Operator {#logical-or-operator}

`a OR b` – The `or(a, b)` function.

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
Sometimes this doesn’t work the way you expect. For example, `SELECT 4 > 2 > 3` will result in 0.

For efficiency, the `and` and `or` functions accept any number of arguments. The corresponding chains of `AND` and `OR` operators are transformed into a single call of these functions.

## Checking for `NULL` {#checking-for-null}

ClickHouse supports the `IS NULL` and `IS NOT NULL` operators.

### IS NULL {#operator-is-null}

-   For [Nullable](../../sql-reference/data-types/nullable.md) type values, the `IS NULL` operator returns:
    -   `1`, if the value is `NULL`.
    -   `0` otherwise.
-   For other values, the `IS NULL` operator always returns `0`.

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

[Original article](https://clickhouse.tech/docs/en/query_language/operators/) <!--hide-->
