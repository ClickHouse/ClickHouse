---
sidebar_position: 67
sidebar_label: Other
---

# Other Functions

## hostName()

Returns a string with the name of the host that this function was performed on. For distributed processing, this is the name of the remote server host, if the function is performed on a remote server.
If it is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

## getMacro

Gets a named value from the [macros](../../operations/server-configuration-parameters/settings.md#macros) section of the server configuration.

**Syntax**

``` sql
getMacro(name);
```

**Arguments**

-   `name` — Name to retrieve from the `macros` section. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Value of the specified macro.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

The example `macros` section in the server configuration file:

``` xml
<macros>
    <test>Value</test>
</macros>
```

Query:

``` sql
SELECT getMacro('test');
```

Result:

``` text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

An alternative way to get the same value:

``` sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

``` text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## FQDN

Returns the fully qualified domain name.

**Syntax**

``` sql
fqdn();
```

This function is case-insensitive.

**Returned value**

-   String with the fully qualified domain name.

Type: `String`.

**Example**

Query:

``` sql
SELECT FQDN();
```

Result:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename

Extracts the trailing part of a string after the last slash or backslash. This function if often used to extract the filename from a path.

``` sql
basename( expr )
```

**Arguments**

-   `expr` — Expression resulting in a [String](../../sql-reference/data-types/string.md) type value. All the backslashes must be escaped in the resulting value.

**Returned Value**

A string that contains:

-   The trailing part of a string after the last slash or backslash.

        If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

-   The original string if there are no slashes or backslashes.

**Example**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x)

Calculates the approximate width when outputting values to the console in text format (tab-separated).
This function is used by the system for implementing Pretty formats.

`NULL` is represented as a string corresponding to `NULL` in `Pretty` formats.

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName(x)

Returns a string containing the type name of the passed argument.

If `NULL` is passed to the function as input, then it returns the `Nullable(Nothing)` type, which corresponds to an internal `NULL` representation in ClickHouse.

## blockSize()

Gets the size of the block.
In ClickHouse, queries are always run on blocks (sets of column parts). This function allows getting the size of the block that you called it for.

## byteSize

Returns estimation of uncompressed byte size of its arguments in memory.

**Syntax**

```sql
byteSize(argument [, ...])
```

**Arguments**

-   `argument` — Value.

**Returned value**

-   Estimation of byte size of the arguments in memory.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Examples**

For [String](../../sql-reference/data-types/string.md) arguments the funtion returns the string length + 9 (terminating zero + length).

Query:

```sql
SELECT byteSize('string');
```

Result:

```text
┌─byteSize('string')─┐
│                 15 │
└────────────────────┘
```

Query:

```sql
CREATE TABLE test
(
    `key` Int32,
    `u8` UInt8,
    `u16` UInt16,
    `u32` UInt32,
    `u64` UInt64,
    `i8` Int8,
    `i16` Int16,
    `i32` Int32,
    `i64` Int64,
    `f32` Float32,
    `f64` Float64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test VALUES(1, 8, 16, 32, 64,  -8, -16, -32, -64, 32.32, 64.64);

SELECT key, byteSize(u8) AS `byteSize(UInt8)`, byteSize(u16) AS `byteSize(UInt16)`, byteSize(u32) AS `byteSize(UInt32)`, byteSize(u64) AS `byteSize(UInt64)`, byteSize(i8) AS `byteSize(Int8)`, byteSize(i16) AS `byteSize(Int16)`, byteSize(i32) AS `byteSize(Int32)`, byteSize(i64) AS `byteSize(Int64)`, byteSize(f32) AS `byteSize(Float32)`, byteSize(f64) AS `byteSize(Float64)` FROM test ORDER BY key ASC FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
key:               1
byteSize(UInt8):   1
byteSize(UInt16):  2
byteSize(UInt32):  4
byteSize(UInt64):  8
byteSize(Int8):    1
byteSize(Int16):   2
byteSize(Int32):   4
byteSize(Int64):   8
byteSize(Float32): 4
byteSize(Float64): 8
```

If the function takes multiple arguments, it returns their combined byte size.

Query:

```sql
SELECT byteSize(NULL, 1, 0.3, '');
```

Result:

```text
┌─byteSize(NULL, 1, 0.3, '')─┐
│                         19 │
└────────────────────────────┘
```

## materialize(x)

Turns a constant into a full column containing just one value.
In ClickHouse, full columns and constants are represented differently in memory. Functions work differently for constant arguments and normal arguments (different code is executed), although the result is almost always the same. This function is for debugging this behavior.

## ignore(…)

Accepts any arguments, including `NULL`. Always returns 0.
However, the argument is still evaluated. This can be used for benchmarks.

## sleep(seconds)

Sleeps ‘seconds’ seconds on each data block. You can specify an integer or a floating-point number.

## sleepEachRow(seconds)

Sleeps ‘seconds’ seconds on each row. You can specify an integer or a floating-point number.

## currentDatabase()

Returns the name of the current database.
You can use this function in table engine parameters in a CREATE TABLE query where you need to specify the database.

## currentUser()

Returns the login of current user. Login of user, that initiated query, will be returned in case distibuted query.

``` sql
SELECT currentUser();
```

Alias: `user()`, `USER()`.

**Returned values**

-   Login of current user.
-   Login of user that initiated query in case of disributed query.

Type: `String`.

**Example**

Query:

``` sql
SELECT currentUser();
```

Result:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## isConstant

Checks whether the argument is a constant expression.

A constant expression means an expression whose resulting value is known at the query analysis (i.e. before execution). For example, expressions over [literals](../../sql-reference/syntax.md#literals) are constant expressions.

The function is intended for development, debugging and demonstration.

**Syntax**

``` sql
isConstant(x)
```

**Arguments**

-   `x` — Expression to check.

**Returned values**

-   `1` — `x` is constant.
-   `0` — `x` is non-constant.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

Result:

``` text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Query:

``` sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

Result:

``` text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

Query:

``` sql
SELECT isConstant(number) FROM numbers(1)
```

Result:

``` text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## isFinite(x)

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is not infinite and not a NaN, otherwise 0.

## isInfinite(x)

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is infinite, otherwise 0. Note that 0 is returned for a NaN.

## ifNotFinite

Checks whether floating point value is finite.

**Syntax**

    ifNotFinite(x,y)

**Arguments**

-   `x` — Value to be checked for infinity. Type: [Float\*](../../sql-reference/data-types/float.md).
-   `y` — Fallback value. Type: [Float\*](../../sql-reference/data-types/float.md).

**Returned value**

-   `x` if `x` is finite.
-   `y` if `x` is not finite.

**Example**

Query:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Result:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

You can get similar result by using [ternary operator](../../sql-reference/functions/conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## isNaN(x)

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is a NaN, otherwise 0.

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’)

Accepts constant strings: database name, table name, and column name. Returns a UInt8 constant expression equal to 1 if there is a column, otherwise 0. If the hostname parameter is set, the test will run on a remote server.
The function throws an exception if the table does not exist.
For elements in a nested data structure, the function checks for the existence of a column. For the nested data structure itself, the function returns 0.

## bar

Allows building a unicode-art diagram.

`bar(x, min, max, width)` draws a band with a width proportional to `(x - min)` and equal to `width` characters when `x = max`.

**Arguments**

-   `x` — Size to display.
-   `min, max` — Integer constants. The value must fit in `Int64`.
-   `width` — Constant, positive integer, can be fractional.

The band is drawn with accuracy to one eighth of a symbol.

Example:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## transform

Transforms a value according to the explicitly defined mapping of some elements to other ones.
There are two variations of this function:

### transform(x, array_from, array_to, default)

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in ‘from’ to.

`default` – Which value to use if ‘x’ is not equal to any of the values in ‘from’.

`array_from` and `array_to` – Arrays of the same size.

Types:

`transform(T, Array(T), Array(U), U) -> U`

`T` and `U` can be numeric, string, or Date or DateTime types.
Where the same letter is indicated (T or U), for numeric types these might not be matching types, but types that have a common type.
For example, the first argument can have the Int64 type, while the second has the Array(UInt16) type.

If the ‘x’ value is equal to one of the elements in the ‘array_from’ array, it returns the existing element (that is numbered the same) from the ‘array_to’ array. Otherwise, it returns ‘default’. If there are multiple matching elements in ‘array_from’, it returns one of the matches.

Example:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### transform(x, array_from, array_to)

Differs from the first variation in that the ‘default’ argument is omitted.
If the ‘x’ value is equal to one of the elements in the ‘array_from’ array, it returns the matching element (that is numbered the same) from the ‘array_to’ array. Otherwise, it returns ‘x’.

Types:

`transform(T, Array(T), Array(T)) -> T`

Example:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## formatReadableSize(x)

Accepts the size (number of bytes). Returns a rounded size with a suffix (KiB, MiB, etc.) as a string.

Example:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## formatReadableQuantity(x)

Accepts the number. Returns a rounded number with a suffix (thousand, million, billion, etc.) as a string.

It is useful for reading big numbers by human.

Example:

``` sql
SELECT
    arrayJoin([1024, 1234 * 1000, (4567 * 1000) * 1000, 98765432101234]) AS number,
    formatReadableQuantity(number) AS number_for_humans
```

``` text
┌─────────number─┬─number_for_humans─┐
│           1024 │ 1.02 thousand     │
│        1234000 │ 1.23 million      │
│     4567000000 │ 4.57 billion      │
│ 98765432101234 │ 98.77 trillion    │
└────────────────┴───────────────────┘
```

## formatReadableTimeDelta

Accepts the time delta in seconds. Returns a time delta with (year, month, day, hour, minute, second) as a string.

**Syntax**

``` sql
formatReadableTimeDelta(column[, maximum_unit])
```

**Arguments**

-   `column` — A column with numeric time delta.
-   `maximum_unit` — Optional. Maximum unit to show. Acceptable values seconds, minutes, hours, days, months, years.

Example:

``` sql
SELECT
    arrayJoin([100, 12345, 432546534]) AS elapsed,
    formatReadableTimeDelta(elapsed) AS time_delta
```

``` text
┌────elapsed─┬─time_delta ─────────────────────────────────────────────────────┐
│        100 │ 1 minute and 40 seconds                                         │
│      12345 │ 3 hours, 25 minutes and 45 seconds                              │
│  432546534 │ 13 years, 8 months, 17 days, 7 hours, 48 minutes and 54 seconds │
└────────────┴─────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT
    arrayJoin([100, 12345, 432546534]) AS elapsed,
    formatReadableTimeDelta(elapsed, 'minutes') AS time_delta
```

``` text
┌────elapsed─┬─time_delta ─────────────────────────────────────────────────────┐
│        100 │ 1 minute and 40 seconds                                         │
│      12345 │ 205 minutes and 45 seconds                                      │
│  432546534 │ 7209108 minutes and 54 seconds                                  │
└────────────┴─────────────────────────────────────────────────────────────────┘
```

## least(a, b)

Returns the smallest value from a and b.

## greatest(a, b)

Returns the largest value of a and b.

## uptime()

Returns the server’s uptime in seconds.
If it is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

## version()

Returns the version of the server as a string.
If it is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

## buildId()

Returns the build ID generated by a compiler for the running ClickHouse server binary.
If it is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise it produces a constant value.


## blockNumber

Returns the sequence number of the data block where the row is located.

## rowNumberInBlock

Returns the ordinal number of the row in the data block. Different data blocks are always recalculated.

## rowNumberInAllBlocks()

Returns the ordinal number of the row in the data block. This function only considers the affected data blocks.

## neighbor

The window function that provides access to a row at a specified offset which comes before or after the current row of a given column.

**Syntax**

``` sql
neighbor(column, offset[, default_value])
```

The result of the function depends on the affected data blocks and the order of data in the block.

:::warning    
It can reach the neighbor rows only inside the currently processed data block.
:::

The rows order used during the calculation of `neighbor` can differ from the order of rows returned to the user.
To prevent that you can make a subquery with [ORDER BY](../../sql-reference/statements/select/order-by.md) and call the function from outside the subquery.

**Arguments**

-   `column` — A column name or scalar expression.
-   `offset` — The number of rows forwards or backwards from the current row of `column`. [Int64](../../sql-reference/data-types/int-uint.md).
-   `default_value` — Optional. The value to be returned if offset goes beyond the scope of the block. Type of data blocks affected.

**Returned values**

-   Value for `column` in `offset` distance from current row if `offset` value is not outside block bounds.
-   Default value for `column` if `offset` value is outside block bounds. If `default_value` is given, then it will be used.

Type: type of data blocks affected or default value type.

**Example**

Query:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Result:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

Query:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

Result:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

This function can be used to compute year-over-year metric value:

Query:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

Result:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## runningDifference(x)

Calculates the difference between successive row values ​​in the data block.
Returns 0 for the first row and the difference from the previous row for each subsequent row.

:::warning    
It can reach the previous row only inside the currently processed data block.
:::

The result of the function depends on the affected data blocks and the order of data in the block.

The rows order used during the calculation of `runningDifference` can differ from the order of rows returned to the user.
To prevent that you can make a subquery with [ORDER BY](../../sql-reference/statements/select/order-by.md) and call the function from outside the subquery.

Example:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

Please note - block size affects the result. With each new block, the `runningDifference` state is reset.

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

``` sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstValue

Same as for [runningDifference](./other-functions.md#other_functions-runningdifference), the difference is the value of the first row, returned the value of the first row, and each subsequent row returns the difference from the previous row.

## runningConcurrency

Calculates the number of concurrent events.
Each event has a start time and an end time. The start time is included in the event, while the end time is excluded. Columns with a start time and an end time must be of the same data type.
The function calculates the total number of active (concurrent) events for each event start time.


:::warning    
Events must be ordered by the start time in ascending order. If this requirement is violated the function raises an exception. Every data block is processed separately. If events from different data blocks overlap then they can not be processed correctly.
:::

**Syntax**

``` sql
runningConcurrency(start, end)
```

**Arguments**

-   `start` — A column with the start time of events. [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), or [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `end` — A column with the end time of events.  [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Returned values**

-   The number of concurrent events at each event start time.

Type: [UInt32](../../sql-reference/data-types/int-uint.md)

**Example**

Consider the table:

``` text
┌──────start─┬────────end─┐
│ 2021-03-03 │ 2021-03-11 │
│ 2021-03-06 │ 2021-03-12 │
│ 2021-03-07 │ 2021-03-08 │
│ 2021-03-11 │ 2021-03-12 │
└────────────┴────────────┘
```

Query:

``` sql
SELECT start, runningConcurrency(start, end) FROM example_table;
```

Result:

``` text
┌──────start─┬─runningConcurrency(start, end)─┐
│ 2021-03-03 │                              1 │
│ 2021-03-06 │                              2 │
│ 2021-03-07 │                              3 │
│ 2021-03-11 │                              2 │
└────────────┴────────────────────────────────┘
```

## MACNumToString(num)

Accepts a UInt64 number. Interprets it as a MAC address in big endian. Returns a string containing the corresponding MAC address in the format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form).

## MACStringToNum(s)

The inverse function of MACNumToString. If the MAC address has an invalid format, it returns 0.

## MACStringToOUI(s)

Accepts a MAC address in the format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form). Returns the first three octets as a UInt64 number. If the MAC address has an invalid format, it returns 0.

## getSizeOfEnumType

Returns the number of fields in [Enum](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**Arguments:**

-   `value` — Value of type `Enum`.

**Returned values**

-   The number of fields with `Enum` input values.
-   An exception is thrown if the type is not `Enum`.

**Example**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## blockSerializedSize

Returns size on disk (without taking into account compression).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**Arguments**

-   `value` — Any value.

**Returned values**

-   The number of bytes that will be written to disk for block of values (without compression).

**Example**

Query:

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

Result:

``` text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName

Returns the name of the class that represents the data type of the column in RAM.

``` sql
toColumnTypeName(value)
```

**Arguments:**

-   `value` — Any type of value.

**Returned values**

-   A string with the name of the class that is used for representing the `value` data type in RAM.

**Example of the difference between`toTypeName ' and ' toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

The example shows that the `DateTime` data type is stored in memory as `Const(UInt32)`.

## dumpColumnStructure

Outputs a detailed description of data structures in RAM

``` sql
dumpColumnStructure(value)
```

**Arguments:**

-   `value` — Any type of value.

**Returned values**

-   A string describing the structure that is used for representing the `value` data type in RAM.

**Example**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType

Outputs the default value for the data type.

Does not include default values for custom columns set by the user.

``` sql
defaultValueOfArgumentType(expression)
```

**Arguments:**

-   `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**Returned values**

-   `0` for numbers.
-   Empty string for strings.
-   `ᴺᵁᴸᴸ` for [Nullable](../../sql-reference/data-types/nullable.md).

**Example**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## defaultValueOfTypeName

Outputs the default value for given type name.

Does not include default values for custom columns set by the user.

``` sql
defaultValueOfTypeName(type)
```

**Arguments:**

-   `type` — A string representing a type name.

**Returned values**

-   `0` for numbers.
-   Empty string for strings.
-   `ᴺᵁᴸᴸ` for [Nullable](../../sql-reference/data-types/nullable.md).

**Example**

``` sql
SELECT defaultValueOfTypeName('Int8')
```

``` text
┌─defaultValueOfTypeName('Int8')─┐
│                              0 │
└────────────────────────────────┘
```

``` sql
SELECT defaultValueOfTypeName('Nullable(Int8)')
```

``` text
┌─defaultValueOfTypeName('Nullable(Int8)')─┐
│                                     ᴺᵁᴸᴸ │
└──────────────────────────────────────────┘
```

## indexHint
The function is intended for debugging and introspection purposes. The function ignores it's argument and always returns 1. Arguments are not even evaluated.

But for the purpose of index analysis, the argument of this function is analyzed as if it was present directly without being wrapped inside `indexHint` function. This allows to select data in index ranges by the corresponding condition but without further filtering by this condition. The index in ClickHouse is sparse and using `indexHint` will yield more data than specifying the same condition directly.

**Syntax**

```sql
SELECT * FROM table WHERE indexHint(<expression>)
```

**Returned value**

1. Type: [Uint8](https://clickhouse.com/docs/en/data_types/int_uint/#diapazony-uint).

**Example**

Here is the example of test data from the table [ontime](../../getting-started/example-datasets/ontime.md).

Input table:

```sql
SELECT count() FROM ontime
```

```text
┌─count()─┐
│ 4276457 │
└─────────┘
```

The table has indexes on the fields `(FlightDate, (Year, FlightDate))`.

Create a query, where the index is not used.

Query:

```sql
SELECT FlightDate AS k, count() FROM ontime GROUP BY k ORDER BY k
```

ClickHouse processed the entire table (`Processed 4.28 million rows`).

Result:

```text
┌──────────k─┬─count()─┐
│ 2017-01-01 │   13970 │
│ 2017-01-02 │   15882 │
........................
│ 2017-09-28 │   16411 │
│ 2017-09-29 │   16384 │
│ 2017-09-30 │   12520 │
└────────────┴─────────┘
```

To apply the index, select a specific date.

Query:

```sql
SELECT FlightDate AS k, count() FROM ontime WHERE k = '2017-09-15' GROUP BY k ORDER BY k
```

By using the index, ClickHouse processed a significantly smaller number of rows (`Processed 32.74 thousand rows`).

Result:

```text
┌──────────k─┬─count()─┐
│ 2017-09-15 │   16428 │
└────────────┴─────────┘
```

Now wrap the expression `k = '2017-09-15'` into `indexHint` function.

Query:

```sql
SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE indexHint(k = '2017-09-15')
GROUP BY k
ORDER BY k ASC
```

ClickHouse used the index in the same way as the previous time (`Processed 32.74 thousand rows`).
The expression `k = '2017-09-15'` was not used when generating the result.
In examle the `indexHint` function allows to see adjacent dates.

Result:

```text
┌──────────k─┬─count()─┐
│ 2017-09-14 │    7071 │
│ 2017-09-15 │   16428 │
│ 2017-09-16 │    1077 │
│ 2017-09-30 │    8167 │
└────────────┴─────────┘
```

## replicate

Creates an array with a single value.

Used for internal implementation of [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Arguments:**

-   `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
-   `x` — The value that the resulting array will be filled with.

**Returned value**

An array filled with the value `x`.

Type: `Array`.

**Example**

Query:

``` sql
SELECT replicate(1, ['a', 'b', 'c'])
```

Result:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable

Returns amount of remaining space on the filesystem where the files of the databases located. It is always smaller than total free space ([filesystemFree](#filesystemfree)) because some space is reserved for OS.

**Syntax**

``` sql
filesystemAvailable()
```

**Returned value**

-   The amount of remaining space available in bytes.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

Result:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## filesystemFree

Returns total amount of the free space on the filesystem where the files of the databases located. See also `filesystemAvailable`

**Syntax**

``` sql
filesystemFree()
```

**Returned value**

-   Amount of free space in bytes.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

Result:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## filesystemCapacity

Returns the capacity of the filesystem in bytes. For evaluation, the [path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) to the data directory must be configured.

**Syntax**

``` sql
filesystemCapacity()
```

**Returned value**

-   Capacity information of the filesystem in bytes.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

Result:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## initializeAggregation

Calculates result of aggregate function based on single value. It is intended to use this function to initialize aggregate functions with combinator [-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state). You can create states of aggregate functions and insert them to columns of type [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction) or use initialized aggregates as default values.

**Syntax**

``` sql
initializeAggregation (aggregate_function, arg1, arg2, ..., argN)
```

**Arguments**

-   `aggregate_function` — Name of the aggregation function to initialize. [String](../../sql-reference/data-types/string.md).
-   `arg` — Arguments of aggregate function.

**Returned value(s)**

- Result of aggregation for every row passed to the function.

The return type is the same as the return type of function, that `initializeAgregation` takes as first argument.

**Example**

Query:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM numbers(10000));
```
Result:

```text
┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
```

Query:

```sql
SELECT finalizeAggregation(state), toTypeName(state) FROM (SELECT initializeAggregation('sumState', number % 3) AS state FROM numbers(5));
```
Result:

```text
┌─finalizeAggregation(state)─┬─toTypeName(state)─────────────┐
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
│                          2 │ AggregateFunction(sum, UInt8) │
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
└────────────────────────────┴───────────────────────────────┘
```

Example with `AggregatingMergeTree` table engine and `AggregateFunction` column:

```sql
CREATE TABLE metrics
(
    key UInt64,
    value AggregateFunction(sum, UInt64) DEFAULT initializeAggregation('sumState', toUInt64(0))
)
ENGINE = AggregatingMergeTree
ORDER BY key
```

```sql
INSERT INTO metrics VALUES (0, initializeAggregation('sumState', toUInt64(42)))
```

**See Also**
-   [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)

## finalizeAggregation

Takes state of aggregate function. Returns result of aggregation (or finalized state when using[-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state) combinator).

**Syntax**

``` sql
finalizeAggregation(state)
```

**Arguments**

-   `state` — State of aggregation. [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction).

**Returned value(s)**

-   Value/values that was aggregated.

Type: Value of any types that was aggregated.

**Examples**

Query:

```sql
SELECT finalizeAggregation(( SELECT countState(number) FROM numbers(10)));
```

Result:

```text
┌─finalizeAggregation(_subquery16)─┐
│                               10 │
└──────────────────────────────────┘
```

Query:

```sql
SELECT finalizeAggregation(( SELECT sumState(number) FROM numbers(10)));
```

Result:

```text
┌─finalizeAggregation(_subquery20)─┐
│                               45 │
└──────────────────────────────────┘
```

Note that `NULL` values are ignored.

Query:

```sql
SELECT finalizeAggregation(arrayReduce('anyState', [NULL, 2, 3]));
```

Result:

```text
┌─finalizeAggregation(arrayReduce('anyState', [NULL, 2, 3]))─┐
│                                                          2 │
└────────────────────────────────────────────────────────────┘
```

Combined example:

Query:

```sql
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(10);
```

Result:

```text
┌─number─┬─one_row_sum─┬─cumulative_sum─┐
│      0 │           0 │              0 │
│      1 │           1 │              1 │
│      2 │           2 │              3 │
│      3 │           3 │              6 │
│      4 │           4 │             10 │
│      5 │           5 │             15 │
│      6 │           6 │             21 │
│      7 │           7 │             28 │
│      8 │           8 │             36 │
│      9 │           9 │             45 │
└────────┴─────────────┴────────────────┘
```

**See Also**
-   [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)
-   [initializeAggregation](#initializeaggregation)

## runningAccumulate

Accumulates states of an aggregate function for each row of a data block.

:::warning    
The state is reset for each new data block.
:::

**Syntax**

``` sql
runningAccumulate(agg_state[, grouping]);
```

**Arguments**

-   `agg_state` — State of the aggregate function. [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction).
-   `grouping` — Grouping key. Optional. The state of the function is reset if the `grouping` value is changed. It can be any of the [supported data types](../../sql-reference/data-types/index.md) for which the equality operator is defined.

**Returned value**

-   Each resulting row contains a result of the aggregate function, accumulated for all the input rows from 0 to the current position. `runningAccumulate` resets states for each new data block or when the `grouping` value changes.

Type depends on the aggregate function used.

**Examples**

Consider how you can use `runningAccumulate` to find the cumulative sum of numbers without and with grouping.

Query:

``` sql
SELECT k, runningAccumulate(sum_k) AS res FROM (SELECT number as k, sumState(k) AS sum_k FROM numbers(10) GROUP BY k ORDER BY k);
```

Result:

``` text
┌─k─┬─res─┐
│ 0 │   0 │
│ 1 │   1 │
│ 2 │   3 │
│ 3 │   6 │
│ 4 │  10 │
│ 5 │  15 │
│ 6 │  21 │
│ 7 │  28 │
│ 8 │  36 │
│ 9 │  45 │
└───┴─────┘
```

The subquery generates `sumState` for every number from `0` to `9`. `sumState` returns the state of the [sum](../../sql-reference/aggregate-functions/reference/sum.md) function that contains the sum of a single number.

The whole query does the following:

1.  For the first row, `runningAccumulate` takes `sumState(0)` and returns `0`.
2.  For the second row, the function merges `sumState(0)` and `sumState(1)` resulting in `sumState(0 + 1)`, and returns `1` as a result.
3.  For the third row, the function merges `sumState(0 + 1)` and `sumState(2)` resulting in `sumState(0 + 1 + 2)`, and returns `3` as a result.
4.  The actions are repeated until the block ends.

The following example shows the `groupping` parameter usage:

Query:

``` sql
SELECT
    grouping,
    item,
    runningAccumulate(state, grouping) AS res
FROM
(
    SELECT
        toInt8(number / 4) AS grouping,
        number AS item,
        sumState(number) AS state
    FROM numbers(15)
    GROUP BY item
    ORDER BY item ASC
);
```

Result:

``` text
┌─grouping─┬─item─┬─res─┐
│        0 │    0 │   0 │
│        0 │    1 │   1 │
│        0 │    2 │   3 │
│        0 │    3 │   6 │
│        1 │    4 │   4 │
│        1 │    5 │   9 │
│        1 │    6 │  15 │
│        1 │    7 │  22 │
│        2 │    8 │   8 │
│        2 │    9 │  17 │
│        2 │   10 │  27 │
│        2 │   11 │  38 │
│        3 │   12 │  12 │
│        3 │   13 │  25 │
│        3 │   14 │  39 │
└──────────┴──────┴─────┘
```

As you can see, `runningAccumulate` merges states for each group of rows separately.

## joinGet

The function lets you extract data from the table the same way as from a [dictionary](../../sql-reference/dictionaries/index.md).

Gets data from [Join](../../engines/table-engines/special/join.md#creating-a-table) tables using the specified join key.

Only supports tables created with the `ENGINE = Join(ANY, LEFT, <join_keys>)` statement.

**Syntax**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Arguments**

-   `join_storage_table_name` — an [identifier](../../sql-reference/syntax.md#syntax-identifiers) indicates where search is performed. The identifier is searched in the default database (see parameter `default_database` in the config file). To override the default database, use the `USE db_name` or specify the database and the table through the separator `db_name.db_table`, see the example.
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**Returned value**

Returns list of values corresponded to list of keys.

If certain does not exist in source table then `0` or `null` will be returned based on [join_use_nulls](../../operations/settings/settings.md#join_use_nulls) setting.

More info about `join_use_nulls` in [Join operation](../../engines/table-engines/special/join.md).

**Example**

Input table:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

Query:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

Result:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model_name, …)

Evaluate external model.
Accepts a model name and model arguments. Returns Float64.

## throwIf(x\[, custom_message\])

Throw an exception if the argument is non zero.
custom_message - is an optional parameter: a constant string, provides an error message

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identity

Returns the same value that was used as its argument. Used for debugging and testing, allows to cancel using index, and get the query performance of a full scan. When query is analyzed for possible use of index, the analyzer does not look inside `identity` functions. Also constant folding is not applied too.

**Syntax**

``` sql
identity(x)
```

**Example**

Query:

``` sql
SELECT identity(42)
```

Result:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## randomPrintableASCII

Generates a string with a random set of [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) printable characters.

**Syntax**

``` sql
randomPrintableASCII(length)
```

**Arguments**

-   `length` — Resulting string length. Positive integer.

        If you pass `length < 0`, behavior of the function is undefined.

**Returned value**

-   String with a random set of [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) printable characters.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

## randomString

Generates a binary string of the specified length filled with random bytes (including zero bytes).

**Syntax**

``` sql
randomString(length)
```

**Arguments**

-   `length` — String length. Positive integer.

**Returned value**

-   String filled with random bytes.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT randomString(30) AS str, length(str) AS len FROM numbers(2) FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
str: 3 G  :   pT ?w тi  k aV f6
len: 30

Row 2:
──────
str: 9 ,]    ^   )  ]??  8
len: 30
```

**See Also**

-   [generateRandom](../../sql-reference/table-functions/generate.md#generaterandom)
-   [randomPrintableASCII](../../sql-reference/functions/other-functions.md#randomascii)


## randomFixedString

Generates a binary string of the specified length filled with random bytes (including zero bytes).

**Syntax**

``` sql
randomFixedString(length);
```

**Arguments**

-   `length` — String length in bytes. [UInt64](../../sql-reference/data-types/int-uint.md).

**Returned value(s)**

-   String filled with random bytes.

Type: [FixedString](../../sql-reference/data-types/fixedstring.md).

**Example**

Query:

```sql
SELECT randomFixedString(13) as rnd, toTypeName(rnd)
```

Result:

```text
┌─rnd──────┬─toTypeName(randomFixedString(13))─┐
│ j▒h㋖HɨZ'▒ │ FixedString(13)                 │
└──────────┴───────────────────────────────────┘

```

## randomStringUTF8

Generates a random string of a specified length. Result string contains valid UTF-8 code points. The value of code points may be outside of the range of assigned Unicode.

**Syntax**

``` sql
randomStringUTF8(length);
```

**Arguments**

-   `length` — Required length of the resulting string in code points. [UInt64](../../sql-reference/data-types/int-uint.md).

**Returned value(s)**

-   UTF-8 random string.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

```sql
SELECT randomStringUTF8(13)
```

Result:

```text
┌─randomStringUTF8(13)─┐
│ 𘤗𙉝д兠庇󡅴󱱎󦐪􂕌𔊹𓰛   │
└──────────────────────┘

```

## getSetting

Returns the current value of a [custom setting](../../operations/settings/index.md#custom_settings).

**Syntax**

```sql
getSetting('custom_setting');
```

**Parameter**

-   `custom_setting` — The setting name. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   The setting current value.

**Example**

```sql
SET custom_a = 123;
SELECT getSetting('custom_a');
```

**Result**

```
123
```

**See Also**

-   [Custom Settings](../../operations/settings/index.md#custom_settings)

## isDecimalOverflow

Checks whether the [Decimal](../../sql-reference/data-types/decimal.md) value is out of its (or specified) precision.

**Syntax**

``` sql
isDecimalOverflow(d, [p])
```

**Arguments**

-   `d` — value. [Decimal](../../sql-reference/data-types/decimal.md).
-   `p` — precision. Optional. If omitted, the initial precision of the first argument is used. Using of this paratemer could be helpful for data extraction to another DBMS or file. [UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Returned values**

-   `1` — Decimal value has more digits then it's precision allow,
-   `0` — Decimal value satisfies the specified precision.

**Example**

Query:

``` sql
SELECT isDecimalOverflow(toDecimal32(1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(1000000000, 0)),
       isDecimalOverflow(toDecimal32(-1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(-1000000000, 0));
```

Result:

``` text
1	1	1	1
```

## countDigits

Returns number of decimal digits you need to represent the value.

**Syntax**

``` sql
countDigits(x)
```

**Arguments**

-   `x` — [Int](../../sql-reference/data-types/int-uint.md) or [Decimal](../../sql-reference/data-types/decimal.md) value.

**Returned value**

Number of digits.

Type: [UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges).

:::note    
For `Decimal` values takes into account their scales: calculates result over underlying integer type which is `(value * scale)`. For example: `countDigits(42) = 2`, `countDigits(42.000) = 5`, `countDigits(0.04200) = 4`. I.e. you may check decimal overflow for `Decimal64` with `countDecimal(x) > 18`. It's a slow variant of [isDecimalOverflow](#is-decimal-overflow).
:::

**Example**

Query:

``` sql
SELECT countDigits(toDecimal32(1, 9)), countDigits(toDecimal32(-1, 9)),
       countDigits(toDecimal64(1, 18)), countDigits(toDecimal64(-1, 18)),
       countDigits(toDecimal128(1, 38)), countDigits(toDecimal128(-1, 38));
```

Result:

``` text
10	10	19	19	39	39
```

## errorCodeToName

**Returned value**

-   Variable name for the error code.

Type: [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md).

**Syntax**

``` sql
errorCodeToName(1)
```

Result:

``` text
UNSUPPORTED_METHOD
```

## tcpPort

Returns [native interface](../../interfaces/tcp.md) TCP port number listened by this server.
If it is executed in the context of a distributed table, then it generates a normal column, otherwise it produces a constant value.

**Syntax**

``` sql
tcpPort()
```

**Arguments**

-   None.

**Returned value**

-   The TCP port number.

Type: [UInt16](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT tcpPort();
```

Result:

``` text
┌─tcpPort()─┐
│      9000 │
└───────────┘
```

**See Also**

-   [tcp_port](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port)

## currentProfiles

Returns a list of the current [settings profiles](../../operations/access-rights.md#settings-profiles-management) for the current user. 

The command [SET PROFILE](../../sql-reference/statements/set.md#query-set) could be used to change the current setting profile. If the command `SET PROFILE` was not used the function returns the profiles specified at the current user's definition (see [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement)).

**Syntax**

``` sql
currentProfiles()
```

**Returned value**

-   List of the current user settings profiles. 

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## enabledProfiles

 Returns settings profiles, assigned to the current user both explicitly and implicitly. Explicitly assigned profiles are the same as returned by the [currentProfiles](#current-profiles) function. Implicitly assigned profiles include parent profiles of other assigned profiles, profiles assigned via granted roles, profiles assigned via their own settings, and the main default profile (see the `default_profile` section in the main server configuration file).

**Syntax**

``` sql
enabledProfiles()
```

**Returned value**

-   List of the enabled settings profiles. 

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## defaultProfiles

Returns all the profiles specified at the current user's definition (see [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement) statement).

**Syntax**

``` sql
defaultProfiles()
```

**Returned value**

-   List of the default settings profiles. 

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## currentRoles

Returns the names of the roles which are current for the current user. The current roles can be changed by the [SET ROLE](../../sql-reference/statements/set-role.md#set-role-statement) statement. If the `SET ROLE` statement was not used, the function `currentRoles` returns the same as `defaultRoles`.

**Syntax**

``` sql
currentRoles()
```

**Returned value**

-   List of the current roles for the current user. 

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## enabledRoles

Returns the names of the current roles and the roles, granted to some of the current roles.

**Syntax**

``` sql
enabledRoles()
```

**Returned value**

-   List of the enabled roles for the current user. 

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## defaultRoles

Returns the names of the roles which are enabled by default for the current user when he logins. Initially these are all roles granted to the current user (see [GRANT](../../sql-reference/statements/grant/#grant-select)), but that can be changed with the [SET DEFAULT ROLE](../../sql-reference/statements/set-role.md#set-default-role-statement) statement. 

**Syntax**

``` sql
defaultRoles()
```

**Returned value**

-   List of the default roles for the current user. 

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## getServerPort

Returns the number of the server port. When the port is not used by the server, throws an exception.

**Syntax**

``` sql
getServerPort(port_name)
```

**Arguments**

-   `port_name` — The name of the server port. [String](../../sql-reference/data-types/string.md#string). Possible values:

    -   'tcp_port'
    -   'tcp_port_secure'
    -   'http_port'
    -   'https_port'
    -   'interserver_http_port'
    -   'interserver_https_port'
    -   'mysql_port'
    -   'postgresql_port'
    -   'grpc_port'
    -   'prometheus.port'

**Returned value**

-   The number of the server port.

Type: [UInt16](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT getServerPort('tcp_port');
```

Result:

``` text
┌─getServerPort('tcp_port')─┐
│ 9000                      │
└───────────────────────────┘
```

## queryID

Returns the ID of the current query. Other parameters of a query can be extracted from the [system.query_log](../../operations/system-tables/query_log.md) table via `query_id`.

In contrast to [initialQueryID](#initial-query-id) function, `queryID` can return different results on different shards (see example).

**Syntax**

``` sql
queryID()
```

**Returned value**

-   The ID of the current query.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

Query:

``` sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT queryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

Result:

``` text
┌─count()─┐
│ 3       │
└─────────┘
```

## initialQueryID

Returns the ID of the initial current query. Other parameters of a query can be extracted from the [system.query_log](../../operations/system-tables/query_log.md) table via `initial_query_id`.

In contrast to [queryID](#query-id) function, `initialQueryID` returns the same results on different shards (see example).

**Syntax**

``` sql
initialQueryID()
```

**Returned value**

-   The ID of the initial current query.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

Query:

``` sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT initialQueryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

Result:

``` text
┌─count()─┐
│ 1       │
└─────────┘
```

## shardNum

Returns the index of a shard which processes a part of data for a distributed query. Indices are started from `1`.
If a query is not distributed then constant value `0` is returned.

**Syntax**

``` sql
shardNum()
```

**Returned value**

-   Shard index or constant `0`.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Example**

In the following example a configuration with two shards is used. The query is executed on the [system.one](../../operations/system-tables/one.md) table on every shard.

Query:

``` sql
CREATE TABLE shard_num_example (dummy UInt8) 
    ENGINE=Distributed(test_cluster_two_shards_localhost, system, one, dummy);
SELECT dummy, shardNum(), shardCount() FROM shard_num_example;
```

Result:

``` text
┌─dummy─┬─shardNum()─┬─shardCount()─┐
│     0 │          2 │            2 │
│     0 │          1 │            2 │
└───────┴────────────┴──────────────┘
```

**See Also**

-   [Distributed Table Engine](../../engines/table-engines/special/distributed.md)

## shardCount

Returns the total number of shards for a distributed query.
If a query is not distributed then constant value `0` is returned.

**Syntax**

``` sql
shardCount()
```

**Returned value**

-   Total number of shards or `0`.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**See Also**

- [shardNum()](#shard-num) function example also contains `shardCount()` function call.

## getOSKernelVersion

Returns a string with the current OS kernel version.

**Syntax**

``` sql
getOSKernelVersion()
```

**Arguments**

-   None.

**Returned value**

-   The current OS kernel version.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT getOSKernelVersion();
```

Result:

``` text
┌─getOSKernelVersion()────┐
│ Linux 4.15.0-55-generic │
└─────────────────────────┘
```

## zookeeperSessionUptime

Returns the uptime of the current ZooKeeper session in seconds.

**Syntax**

``` sql
zookeeperSessionUptime()
```

**Arguments**

-   None.

**Returned value**

-   Uptime of the current ZooKeeper session in seconds.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT zookeeperSessionUptime();
```

Result:

``` text
┌─zookeeperSessionUptime()─┐
│                      286 │
└──────────────────────────┘
```
