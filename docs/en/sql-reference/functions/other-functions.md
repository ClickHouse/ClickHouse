---
slug: /en/sql-reference/functions/other-functions
sidebar_position: 140
sidebar_label: Other
---

# Other Functions

## hostName()

Returns the name of the host on which this function was executed. If the function executes on a remote server (distributed processing), the remote server name is returned.
If the function executes in the context of a distributed table, it generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

## getMacro {#getMacro}

Returns a named value from the [macros](../../operations/server-configuration-parameters/settings.md#macros) section of the server configuration.

**Syntax**

```sql
getMacro(name);
```

**Arguments**

- `name` — Macro name to retrieve from the `<macros>` section. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

- Value of the specified macro.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Example `<macros>` section in the server configuration file:

```xml
<macros>
    <test>Value</test>
</macros>
```

Query:

```sql
SELECT getMacro('test');
```

Result:

```text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

The same value can be retrieved as follows:

```sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

```text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## FQDN

Returns the fully qualified domain name of the ClickHouse server.

**Syntax**

```sql
fqdn();
```

This function is case-insensitive.

**Returned value**

- String with the fully qualified domain name.

Type: `String`.

**Example**

```sql
SELECT FQDN();
```

Result:

```text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename

Extracts the tail of a string following its last slash or backslash. This function if often used to extract the filename from a path.

```sql
basename(expr)
```

**Arguments**

- `expr` — A value of type [String](../../sql-reference/data-types/string.md). Backslashes must be escaped.

**Returned Value**

A string that contains:

- The tail of the input string after its last slash or backslash. If the input string ends with a slash or backslash (e.g. `/` or `c:\`), the function returns an empty string.
- The original string if there are no slashes or backslashes.

**Example**

Query:

```sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

Result:

```text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

Query:

```sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

Result:

```text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

Query:

```sql
SELECT 'some-file-name' AS a, basename(a)
```

Result:

```text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x)

Calculates the approximate width when outputting values to the console in text format (tab-separated).
This function is used by the system to implement Pretty formats.

`NULL` is represented as a string corresponding to `NULL` in `Pretty` formats.

```sql
SELECT visibleWidth(NULL)
```

```text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName(x)

Returns the type name of the passed argument.

If `NULL` is passed, then the function returns type `Nullable(Nothing)`, which corresponds to ClickHouse's internal `NULL` representation.

## blockSize() {#blockSize}

In ClickHouse, queries are processed in blocks (chunks).
This function returns the size (row count) of the block the function is called on.

## byteSize

Returns an estimation of uncompressed byte size of its arguments in memory.

**Syntax**

```sql
byteSize(argument [, ...])
```

**Arguments**

- `argument` — Value.

**Returned value**

- Estimation of byte size of the arguments in memory.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Examples**

For [String](../../sql-reference/data-types/string.md) arguments, the function returns the string length + 9 (terminating zero + length).

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

```text
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

If the function has multiple arguments, the function accumulates their byte sizes.

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

Turns a constant into a full column containing a single value.
Full columns and constants are represented differently in memory. Functions usually execute different code for normal and constant arguments, although the result should typically be the same. This function can be used to debug this behavior.

## ignore(…)

Accepts any arguments, including `NULL` and does nothing. Always returns 0.
The argument is internally still evaluated. Useful e.g. for benchmarks.

## sleep

Used to introduce a delay or pause in the execution of a query. It is primarily used for testing and debugging purposes.

**Syntax**

```sql
sleep(seconds)
```

**Arguments**

- `seconds`: [UInt*](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md) The number of seconds to pause the query execution to a maximum of 3 seconds. It can be a floating-point value to specify fractional seconds.

**Returned value**

This function does not return any value.

**Example**

```sql
SELECT sleep(2);
```

This function does not return any value. However, if you run the function with `clickhouse client` you will see something similar to:

```response
SELECT sleep(2)

Query id: 8aa9943e-a686-45e1-8317-6e8e3a5596ac

┌─sleep(2)─┐
│        0 │
└──────────┘

1 row in set. Elapsed: 2.012 sec.
```

This query will pause for 2 seconds before completing. During this time, no results will be returned, and the query will appear to be hanging or unresponsive.

**Implementation details**

The `sleep()` function is generally not used in production environments, as it can negatively impact query performance and system responsiveness. However, it can be useful in the following scenarios:

1. **Testing**: When testing or benchmarking ClickHouse, you may want to simulate delays or introduce pauses to observe how the system behaves under certain conditions.
2. **Debugging**: If you need to examine the state of the system or the execution of a query at a specific point in time, you can use `sleep()` to introduce a pause, allowing you to inspect or collect relevant information.
3. **Simulation**: In some cases, you may want to simulate real-world scenarios where delays or pauses occur, such as network latency or external system dependencies.

It's important to use the `sleep()` function judiciously and only when necessary, as it can potentially impact the overall performance and responsiveness of your ClickHouse system.

## sleepEachRow

Pauses the execution of a query for a specified number of seconds for each row in the result set.

**Syntax**

```sql
sleepEachRow(seconds)
```

**Arguments**

- `seconds`: [UInt*](../../sql-reference/data-types/int-uint.md) or [Float*](../../sql-reference/data-types/float.md) The number of seconds to pause the query execution for each row in the result set to a maximum of 3 seconds. It can be a floating-point value to specify fractional seconds.

**Returned value**

This function returns the same input values as it receives, without modifying them.

**Example**

```sql
SELECT number, sleepEachRow(0.5) FROM system.numbers LIMIT 5;
```

```response
┌─number─┬─sleepEachRow(0.5)─┐
│      0 │                 0 │
│      1 │                 0 │
│      2 │                 0 │
│      3 │                 0 │
│      4 │                 0 │
└────────┴───────────────────┘
```

But the output will be delayed, with a 0.5-second pause between each row.

The `sleepEachRow()` function is primarily used for testing and debugging purposes, similar to the `sleep()` function. It allows you to simulate delays or introduce pauses in the processing of each row, which can be useful in scenarios such as:

1. **Testing**: When testing or benchmarking ClickHouse's performance under specific conditions, you can use `sleepEachRow()` to simulate delays or introduce pauses for each row processed.
2. **Debugging**: If you need to examine the state of the system or the execution of a query for each row processed, you can use `sleepEachRow()` to introduce pauses, allowing you to inspect or collect relevant information.
3. **Simulation**: In some cases, you may want to simulate real-world scenarios where delays or pauses occur for each row processed, such as when dealing with external systems or network latencies.

Like the [`sleep()` function](#sleep), it's important to use `sleepEachRow()` judiciously and only when necessary, as it can significantly impact the overall performance and responsiveness of your ClickHouse system, especially when dealing with large result sets.

## currentDatabase()

Returns the name of the current database.
Useful in table engine parameters of `CREATE TABLE` queries where you need to specify the database.

## currentUser() {#currentUser}

Returns the name of the current user. In case of a distributed query, the name of the user who initiated the query is returned.

```sql
SELECT currentUser();
```

Aliases: `user()`, `USER()`, `current_user()`. Aliases are case insensitive.

**Returned values**

- The name of the current user.
- In distributed queries, the login of the user who initiated the query.

Type: `String`.

**Example**

```sql
SELECT currentUser();
```

Result:

```text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## isConstant

Returns whether the argument is a constant expression.

A constant expression is an expression whose result is known during query analysis, i.e. before execution. For example, expressions over [literals](../../sql-reference/syntax.md#literals) are constant expressions.

This function is mostly intended for development, debugging and demonstration.

**Syntax**

```sql
isConstant(x)
```

**Arguments**

- `x` — Expression to check.

**Returned values**

- `1` if `x` is constant.
- `0` if `x` is non-constant.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Examples**

Query:

```sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

Result:

```text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Query:

```sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

Result:

```text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

Query:

```sql
SELECT isConstant(number) FROM numbers(1)
```

Result:

```text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## isFinite(x)

Returns 1 if the Float32 or Float64 argument not infinite and not a NaN, otherwise this function returns 0.

## isInfinite(x)

Returns 1 if the Float32 or Float64 argument is infinite, otherwise this function returns 0. Note that 0 is returned for a NaN.

## ifNotFinite

Checks whether a floating point value is finite.

**Syntax**

```sql
ifNotFinite(x,y)
```

**Arguments**

- `x` — Value to check for infinity. Type: [Float\*](../../sql-reference/data-types/float.md).
- `y` — Fallback value. Type: [Float\*](../../sql-reference/data-types/float.md).

**Returned value**

- `x` if `x` is finite.
- `y` if `x` is not finite.

**Example**

Query:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Result:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

You can get similar result by using the [ternary operator](../../sql-reference/functions/conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## isNaN(x)

Returns 1 if the Float32 and Float64 argument is NaN, otherwise this function 0.

## hasColumnInTable

Given the database name, the table name, and the column name as constant strings, returns 1 if the given column exists, otherwise 0.

**Syntax**

```sql
hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’)
```

**Parameters**

- `database` : name of the database. [String literal](../syntax#syntax-string-literal)
- `table` : name of the table. [String literal](../syntax#syntax-string-literal) 
- `column` : name of the column. [String literal](../syntax#syntax-string-literal)
- `hostname` : remote server name to perform the check on. [String literal](../syntax#syntax-string-literal)
- `username` : username for remote server. [String literal](../syntax#syntax-string-literal)
- `password` : password for remote server. [String literal](../syntax#syntax-string-literal)

**Returned value**

- `1` if the given column exists.
- `0`, otherwise. 

**Implementation details**

For elements in a nested data structure, the function checks for the existence of a column. For the nested data structure itself, the function returns 0.

**Example**

Query:

```sql
SELECT hasColumnInTable('system','metrics','metric')
```

```response
1
```

```sql
SELECT hasColumnInTable('system','metrics','non-existing_column')
```

```response
0
```

## hasThreadFuzzer

Returns whether Thread Fuzzer is effective. It can be used in tests to prevent runs from being too long.

**Syntax**

```sql
hasThreadFuzzer();
```

## bar

Builds a bar chart.

`bar(x, min, max, width)` draws a band with width proportional to `(x - min)` and equal to `width` characters when `x = max`.

**Arguments**

- `x` — Size to display.
- `min, max` — Integer constants. The value must fit in `Int64`.
- `width` — Constant, positive integer, can be fractional.

The band is drawn with accuracy to one eighth of a symbol.

Example:

```sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

```text
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

`array_from` – Constant array of values to convert.

`array_to` – Constant array of values to convert the values in ‘from’ to.

`default` – Which value to use if ‘x’ is not equal to any of the values in ‘from’.

`array_from` and `array_to` must have equally many elements.

Signature:

For `x` equal to one of the elements in `array_from`, the function returns the corresponding element in `array_to`, i.e. the one at the same array index. Otherwise, it returns `default`. If multiple matching elements exist `array_from`, it returns the element corresponding to the first of them.

`transform(T, Array(T), Array(U), U) -> U`

`T` and `U` can be numeric, string, or Date or DateTime types.
The same letter (T or U) means that types must be mutually compatible and not necessarily equal.
For example, the first argument could have type `Int64`, while the second argument could have type `Array(UInt16)`.

Example:

```sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

```text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### transform(x, array_from, array_to)

Similar to the other variation but has no ‘default’ argument. In case no match can be found, `x` is returned.

Example:

```sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vkontakte.ru'], ['www.yandex', 'example.com', 'vk.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

```text
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

## formatReadableDecimalSize(x)

Given a size (number of bytes), this function returns a readable, rounded size with suffix (KB, MB, etc.) as string.

Example:

```sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableDecimalSize(filesize_bytes) AS filesize
```

```text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.02 KB   │
│        1048576 │ 1.05 MB   │
│      192851925 │ 192.85 MB │
└────────────────┴────────────┘
```

## formatReadableSize(x)

Given a size (number of bytes), this function returns a readable, rounded size with suffix (KiB, MiB, etc.) as string.

Example:

```sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

Alias: `FORMAT_BYTES`.

```text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## formatReadableQuantity(x)

Given a number, this function returns a rounded number with suffix (thousand, million, billion, etc.) as string.

Example:

```sql
SELECT
    arrayJoin([1024, 1234 * 1000, (4567 * 1000) * 1000, 98765432101234]) AS number,
    formatReadableQuantity(number) AS number_for_humans
```

```text
┌─────────number─┬─number_for_humans─┐
│           1024 │ 1.02 thousand     │
│        1234000 │ 1.23 million      │
│     4567000000 │ 4.57 billion      │
│ 98765432101234 │ 98.77 trillion    │
└────────────────┴───────────────────┘
```

## formatReadableTimeDelta

Given a time interval (delta) in seconds, this function returns a time delta with year/month/day/hour/minute/second/millisecond/microsecond/nanosecond as string.

**Syntax**

```sql
formatReadableTimeDelta(column[, maximum_unit, minimum_unit])
```

**Arguments**

- `column` — A column with a numeric time delta.
- `maximum_unit` — Optional. Maximum unit to show.
  - Acceptable values: `nanoseconds`, `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `months`, `years`.
  - Default value: `years`.
- `minimum_unit` — Optional. Minimum unit to show. All smaller units are truncated.
  - Acceptable values: `nanoseconds`, `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `months`, `years`.
  - If explicitly specified value is bigger than `maximum_unit`, an exception will be thrown.
  - Default value: `seconds` if `maximum_unit` is `seconds` or bigger, `nanoseconds` otherwise.

**Example**

```sql
SELECT
    arrayJoin([100, 12345, 432546534]) AS elapsed,
    formatReadableTimeDelta(elapsed) AS time_delta
```

```text
┌────elapsed─┬─time_delta ─────────────────────────────────────────────────────┐
│        100 │ 1 minute and 40 seconds                                         │
│      12345 │ 3 hours, 25 minutes and 45 seconds                              │
│  432546534 │ 13 years, 8 months, 17 days, 7 hours, 48 minutes and 54 seconds │
└────────────┴─────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    arrayJoin([100, 12345, 432546534]) AS elapsed,
    formatReadableTimeDelta(elapsed, 'minutes') AS time_delta
```

```text
┌────elapsed─┬─time_delta ─────────────────────────────────────────────────────┐
│        100 │ 1 minute and 40 seconds                                         │
│      12345 │ 205 minutes and 45 seconds                                      │
│  432546534 │ 7209108 minutes and 54 seconds                                  │
└────────────┴─────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    arrayJoin([100, 12345, 432546534.00000006]) AS elapsed,
    formatReadableTimeDelta(elapsed, 'minutes', 'nanoseconds') AS time_delta
```

```text
┌────────────elapsed─┬─time_delta─────────────────────────────────────┐
│                100 │ 1 minute and 40 seconds                        │
│              12345 │ 205 minutes and 45 seconds                     │
│ 432546534.00000006 │ 7209108 minutes, 54 seconds and 60 nanoseconds │
└────────────────────┴────────────────────────────────────────────────┘
```

## parseTimeDelta

Parse a sequence of numbers followed by something resembling a time unit.

**Syntax**

```sql
parseTimeDelta(timestr)
```

**Arguments**

- `timestr` — A sequence of numbers followed by something resembling a time unit.

**Returned value**

- A floating-point number with the number of seconds.

**Example**

```sql
SELECT parseTimeDelta('11s+22min')
```

```text
┌─parseTimeDelta('11s+22min')─┐
│                        1331 │
└─────────────────────────────┘
```

```sql
SELECT parseTimeDelta('1yr2mo')
```

```text
┌─parseTimeDelta('1yr2mo')─┐
│                 36806400 │
└──────────────────────────┘
```

## least(a, b)

Returns the smaller value of a and b.

## greatest(a, b)

Returns the larger value of a and b.

## uptime()

Returns the server’s uptime in seconds.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

**Syntax**

``` sql
uptime()
```

**Returned value**

- Time value of seconds.

Type: [UInt32](/docs/en/sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT uptime() as Uptime;
```

Result:

``` response
┌─Uptime─┐
│  55867 │
└────────┘
```

## version()

Returns the current version of ClickHouse as a string in the form of:

- Major version
- Minor version
- Patch version
- Number of commits since the previous stable release.

```plaintext
major_version.minor_version.patch_version.number_of_commits_since_the_previous_stable_release
```

If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard. Otherwise, it produces a constant value.

**Syntax**

```sql
version()
```

**Arguments**

None.

**Returned value**

Type: [String](../data-types/string)

**Implementation details**

None.

**Example**

Query:

```sql
SELECT version()
```

**Result**:

```response
┌─version()─┐
│ 24.2.1.1  │
└───────────┘
```

## buildId()

Returns the build ID generated by a compiler for the running ClickHouse server binary.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

## blockNumber()

Returns the sequence number of the data block where the row is located.

## rowNumberInBlock() {#rowNumberInBlock}

Returns the ordinal number of the row in the data block. Different data blocks are always recalculated.

## rowNumberInAllBlocks()

Returns the ordinal number of the row in the data block. This function only considers the affected data blocks.

## neighbor

The window function that provides access to a row at a specified offset before or after the current row of a given column.

**Syntax**

```sql
neighbor(column, offset[, default_value])
```

The result of the function depends on the affected data blocks and the order of data in the block.

:::note
Only returns neighbor inside the currently processed data block.
:::

The order of rows during calculation of `neighbor()` can differ from the order of rows returned to the user.
To prevent that you can create a subquery with [ORDER BY](../../sql-reference/statements/select/order-by.md) and call the function from outside the subquery.

**Arguments**

- `column` — A column name or scalar expression.
- `offset` — The number of rows to look before or ahead of the current row in `column`. [Int64](../../sql-reference/data-types/int-uint.md).
- `default_value` — Optional. The returned value if offset is beyond the block boundaries. Type of data blocks affected.

**Returned values**

- Value of `column` with `offset` distance from current row, if `offset` is not outside the block boundaries.
- The default value of `column` or `default_value` (if given), if `offset` is outside the block boundaries.

Type: type of data blocks affected or default value type.

**Example**

Query:

```sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Result:

```text
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

```sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

Result:

```text
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

```sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

Result:

```text
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

## runningDifference(x) {#runningDifference}

Calculates the difference between two consecutive row values in the data block.
Returns 0 for the first row, and for subsequent rows the difference to the previous row.

:::note
Only returns differences inside the currently processed data block.
:::

The result of the function depends on the affected data blocks and the order of data in the block.

The order of rows during calculation of `runningDifference()` can differ from the order of rows returned to the user.
To prevent that you can create a subquery with [ORDER BY](../../sql-reference/statements/select/order-by.md) and call the function from outside the subquery.

Example:

```sql
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

```text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

Please note that the block size affects the result. The internal state of `runningDifference` state is reset for each new block.

```sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

```text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

```sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

```text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstValue

Same as [runningDifference](./other-functions.md#other_functions-runningdifference), but returns the value of the first row as the value on the first row.

## runningConcurrency

Calculates the number of concurrent events.
Each event has a start time and an end time. The start time is included in the event, while the end time is excluded. Columns with a start time and an end time must be of the same data type.
The function calculates the total number of active (concurrent) events for each event start time.

:::tip
Events must be ordered by the start time in ascending order. If this requirement is violated the function raises an exception. Every data block is processed separately. If events from different data blocks overlap then they can not be processed correctly.
:::

**Syntax**

```sql
runningConcurrency(start, end)
```

**Arguments**

- `start` — A column with the start time of events. [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), or [DateTime64](../../sql-reference/data-types/datetime64.md).
- `end` — A column with the end time of events. [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Returned values**

- The number of concurrent events at each event start time.

Type: [UInt32](../../sql-reference/data-types/int-uint.md)

**Example**

Consider the table:

```text
┌──────start─┬────────end─┐
│ 2021-03-03 │ 2021-03-11 │
│ 2021-03-06 │ 2021-03-12 │
│ 2021-03-07 │ 2021-03-08 │
│ 2021-03-11 │ 2021-03-12 │
└────────────┴────────────┘
```

Query:

```sql
SELECT start, runningConcurrency(start, end) FROM example_table;
```

Result:

```text
┌──────start─┬─runningConcurrency(start, end)─┐
│ 2021-03-03 │                              1 │
│ 2021-03-06 │                              2 │
│ 2021-03-07 │                              3 │
│ 2021-03-11 │                              2 │
└────────────┴────────────────────────────────┘
```

## MACNumToString(num)

Interprets a UInt64 number as a MAC address in big endian format. Returns the corresponding MAC address in format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form) as string.

## MACStringToNum(s)

The inverse function of MACNumToString. If the MAC address has an invalid format, it returns 0.

## MACStringToOUI(s)

Given a MAC address in format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form), returns the first three octets as a UInt64 number. If the MAC address has an invalid format, it returns 0.

## getSizeOfEnumType

Returns the number of fields in [Enum](../../sql-reference/data-types/enum.md).
An exception is thrown if the type is not `Enum`.

```sql
getSizeOfEnumType(value)
```

**Arguments:**

- `value` — Value of type `Enum`.

**Returned values**

- The number of fields with `Enum` input values.

**Example**

```sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

```text
┌─x─┐
│ 2 │
└───┘
```

## blockSerializedSize

Returns the size on disk without considering compression.

```sql
blockSerializedSize(value[, value[, ...]])
```

**Arguments**

- `value` — Any value.

**Returned values**

- The number of bytes that will be written to disk for block of values without compression.

**Example**

Query:

```sql
SELECT blockSerializedSize(maxState(1)) as x
```

Result:

```text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName

Returns the internal name of the data type that represents the value.

```sql
toColumnTypeName(value)
```

**Arguments:**

- `value` — Any type of value.

**Returned values**

- The internal data type name used to represent `value`.

**Example**

Difference between `toTypeName ' and ' toColumnTypeName`:

```sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

Result:

```text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

Query:

```sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

Result:

```text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

The example shows that the `DateTime` data type is internally stored as `Const(UInt32)`.

## dumpColumnStructure

Outputs a detailed description of data structures in RAM

```sql
dumpColumnStructure(value)
```

**Arguments:**

- `value` — Any type of value.

**Returned values**

- A description of the column structure used for representing `value`.

**Example**

```sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

```text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType

Returns the default value for the given data type.

Does not include default values for custom columns set by the user.

```sql
defaultValueOfArgumentType(expression)
```

**Arguments:**

- `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**Returned values**

- `0` for numbers.
- Empty string for strings.
- `ᴺᵁᴸᴸ` for [Nullable](../../sql-reference/data-types/nullable.md).

**Example**

Query:

```sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

Result:

```text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

Query:

```sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

Result:

```text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## defaultValueOfTypeName

Returns the default value for the given type name.

Does not include default values for custom columns set by the user.

```sql
defaultValueOfTypeName(type)
```

**Arguments:**

- `type` — A string representing a type name.

**Returned values**

- `0` for numbers.
- Empty string for strings.
- `ᴺᵁᴸᴸ` for [Nullable](../../sql-reference/data-types/nullable.md).

**Example**

Query:

```sql
SELECT defaultValueOfTypeName('Int8')
```

Result:

```text
┌─defaultValueOfTypeName('Int8')─┐
│                              0 │
└────────────────────────────────┘
```

Query:

```sql
SELECT defaultValueOfTypeName('Nullable(Int8)')
```

Result:

```text
┌─defaultValueOfTypeName('Nullable(Int8)')─┐
│                                     ᴺᵁᴸᴸ │
└──────────────────────────────────────────┘
```

## indexHint

This function is intended for debugging and introspection. It ignores its argument and always returns 1. The arguments are not evaluated.

But during index analysis, the argument of this function is assumed to be not wrapped in `indexHint`. This allows to select data in index ranges by the corresponding condition but without further filtering by this condition. The index in ClickHouse is sparse and using `indexHint` will yield more data than specifying the same condition directly.

**Syntax**

```sql
SELECT * FROM table WHERE indexHint(<expression>)
```

**Returned value**

Type: [Uint8](https://clickhouse.com/docs/en/data_types/int_uint/#diapazony-uint).

**Example**

Here is the example of test data from the table [ontime](../../getting-started/example-datasets/ontime.md).

Table:

```sql
SELECT count() FROM ontime
```

```text
┌─count()─┐
│ 4276457 │
└─────────┘
```

The table has indexes on the fields `(FlightDate, (Year, FlightDate))`.

Create a query which does not use the index:

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

To apply the index, select a specific date:

```sql
SELECT FlightDate AS k, count() FROM ontime WHERE k = '2017-09-15' GROUP BY k ORDER BY k
```

ClickHouse now uses the index to process a significantly smaller number of rows (`Processed 32.74 thousand rows`).

Result:

```text
┌──────────k─┬─count()─┐
│ 2017-09-15 │   16428 │
└────────────┴─────────┘
```

Now wrap the expression `k = '2017-09-15'` in function `indexHint`:

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

ClickHouse used the index the same way as previously (`Processed 32.74 thousand rows`).
The expression `k = '2017-09-15'` was not used when generating the result.
In example, the `indexHint` function allows to see adjacent dates.

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

Used for the internal implementation of [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin).

```sql
SELECT replicate(x, arr);
```

**Arguments:**

- `arr` — An array.
- `x` — The value to fill the result array with.

**Returned value**

An array of the lame length as `arr` filled with value `x`.

Type: `Array`.

**Example**

Query:

```sql
SELECT replicate(1, ['a', 'b', 'c'])
```

Result:

```text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable

Returns the amount of free space in the filesystem hosting the database persistence. The returned value is always smaller than total free space ([filesystemFree](#filesystemfree)) because some space is reserved for the operating system.

**Syntax**

```sql
filesystemAvailable()
```

**Returned value**

- The amount of remaining space available in bytes.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space";
```

Result:

```text
┌─Available space─┐
│ 30.75 GiB       │
└─────────────────┘
```

## filesystemFree

Returns the total amount of the free space on the filesystem hosting the database persistence. See also `filesystemAvailable`

**Syntax**

```sql
filesystemFree()
```

**Returned value**

- The amount of free space in bytes.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SELECT formatReadableSize(filesystemFree()) AS "Free space";
```

Result:

```text
┌─Free space─┐
│ 32.39 GiB  │
└────────────┘
```

## filesystemCapacity

Returns the capacity of the filesystem in bytes. Needs the [path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) to the data directory to be configured.

**Syntax**

```sql
filesystemCapacity()
```

**Returned value**

- Capacity of the filesystem in bytes.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity";
```

Result:

```text
┌─Capacity──┐
│ 39.32 GiB │
└───────────┘
```

## initializeAggregation

Calculates the result of an aggregate function based on a single value. This function can be used to initialize aggregate functions with combinator [-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state). You can create states of aggregate functions and insert them to columns of type [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction) or use initialized aggregates as default values.

**Syntax**

```sql
initializeAggregation (aggregate_function, arg1, arg2, ..., argN)
```

**Arguments**

- `aggregate_function` — Name of the aggregation function to initialize. [String](../../sql-reference/data-types/string.md).
- `arg` — Arguments of aggregate function.

**Returned value(s)**

- Result of aggregation for every row passed to the function.

The return type is the same as the return type of function, that `initializeAggregation` takes as first argument.

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

- [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)

## finalizeAggregation

Given a state of aggregate function, this function returns the result of aggregation (or finalized state when using a [-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state) combinator).

**Syntax**

```sql
finalizeAggregation(state)
```

**Arguments**

- `state` — State of aggregation. [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction).

**Returned value(s)**

- Value/values that was aggregated.

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

- [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)
- [initializeAggregation](#initializeaggregation)

## runningAccumulate

Accumulates the states of an aggregate function for each row of a data block.

:::note
The state is reset for each new block of data.
:::

**Syntax**

```sql
runningAccumulate(agg_state[, grouping]);
```

**Arguments**

- `agg_state` — State of the aggregate function. [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction).
- `grouping` — Grouping key. Optional. The state of the function is reset if the `grouping` value is changed. It can be any of the [supported data types](../../sql-reference/data-types/index.md) for which the equality operator is defined.

**Returned value**

- Each resulting row contains a result of the aggregate function, accumulated for all the input rows from 0 to the current position. `runningAccumulate` resets states for each new data block or when the `grouping` value changes.

Type depends on the aggregate function used.

**Examples**

Consider how you can use `runningAccumulate` to find the cumulative sum of numbers without and with grouping.

Query:

```sql
SELECT k, runningAccumulate(sum_k) AS res FROM (SELECT number as k, sumState(k) AS sum_k FROM numbers(10) GROUP BY k ORDER BY k);
```

Result:

```text
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

1. For the first row, `runningAccumulate` takes `sumState(0)` and returns `0`.
2. For the second row, the function merges `sumState(0)` and `sumState(1)` resulting in `sumState(0 + 1)`, and returns `1` as a result.
3. For the third row, the function merges `sumState(0 + 1)` and `sumState(2)` resulting in `sumState(0 + 1 + 2)`, and returns `3` as a result.
4. The actions are repeated until the block ends.

The following example shows the `groupping` parameter usage:

Query:

```sql
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

```text
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

Gets the data from [Join](../../engines/table-engines/special/join.md#creating-a-table) tables using the specified join key.

Only supports tables created with the `ENGINE = Join(ANY, LEFT, <join_keys>)` statement.

**Syntax**

```sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Arguments**

- `join_storage_table_name` — an [identifier](../../sql-reference/syntax.md#syntax-identifiers) indicating where the search is performed. The identifier is searched in the default database (see setting `default_database` in the config file). To override the default database, use `USE db_name` or specify the database and the table through the separator `db_name.db_table` as in the example.
- `value_column` — name of the column of the table that contains required data.
- `join_keys` — list of keys.

**Returned value**

Returns a list of values corresponded to list of keys.

If certain does not exist in source table then `0` or `null` will be returned based on [join_use_nulls](../../operations/settings/settings.md#join_use_nulls) setting.

More info about `join_use_nulls` in [Join operation](../../engines/table-engines/special/join.md).

**Example**

Input table:

```sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

```text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

Query:

```sql
SELECT joinGet(db_test.id_val, 'val', toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

Result:

```text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## catboostEvaluate(path_to_model, feature_1, feature_2, …, feature_n)

:::note
This function is not available in ClickHouse Cloud.
:::

Evaluate an external catboost model. [CatBoost](https://catboost.ai) is an open-source gradient boosting library developed by Yandex for machine learning.
Accepts a path to a catboost model and model arguments (features). Returns Float64.

```sql
SELECT feat1, ..., feat_n, catboostEvaluate('/path/to/model.bin', feat_1, ..., feat_n) AS prediction
FROM data_table
```

**Prerequisites**

1. Build the catboost evaluation library

Before evaluating catboost models, the `libcatboostmodel.<so|dylib>` library must be made available. See [CatBoost documentation](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html) how to compile it.

Next, specify the path to `libcatboostmodel.<so|dylib>` in the clickhouse configuration:

```xml
<clickhouse>
...
    <catboost_lib_path>/path/to/libcatboostmodel.so</catboost_lib_path>
...
</clickhouse>
```

For security and isolation reasons, the model evaluation does not run in the server process but in the clickhouse-library-bridge process.
At the first execution of `catboostEvaluate()`, the server starts the library bridge process if it is not running already. Both processes
communicate using a HTTP interface. By default, port `9012` is used. A different port can be specified as follows - this is useful if port
`9012` is already assigned to a different service.

```xml
<library_bridge>
    <port>9019</port>
</library_bridge>
```

2. Train a catboost model using libcatboost

See [Training and applying models](https://catboost.ai/docs/features/training.html#training) for how to train catboost models from a training data set.

## throwIf(x\[, message\[, error_code\]\])

Throw an exception if argument `x` is true.

**Arguments**

- `x` - the condition to check.
- `message` - a constant string providing a custom error message. Optional.
- `error_code` - A constant integer providing a custom error code. Optional.

To use the `error_code` argument, configuration parameter `allow_custom_error_code_in_throwif` must be enabled.

**Example**

```sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

Result:

```text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identity

Returns its argument. Intended for debugging and testing. Allows to cancel using index, and get the query performance of a full scan. When the query is analyzed for possible use of an index, the analyzer ignores everything in `identity` functions. Also disables constant folding.

**Syntax**

```sql
identity(x)
```

**Example**

Query:

```sql
SELECT identity(42);
```

Result:

```text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## getSetting

Returns the current value of a [custom setting](../../operations/settings/index.md#custom_settings).

**Syntax**

```sql
getSetting('custom_setting');
```

**Parameter**

- `custom_setting` — The setting name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- The setting's current value.

**Example**

```sql
SET custom_a = 123;
SELECT getSetting('custom_a');
```

Result:

```
123
```

**See Also**

- [Custom Settings](../../operations/settings/index.md#custom_settings)

## isDecimalOverflow

Checks whether the [Decimal](../../sql-reference/data-types/decimal.md) value is outside its precision or outside the specified precision.

**Syntax**

```sql
isDecimalOverflow(d, [p])
```

**Arguments**

- `d` — value. [Decimal](../../sql-reference/data-types/decimal.md).
- `p` — precision. Optional. If omitted, the initial precision of the first argument is used. This parameter can be helpful to migrate data from/to another database or file. [UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Returned values**

- `1` — Decimal value has more digits then allowed by its precision,
- `0` — Decimal value satisfies the specified precision.

**Example**

Query:

```sql
SELECT isDecimalOverflow(toDecimal32(1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(1000000000, 0)),
       isDecimalOverflow(toDecimal32(-1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(-1000000000, 0));
```

Result:

```text
1	1	1	1
```

## countDigits

Returns number of decimal digits need to represent a value.

**Syntax**

```sql
countDigits(x)
```

**Arguments**

- `x` — [Int](../../sql-reference/data-types/int-uint.md) or [Decimal](../../sql-reference/data-types/decimal.md) value.

**Returned value**

Number of digits.

Type: [UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges).

:::note
For `Decimal` values takes into account their scales: calculates result over underlying integer type which is `(value * scale)`. For example: `countDigits(42) = 2`, `countDigits(42.000) = 5`, `countDigits(0.04200) = 4`. I.e. you may check decimal overflow for `Decimal64` with `countDecimal(x) > 18`. It's a slow variant of [isDecimalOverflow](#is-decimal-overflow).
:::

**Example**

Query:

```sql
SELECT countDigits(toDecimal32(1, 9)), countDigits(toDecimal32(-1, 9)),
       countDigits(toDecimal64(1, 18)), countDigits(toDecimal64(-1, 18)),
       countDigits(toDecimal128(1, 38)), countDigits(toDecimal128(-1, 38));
```

Result:

```text
10	10	19	19	39	39
```

## errorCodeToName

Returns the textual name of an error code.

Type: [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md).

**Syntax**

```sql
errorCodeToName(1)
```

Result:

```text
UNSUPPORTED_METHOD
```

## tcpPort

Returns [native interface](../../interfaces/tcp.md) TCP port number listened by this server.
If executed in the context of a distributed table, this function generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

**Syntax**

```sql
tcpPort()
```

**Arguments**

- None.

**Returned value**

- The TCP port number.

Type: [UInt16](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SELECT tcpPort();
```

Result:

```text
┌─tcpPort()─┐
│      9000 │
└───────────┘
```

**See Also**

- [tcp_port](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port)

## currentProfiles

Returns a list of the current [settings profiles](../../guides/sre/user-management/index.md#settings-profiles-management) for the current user.

The command [SET PROFILE](../../sql-reference/statements/set.md#query-set) could be used to change the current setting profile. If the command `SET PROFILE` was not used the function returns the profiles specified at the current user's definition (see [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement)).

**Syntax**

```sql
currentProfiles()
```

**Returned value**

- List of the current user settings profiles.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## enabledProfiles

Returns settings profiles, assigned to the current user both explicitly and implicitly. Explicitly assigned profiles are the same as returned by the [currentProfiles](#current-profiles) function. Implicitly assigned profiles include parent profiles of other assigned profiles, profiles assigned via granted roles, profiles assigned via their own settings, and the main default profile (see the `default_profile` section in the main server configuration file).

**Syntax**

```sql
enabledProfiles()
```

**Returned value**

- List of the enabled settings profiles.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## defaultProfiles

Returns all the profiles specified at the current user's definition (see [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement) statement).

**Syntax**

```sql
defaultProfiles()
```

**Returned value**

- List of the default settings profiles.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## currentRoles

Returns the roles assigned to the current user. The roles can be changed by the [SET ROLE](../../sql-reference/statements/set-role.md#set-role-statement) statement. If no `SET ROLE` statement was not, the function `currentRoles` returns the same as `defaultRoles`.

**Syntax**

```sql
currentRoles()
```

**Returned value**

- A list of the current roles for the current user.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## enabledRoles

Returns the names of the current roles and the roles, granted to some of the current roles.

**Syntax**

```sql
enabledRoles()
```

**Returned value**

- List of the enabled roles for the current user.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## defaultRoles

Returns the roles which are enabled by default for the current user when he logs in. Initially these are all roles granted to the current user (see [GRANT](../../sql-reference/statements/grant.md#grant-select)), but that can be changed with the [SET DEFAULT ROLE](../../sql-reference/statements/set-role.md#set-default-role-statement) statement.

**Syntax**

```sql
defaultRoles()
```

**Returned value**

- List of the default roles for the current user.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## getServerPort

Returns the server port number. When the port is not used by the server, throws an exception.

**Syntax**

```sql
getServerPort(port_name)
```

**Arguments**

- `port_name` — The name of the server port. [String](../../sql-reference/data-types/string.md#string). Possible values:

  - 'tcp_port'
  - 'tcp_port_secure'
  - 'http_port'
  - 'https_port'
  - 'interserver_http_port'
  - 'interserver_https_port'
  - 'mysql_port'
  - 'postgresql_port'
  - 'grpc_port'
  - 'prometheus.port'

**Returned value**

- The number of the server port.

Type: [UInt16](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SELECT getServerPort('tcp_port');
```

Result:

```text
┌─getServerPort('tcp_port')─┐
│ 9000                      │
└───────────────────────────┘
```

## queryID {#queryID}

Returns the ID of the current query. Other parameters of a query can be extracted from the [system.query_log](../../operations/system-tables/query_log.md) table via `query_id`.

In contrast to [initialQueryID](#initial-query-id) function, `queryID` can return different results on different shards (see the example).

**Syntax**

```sql
queryID()
```

**Returned value**

- The ID of the current query.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

Query:

```sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT queryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

Result:

```text
┌─count()─┐
│ 3       │
└─────────┘
```

## initialQueryID

Returns the ID of the initial current query. Other parameters of a query can be extracted from the [system.query_log](../../operations/system-tables/query_log.md) table via `initial_query_id`.

In contrast to [queryID](#query-id) function, `initialQueryID` returns the same results on different shards (see example).

**Syntax**

```sql
initialQueryID()
```

**Returned value**

- The ID of the initial current query.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

Query:

```sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT initialQueryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

Result:

```text
┌─count()─┐
│ 1       │
└─────────┘
```

## shardNum

Returns the index of a shard which processes a part of data in a distributed query. Indices are started from `1`.
If a query is not distributed then constant value `0` is returned.

**Syntax**

```sql
shardNum()
```

**Returned value**

- Shard index or constant `0`.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Example**

In the following example a configuration with two shards is used. The query is executed on the [system.one](../../operations/system-tables/one.md) table on every shard.

Query:

```sql
CREATE TABLE shard_num_example (dummy UInt8)
    ENGINE=Distributed(test_cluster_two_shards_localhost, system, one, dummy);
SELECT dummy, shardNum(), shardCount() FROM shard_num_example;
```

Result:

```text
┌─dummy─┬─shardNum()─┬─shardCount()─┐
│     0 │          2 │            2 │
│     0 │          1 │            2 │
└───────┴────────────┴──────────────┘
```

**See Also**

- [Distributed Table Engine](../../engines/table-engines/special/distributed.md)

## shardCount

Returns the total number of shards for a distributed query.
If a query is not distributed then constant value `0` is returned.

**Syntax**

```sql
shardCount()
```

**Returned value**

- Total number of shards or `0`.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**See Also**

- [shardNum()](#shard-num) function example also contains `shardCount()` function call.

## getOSKernelVersion

Returns a string with the current OS kernel version.

**Syntax**

```sql
getOSKernelVersion()
```

**Arguments**

- None.

**Returned value**

- The current OS kernel version.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

```sql
SELECT getOSKernelVersion();
```

Result:

```text
┌─getOSKernelVersion()────┐
│ Linux 4.15.0-55-generic │
└─────────────────────────┘
```

## zookeeperSessionUptime

Returns the uptime of the current ZooKeeper session in seconds.

**Syntax**

```sql
zookeeperSessionUptime()
```

**Arguments**

- None.

**Returned value**

- Uptime of the current ZooKeeper session in seconds.

Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SELECT zookeeperSessionUptime();
```

Result:

```text
┌─zookeeperSessionUptime()─┐
│                      286 │
└──────────────────────────┘
```

## generateRandomStructure

Generates random table structure in a format `column1_name column1_type, column2_name column2_type, ...`.

**Syntax**

```sql
generateRandomStructure([number_of_columns, seed])
```

**Arguments**

- `number_of_columns` — The desired number of columns in the result table structure. If set to 0 or `Null`, the number of columns will be random from 1 to 128. Default value: `Null`.
- `seed` - Random seed to produce stable results. If seed is not specified or set to `Null`, it is randomly generated.

All arguments must be constant.

**Returned value**

- Randomly generated table structure.

Type: [String](../../sql-reference/data-types/string.md).

**Examples**

Query:

```sql
SELECT generateRandomStructure()
```

Result:

```text
┌─generateRandomStructure()─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ c1 Decimal32(5), c2 Date, c3 Tuple(LowCardinality(String), Int128, UInt64, UInt16, UInt8, IPv6), c4 Array(UInt128), c5 UInt32, c6 IPv4, c7 Decimal256(64), c8 Decimal128(3), c9 UInt256, c10 UInt64, c11 DateTime │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

```sql
SELECT generateRandomStructure(1)
```

Result:

```text
┌─generateRandomStructure(1)─┐
│ c1 Map(UInt256, UInt16)    │
└────────────────────────────┘
```

Query:

```sql
SELECT generateRandomStructure(NULL, 33)
```

Result:

```text
┌─generateRandomStructure(NULL, 33)─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ c1 DateTime, c2 Enum8('c2V0' = 0, 'c2V1' = 1, 'c2V2' = 2, 'c2V3' = 3), c3 LowCardinality(Nullable(FixedString(30))), c4 Int16, c5 Enum8('c5V0' = 0, 'c5V1' = 1, 'c5V2' = 2, 'c5V3' = 3), c6 Nullable(UInt8), c7 String, c8 Nested(e1 IPv4, e2 UInt8, e3 UInt16, e4 UInt16, e5 Int32, e6 Map(Date, Decimal256(70))) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Note**: the maximum nesting depth of complex types (Array, Tuple, Map, Nested) is limited to 16.

This function can be used together with [generateRandom](../../sql-reference/table-functions/generate.md) to generate completely random tables.

## structureToCapnProtoSchema {#structure_to_capn_proto_schema}

Converts ClickHouse table structure to CapnProto schema.

**Syntax**

```sql
structureToCapnProtoSchema(structure)
```

**Arguments**

- `structure` — Table structure in a format `column1_name column1_type, column2_name column2_type, ...`.
- `root_struct_name` — Name for root struct in CapnProto schema. Default value - `Message`;

**Returned value**

- CapnProto schema

Type: [String](../../sql-reference/data-types/string.md).

**Examples**

Query:

```sql
SELECT structureToCapnProtoSchema('column1 String, column2 UInt32, column3 Array(String)') FORMAT RawBLOB
```

Result:

```text
@0xf96402dd754d0eb7;

struct Message
{
    column1 @0 : Data;
    column2 @1 : UInt32;
    column3 @2 : List(Data);
}
```

Query:

```sql
SELECT structureToCapnProtoSchema('column1 Nullable(String), column2 Tuple(element1 UInt32, element2 Array(String)), column3 Map(String, String)') FORMAT RawBLOB
```

Result:

```text
@0xd1c8320fecad2b7f;

struct Message
{
    struct Column1
    {
        union
        {
            value @0 : Data;
            null @1 : Void;
        }
    }
    column1 @0 : Column1;
    struct Column2
    {
        element1 @0 : UInt32;
        element2 @1 : List(Data);
    }
    column2 @1 : Column2;
    struct Column3
    {
        struct Entry
        {
            key @0 : Data;
            value @1 : Data;
        }
        entries @0 : List(Entry);
    }
    column3 @2 : Column3;
}
```

Query:

```sql
SELECT structureToCapnProtoSchema('column1 String, column2 UInt32', 'Root') FORMAT RawBLOB
```

Result:

```text
@0x96ab2d4ab133c6e1;

struct Root
{
    column1 @0 : Data;
    column2 @1 : UInt32;
}
```

## structureToProtobufSchema {#structure_to_protobuf_schema}

Converts ClickHouse table structure to Protobuf schema.

**Syntax**

```sql
structureToProtobufSchema(structure)
```

**Arguments**

- `structure` — Table structure in a format `column1_name column1_type, column2_name column2_type, ...`.
- `root_message_name` — Name for root message in Protobuf schema. Default value - `Message`;

**Returned value**

- Protobuf schema

Type: [String](../../sql-reference/data-types/string.md).

**Examples**

Query:

```sql
SELECT structureToProtobufSchema('column1 String, column2 UInt32, column3 Array(String)') FORMAT RawBLOB
```

Result:

```text
syntax = "proto3";

message Message
{
    bytes column1 = 1;
    uint32 column2 = 2;
    repeated bytes column3 = 3;
}
```

Query:

```sql
SELECT structureToProtobufSchema('column1 Nullable(String), column2 Tuple(element1 UInt32, element2 Array(String)), column3 Map(String, String)') FORMAT RawBLOB
```

Result:

```text
syntax = "proto3";

message Message
{
    bytes column1 = 1;
    message Column2
    {
        uint32 element1 = 1;
        repeated bytes element2 = 2;
    }
    Column2 column2 = 2;
    map<string, bytes> column3 = 3;
}
```

Query:

```sql
SELECT structureToProtobufSchema('column1 String, column2 UInt32', 'Root') FORMAT RawBLOB
```

Result:

```text
syntax = "proto3";

message Root
{
    bytes column1 = 1;
    uint32 column2 = 2;
}
```

## formatQuery

Returns a formatted, possibly multi-line, version of the given SQL query.

Throws an exception if the query is not well-formed. To return `NULL` instead, function `formatQueryOrNull()` may be used.

**Syntax**

```sql
formatQuery(query)
formatQueryOrNull(query)
```

**Arguments**

- `query` - The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)

**Returned value**

- The formatted query. [String](../../sql-reference/data-types/string.md).

**Example**

```sql
SELECT formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3');
```

Result:

```result
┌─formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3')─┐
│ SELECT
    a,
    b
FROM tab
WHERE (a > 3) AND (b < 3)            │
└───────────────────────────────────────────────────────────────┘
```

## formatQuerySingleLine

Like formatQuery() but the returned formatted string contains no line breaks.

Throws an exception if the query is not well-formed. To return `NULL` instead, function `formatQuerySingleLineOrNull()` may be used.

**Syntax**

```sql
formatQuerySingleLine(query)
formatQuerySingleLineOrNull(query)
```

**Arguments**

- `query` - The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)

**Returned value**

- The formatted query. [String](../../sql-reference/data-types/string.md).

**Example**

```sql
SELECT formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3');
```

Result:

```result
┌─formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3')─┐
│ SELECT a, b FROM tab WHERE (a > 3) AND (b < 3)                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## variantElement

Extracts a column with specified type from a `Variant` column.

**Syntax**

```sql
variantElement(variant, type_name, [, default_value])
```

**Arguments**

- `variant` — Variant column. [Variant](../../sql-reference/data-types/variant.md).
- `type_name` — The name of the variant type to extract. [String](../../sql-reference/data-types/string.md).
- `default_value` - The default value that will be used if variant doesn't have variant with specified type. Can be any type. Optional.

**Returned value**

- Subcolumn of a `Variant` column with specified type.

**Example**

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM test;
```

```text
┌─v─────────────┬─variantElement(v, 'String')─┬─variantElement(v, 'UInt64')─┬─variantElement(v, 'Array(UInt64)')─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ []                                 │
│ 42            │ ᴺᵁᴸᴸ                        │                          42 │ []                                 │
│ Hello, World! │ Hello, World!               │                        ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ [1,2,3]                            │
└───────────────┴─────────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
```

## variantType

Returns the variant type name for each row of `Variant` column. If row contains NULL, it returns `'None'` for it.

**Syntax**

```sql
variantType(variant)
```

**Arguments**

- `variant` — Variant column. [Variant](../../sql-reference/data-types/variant.md).

**Returned value**

- Enum8 column with variant type name for each row.

**Example**

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT variantType(v) FROM test;
```

```text
┌─variantType(v)─┐
│ None           │
│ UInt64         │
│ String         │
│ Array(UInt64)  │
└────────────────┘
```

```sql
SELECT toTypeName(variantType(v)) FROM test LIMIT 1;
```

```text
┌─toTypeName(variantType(v))──────────────────────────────────────────┐
│ Enum8('None' = -1, 'Array(UInt64)' = 0, 'String' = 1, 'UInt64' = 2) │
└─────────────────────────────────────────────────────────────────────┘
```

## minSampleSizeConversion

Calculates minimum required sample size for an A/B test comparing conversions (proportions) in two samples.

**Syntax**

```sql
minSampleSizeConversion(baseline, mde, power, alpha)
```

Uses the formula described in [this article](https://towardsdatascience.com/required-sample-size-for-a-b-testing-6f6608dd330a). Assumes equal sizes of treatment and control groups. Returns the sample size required for one group (i.e. the sample size required for the whole experiment is twice the returned value).

**Arguments**

- `baseline` — Baseline conversion. [Float](../data-types/float.md).
- `mde` — Minimum detectable effect (MDE) as percentage points (e.g. for a baseline conversion 0.25 the MDE 0.03 means an expected change to 0.25 ± 0.03). [Float](../data-types/float.md).
- `power` — Required statistical power of a test (1 - probability of Type II error). [Float](../data-types/float.md).
- `alpha` — Required significance level of a test (probability of Type I error). [Float](../data-types/float.md).

**Returned value**

A named [Tuple](../data-types/tuple.md) with 3 elements:

- `"minimum_sample_size"` — Required sample size. [Float64](../data-types/float.md).
- `"detect_range_lower"` — Lower bound of the range of values not detectable with the returned required sample size (i.e. all values less than or equal to `"detect_range_lower"` are detectable with the provided `alpha` and `power`). Calculated as `baseline - mde`. [Float64](../data-types/float.md).
- `"detect_range_upper"` — Upper bound of the range of values not detectable with the returned required sample size (i.e. all values greater than or equal to `"detect_range_upper"` are detectable with the provided `alpha` and `power`). Calculated as `baseline + mde`. [Float64](../data-types/float.md).

**Example**

The following query calculates the required sample size for an A/B test with baseline conversion of 25%, MDE of 3%, significance level of 5%, and the desired statistical power of 80%:

```sql
SELECT minSampleSizeConversion(0.25, 0.03, 0.80, 0.05) AS sample_size;
```

Result:

```text
┌─sample_size───────────────────┐
│ (3396.077603219163,0.22,0.28) │
└───────────────────────────────┘
```

## minSampleSizeContinuous

Calculates minimum required sample size for an A/B test comparing means of a continuous metric in two samples.

**Syntax**

```sql
minSampleSizeContinous(baseline, sigma, mde, power, alpha)
```

Alias: `minSampleSizeContinous`

Uses the formula described in [this article](https://towardsdatascience.com/required-sample-size-for-a-b-testing-6f6608dd330a). Assumes equal sizes of treatment and control groups. Returns the required sample size for one group (i.e. the sample size required for the whole experiment is twice the returned value). Also assumes equal variance of the test metric in treatment and control groups.

**Arguments**

- `baseline` — Baseline value of a metric. [Integer](../data-types/int-uint.md) or [Float](../data-types/float.md).
- `sigma` — Baseline standard deviation of a metric. [Integer](../data-types/int-uint.md) or [Float](../data-types/float.md).
- `mde` — Minimum detectable effect (MDE) as percentage of the baseline value (e.g. for a baseline value 112.25 the MDE 0.03 means an expected change to 112.25 ± 112.25\*0.03). [Integer](../data-types/int-uint.md) or [Float](../data-types/float.md).
- `power` — Required statistical power of a test (1 - probability of Type II error). [Integer](../data-types/int-uint.md) or [Float](../data-types/float.md).
- `alpha` — Required significance level of a test (probability of Type I error). [Integer](../data-types/int-uint.md) or [Float](../data-types/float.md).

**Returned value**

A named [Tuple](../data-types/tuple.md) with 3 elements:

- `"minimum_sample_size"` — Required sample size. [Float64](../data-types/float.md).
- `"detect_range_lower"` — Lower bound of the range of values not detectable with the returned required sample size (i.e. all values less than or equal to `"detect_range_lower"` are detectable with the provided `alpha` and `power`). Calculated as `baseline * (1 - mde)`. [Float64](../data-types/float.md).
- `"detect_range_upper"` — Upper bound of the range of values not detectable with the returned required sample size (i.e. all values greater than or equal to `"detect_range_upper"` are detectable with the provided `alpha` and `power`). Calculated as `baseline * (1 + mde)`. [Float64](../data-types/float.md).

**Example**

The following query calculates the required sample size for an A/B test on a metric with baseline value of 112.25, standard deviation of 21.1, MDE of 3%, significance level of 5%, and the desired statistical power of 80%:

```sql
SELECT minSampleSizeContinous(112.25, 21.1, 0.03, 0.80, 0.05) AS sample_size;
```

Result:

```text
┌─sample_size───────────────────────────┐
│ (616.2931945826209,108.8825,115.6175) │
└───────────────────────────────────────┘
```

## getClientHTTPHeader

Get the value of an HTTP header.

If there is no such header or the current request is not performed via the HTTP interface, the function returns an empty string.
Certain HTTP headers (e.g., `Authentication` and `X-ClickHouse-*`) are restricted.

The function requires the setting `allow_get_client_http_header` to be enabled.
The setting is not enabled by default for security reasons, because some headers, such as `Cookie`, could contain sensitive info.

HTTP headers are case sensitive for this function.

If the function is used in the context of a distributed query, it returns non-empty result only on the initiator node.
