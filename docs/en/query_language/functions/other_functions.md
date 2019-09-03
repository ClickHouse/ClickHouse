# Other functions

## hostName()

Returns a string with the name of the host that this function was performed on. For distributed processing, this is the name of the remote server host, if the function is performed on a remote server.

## basename

Extracts the trailing part of a string after the last slash or backslash. This function if often used to extract the filename from a path.

```
basename( expr )
```

**Parameters**

- `expr` — Expression resulting in a [String](../../data_types/string.md) type value. All the backslashes must be escaped in the resulting value.

**Returned Value**

A string that contains:

- The trailing part of a string after the last slash or backslash.

    If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

- The original string if there are no slashes or backslashes.

**Example**

```sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```
```text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```
```sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```
```text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```
```sql
SELECT 'some-file-name' AS a, basename(a)
```
```text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x)

Calculates the approximate width when outputting values to the console in text format (tab-separated).
This function is used by the system for implementing Pretty formats.

`NULL` is represented as a string corresponding to `NULL` in `Pretty` formats.

```
SELECT visibleWidth(NULL)

┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName(x)

Returns a string containing the type name of the passed argument.

If `NULL` is passed to the function as input, then it returns the `Nullable(Nothing)` type, which corresponds to an internal `NULL` representation in ClickHouse.

## blockSize() {#function-blocksize}

Gets the size of the block.
In ClickHouse, queries are always run on blocks (sets of column parts). This function allows getting the size of the block that you called it for.

## materialize(x)

Turns a constant into a full column containing just one value.
In ClickHouse, full columns and constants are represented differently in memory. Functions work differently for constant arguments and normal arguments (different code is executed), although the result is almost always the same. This function is for debugging this behavior.

## ignore(...)

Accepts any arguments, including `NULL`. Always returns 0.
However, the argument is still evaluated. This can be used for benchmarks.

## sleep(seconds)

Sleeps 'seconds' seconds on each data block. You can specify an integer or a floating-point number.

## sleepEachRow(seconds)

Sleeps 'seconds' seconds on each row. You can specify an integer or a floating-point number.

## currentDatabase()

Returns the name of the current database.
You can use this function in table engine parameters in a CREATE TABLE query where you need to specify the database.

## currentUser()
Returns the login of authorized user (initiator of query execution).

## isFinite(x)

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is not infinite and not a NaN, otherwise 0.

## isInfinite(x)

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is infinite, otherwise 0. Note that 0 is returned for a NaN.

## isNaN(x)

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is a NaN, otherwise 0.

## hasColumnInTable(\['hostname'\[, 'username'\[, 'password'\]\],\] 'database', 'table', 'column')

Accepts constant strings: database name, table name, and column name. Returns a UInt8 constant expression equal to 1 if there is a column, otherwise 0. If the hostname parameter is set, the test will run on a remote server.
The function throws an exception if the table does not exist.
For elements in a nested data structure, the function checks for the existence of a column. For the nested data structure itself, the function returns 0.

## bar {#function-bar}

Allows building a unicode-art diagram.

`bar(x, min, max, width)` draws a band with a width proportional to `(x - min)` and equal to `width` characters when `x = max`.

Parameters:

- `x` — Size to display.
- `min, max` — Integer constants. The value must fit in `Int64`.
- `width` — Constant, positive integer, can be fractional.

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

```
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

1. `transform(x, array_from, array_to, default)`

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in 'from' to.

`default` – Which value to use if 'x' is not equal to any of the values in 'from'.

`array_from` and `array_to` – Arrays of the same size.

Types:

`transform(T, Array(T), Array(U), U) -> U`

`T` and `U` can be numeric, string, or Date or DateTime types.
Where the same letter is indicated (T or U), for numeric types these might not be matching types, but types that have a common type.
For example, the first argument can have the Int64 type, while the second has the Array(UInt16) type.

If the 'x' value is equal to one of the elements in the 'array_from' array, it returns the existing element (that is numbered the same) from the 'array_to' array. Otherwise, it returns 'default'. If there are multiple matching elements in 'array_from', it returns one of the matches.

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

```
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

2. `transform(x, array_from, array_to)`

Differs from the first variation in that the 'default' argument is omitted.
If the 'x' value is equal to one of the elements in the 'array_from' array, it returns the matching element (that is numbered the same) from the 'array_to' array. Otherwise, it returns 'x'.

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

```
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

```
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## least(a, b)

Returns the smallest value from a and b.

## greatest(a, b)

Returns the largest value of a and b.

## uptime()

Returns the server's uptime in seconds.

## version()

Returns the version of the server as a string.

## timezone()

Returns the timezone of the server.

## blockNumber

Returns the sequence number of the data block where the row is located.

## rowNumberInBlock {#function-rownumberinblock}

Returns the ordinal number of the row in the data block. Different data blocks are always recalculated.

## rowNumberInAllBlocks()

Returns the ordinal number of the row in the data block. This function only considers the affected data blocks.

## neighbor(column, offset\[, default_value\])

Returns value for `column`, in `offset` distance from current row.
This function is a partial implementation of [window functions](https://en.wikipedia.org/wiki/SQL_window_function) LEAD() and LAG().

The result of the function depends on the affected data blocks and the order of data in the block.
If you make a subquery with ORDER BY and call the function from outside the subquery, you can get the expected result.

If `offset` value is outside block bounds, a default value for `column` returned. If `default_value` is given, then it will be used.
This function can be used to compute year-over-year metric value:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

```
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


## runningDifference(x) {#other_functions-runningdifference}

Calculates the difference between successive row values ​​in the data block.
Returns 0 for the first row and the difference from the previous row for each subsequent row.

The result of the function depends on the affected data blocks and the order of data in the block.
If you make a subquery with ORDER BY and call the function from outside the subquery, you can get the expected result.

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

```
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
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘

set max_block_size=100000 -- default value is 65536! 

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstValue

Same as for [runningDifference](./other_functions.md#other_functions-runningdifference), the difference is the value of the first row, returned the value of the first row, and each subsequent row returns the difference from the previous row.

## MACNumToString(num)

Accepts a UInt64 number. Interprets it as a MAC address in big endian. Returns a string containing the corresponding MAC address in the format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form).

## MACStringToNum(s)

The inverse function of MACNumToString. If the MAC address has an invalid format, it returns 0.

## MACStringToOUI(s)

Accepts a MAC address in the format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form). Returns the first three octets as a UInt64 number. If the MAC address has an invalid format, it returns 0.

## getSizeOfEnumType

Returns the number of fields in [Enum](../../data_types/enum.md).

```
getSizeOfEnumType(value)
```

**Parameters:**

- `value` — Value of type `Enum`.

**Returned values**

- The number of fields with `Enum` input values.
- An exception is thrown if the type is not `Enum`.

**Example**

```
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x

┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName

Returns the name of the class that represents the data type of the column in RAM.

```
toColumnTypeName(value)
```

**Parameters:**

- `value` — Any type of value.

**Returned values**

- A string with the name of the class that is used for representing the `value` data type in RAM.

**Example of the difference between` toTypeName ' and ' toColumnTypeName`**

```
:) select toTypeName(cast('2018-01-01 01:02:03' AS DateTime))

SELECT toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))

┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘

1 rows in set. Elapsed: 0.008 sec.

:) select toColumnTypeName(cast('2018-01-01 01:02:03' AS DateTime))

SELECT toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))

┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

The example shows that the `DateTime` data type is stored in memory as `Const(UInt32)`.

## dumpColumnStructure

Outputs a detailed description of data structures in RAM

```
dumpColumnStructure(value)
```

**Parameters:**

- `value` — Any type of value.

**Returned values**

- A string describing the structure that is used for representing the `value` data type in RAM.

**Example**

```
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))

┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType

Outputs the default value for the data type.

Does not include default values for custom columns set by the user.

```
defaultValueOfArgumentType(expression)
```

**Parameters:**

- `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**Returned values**

- `0` for numbers.
- Empty string for strings.
- `ᴺᵁᴸᴸ` for [Nullable](../../data_types/nullable.md).

**Example**

```
:) SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )

SELECT defaultValueOfArgumentType(CAST(1, 'Int8'))

┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.

:) SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )

SELECT defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))

┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

## indexHint

Outputs data in the range selected by the index without filtering by the expression specified as an argument.

The expression passed to the function is not calculated, but ClickHouse applies the index to this expression in the same way as if the expression was in the query without `indexHint`.

**Returned value**

- 1.

**Example**

Here is a table with the test data for [ontime](../../getting_started/example_datasets/ontime.md).

```
SELECT count() FROM ontime

┌─count()─┐
│ 4276457 │
└─────────┘
```

The table has indexes for the fields `(FlightDate, (Year, FlightDate))`.

Create a selection by date like this:

```
:) SELECT FlightDate AS k, count() FROM ontime GROUP BY k ORDER BY k

SELECT
    FlightDate AS k,
    count()
FROM ontime
GROUP BY k
ORDER BY k ASC

┌──────────k─┬─count()─┐
│ 2017-01-01 │   13970 │
│ 2017-01-02 │   15882 │
........................
│ 2017-09-28 │   16411 │
│ 2017-09-29 │   16384 │
│ 2017-09-30 │   12520 │
└────────────┴─────────┘

273 rows in set. Elapsed: 0.072 sec. Processed 4.28 million rows, 8.55 MB (59.00 million rows/s., 118.01 MB/s.)
```

In this selection, the index is not used and ClickHouse processed the entire table (`Processed 4.28 million rows`). To apply the index, select a specific date and run the following query:

```
:) SELECT FlightDate AS k, count() FROM ontime WHERE k = '2017-09-15' GROUP BY k ORDER BY k

SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE k = '2017-09-15'
GROUP BY k
ORDER BY k ASC

┌──────────k─┬─count()─┐
│ 2017-09-15 │   16428 │
└────────────┴─────────┘

1 rows in set. Elapsed: 0.014 sec. Processed 32.74 thousand rows, 65.49 KB (2.31 million rows/s., 4.63 MB/s.)
```

The last line of output shows that by using the index, ClickHouse processed a significantly smaller number of rows (`Processed 32.74 thousand rows`).

Now pass the expression `k = '2017-09-15'` to the `indexHint` function:

```
:) SELECT FlightDate AS k, count() FROM ontime WHERE indexHint(k = '2017-09-15') GROUP BY k ORDER BY k

SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE indexHint(k = '2017-09-15')
GROUP BY k
ORDER BY k ASC

┌──────────k─┬─count()─┐
│ 2017-09-14 │    7071 │
│ 2017-09-15 │   16428 │
│ 2017-09-16 │    1077 │
│ 2017-09-30 │    8167 │
└────────────┴─────────┘

4 rows in set. Elapsed: 0.004 sec. Processed 32.74 thousand rows, 65.49 KB (8.97 million rows/s., 17.94 MB/s.)
```

The response to the request shows that ClickHouse applied the index in the same way as the previous time (`Processed 32.74 thousand rows`). However, the resulting set of rows shows that the expression `k = '2017-09-15'` was not used when generating the result.

Because the index is sparse in ClickHouse, "extra" data ends up in the response when reading a range (in this case, the adjacent dates). Use the `indexHint` function to see it.

## replicate

Creates an array with a single value.

Used for internal implementation of [arrayJoin](array_join.md#functions_arrayjoin).

```
replicate(x, arr)
```

**Parameters:**

- `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
- `x` — The value that the resulting array will be filled with.

**Output value**

- An array filled with the value `x`.

**Example**

```
SELECT replicate(1, ['a', 'b', 'c'])

┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable {#function-filesystemavailable}

Returns the amount of remaining space in the filesystem where the files of the databases located. See the [path](../../operations/server_settings/settings.md#server_settings-path) server setting description.

```
filesystemAvailable()
```

**Returned values**

- Amount of remaining space in bytes.

Type: [UInt64](../../data_types/int_uint.md).

**Example**

```sql
SELECT filesystemAvailable() AS "Free space", toTypeName(filesystemAvailable()) AS "Type"
```
```text
┌──Free space─┬─Type───┐
│ 18152624128 │ UInt64 │
└─────────────┴────────┘
```

## filesystemCapacity

Returns the capacity information of the disk, in bytes. This information is evaluated using the configured by path.

## finalizeAggregation {#function-finalizeaggregation}

Takes state of aggregate function. Returns result of aggregation (finalized state).

## runningAccumulate {#function-runningaccumulate}

Takes the states of the aggregate function and returns a column with values, are the result of the accumulation of these states for a set of block lines, from the first to the current line.
For example, takes state of aggregate function (example runningAccumulate(uniqState(UserID))), and for each row of block, return result of aggregate function on merge of states of all previous rows and current row.
So, result of function depends on partition of data to blocks and on order of data in block.

## joinGet('join_storage_table_name', 'get_column', join_key) {#other_functions-joinget}

Gets data from [Join](../../operations/table_engines/join.md) tables using the specified join key.

Only supports tables created with the `ENGINE = Join(ANY, LEFT, <join_keys>)` statement.

## modelEvaluate(model_name, ...)
Evaluate model.
Accepts a model name and model arguments. Returns Float64.

## throwIf(x\[, custom_message\])

Throw an exception if the argument is non zero.
custom_message - is an optional parameter: a constant string, provides an error message

```sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);

↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identity()

Returns the same value that was used as its argument. 

```sql
SELECT identity(42)

┌─identity(42)─┐
│           42 │
└──────────────┘
```
Used for debugging and testing, allows to "break" access by index, and get the result and query performance for a full scan.

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/other_functions/) <!--hide-->
