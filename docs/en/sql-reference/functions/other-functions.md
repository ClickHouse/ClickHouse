---
toc_priority: 66
toc_title: Other
---

# Other Functions {#other-functions}

## hostName() {#hostname}

Returns a string with the name of the host that this function was performed on. For distributed processing, this is the name of the remote server host, if the function is performed on a remote server.

## getMacro {#getmacro}

Gets a named value from the [macros](../../operations/server-configuration-parameters/settings.md#macros) section of the server configuration.

**Syntax**

``` sql
getMacro(name);
```

**Parameters**

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

## FQDN {#fqdn}

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

## basename {#basename}

Extracts the trailing part of a string after the last slash or backslash. This function if often used to extract the filename from a path.

``` sql
basename( expr )
```

**Parameters**

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

## visibleWidth(x) {#visiblewidthx}

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

## toTypeName(x) {#totypenamex}

Returns a string containing the type name of the passed argument.

If `NULL` is passed to the function as input, then it returns the `Nullable(Nothing)` type, which corresponds to an internal `NULL` representation in ClickHouse.

## blockSize() {#function-blocksize}

Gets the size of the block.
In ClickHouse, queries are always run on blocks (sets of column parts). This function allows getting the size of the block that you called it for.

## materialize(x) {#materializex}

Turns a constant into a full column containing just one value.
In ClickHouse, full columns and constants are represented differently in memory. Functions work differently for constant arguments and normal arguments (different code is executed), although the result is almost always the same. This function is for debugging this behavior.

## ignore(…) {#ignore}

Accepts any arguments, including `NULL`. Always returns 0.
However, the argument is still evaluated. This can be used for benchmarks.

## sleep(seconds) {#sleepseconds}

Sleeps ‘seconds’ seconds on each data block. You can specify an integer or a floating-point number.

## sleepEachRow(seconds) {#sleepeachrowseconds}

Sleeps ‘seconds’ seconds on each row. You can specify an integer or a floating-point number.

## currentDatabase() {#currentdatabase}

Returns the name of the current database.
You can use this function in table engine parameters in a CREATE TABLE query where you need to specify the database.

## currentUser() {#other-function-currentuser}

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

## isConstant {#is-constant}

Checks whether the argument is a constant expression.

A constant expression means an expression whose resulting value is known at the query analysis (i.e. before execution). For example, expressions over [literals](../../sql-reference/syntax.md#literals) are constant expressions.

The function is intended for development, debugging and demonstration.

**Syntax**

``` sql
isConstant(x)
```

**Parameters**

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

## isFinite(x) {#isfinitex}

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is not infinite and not a NaN, otherwise 0.

## isInfinite(x) {#isinfinitex}

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is infinite, otherwise 0. Note that 0 is returned for a NaN.

## ifNotFinite {#ifnotfinite}

Checks whether floating point value is finite.

**Syntax**

    ifNotFinite(x,y)

**Parameters**

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

## isNaN(x) {#isnanx}

Accepts Float32 and Float64 and returns UInt8 equal to 1 if the argument is a NaN, otherwise 0.

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

Accepts constant strings: database name, table name, and column name. Returns a UInt8 constant expression equal to 1 if there is a column, otherwise 0. If the hostname parameter is set, the test will run on a remote server.
The function throws an exception if the table does not exist.
For elements in a nested data structure, the function checks for the existence of a column. For the nested data structure itself, the function returns 0.

## bar {#function-bar}

Allows building a unicode-art diagram.

`bar(x, min, max, width)` draws a band with a width proportional to `(x - min)` and equal to `width` characters when `x = max`.

Parameters:

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

## transform {#transform}

Transforms a value according to the explicitly defined mapping of some elements to other ones.
There are two variations of this function:

### transform(x, array\_from, array\_to, default) {#transformx-array-from-array-to-default}

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

If the ‘x’ value is equal to one of the elements in the ‘array\_from’ array, it returns the existing element (that is numbered the same) from the ‘array\_to’ array. Otherwise, it returns ‘default’. If there are multiple matching elements in ‘array\_from’, it returns one of the matches.

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

### transform(x, array\_from, array\_to) {#transformx-array-from-array-to}

Differs from the first variation in that the ‘default’ argument is omitted.
If the ‘x’ value is equal to one of the elements in the ‘array\_from’ array, it returns the matching element (that is numbered the same) from the ‘array\_to’ array. Otherwise, it returns ‘x’.

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

## formatReadableSize(x) {#formatreadablesizex}

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

## least(a, b) {#leasta-b}

Returns the smallest value from a and b.

## greatest(a, b) {#greatesta-b}

Returns the largest value of a and b.

## uptime() {#uptime}

Returns the server’s uptime in seconds.

## version() {#version}

Returns the version of the server as a string.

## timezone() {#timezone}

Returns the timezone of the server.

## blockNumber {#blocknumber}

Returns the sequence number of the data block where the row is located.

## rowNumberInBlock {#function-rownumberinblock}

Returns the ordinal number of the row in the data block. Different data blocks are always recalculated.

## rowNumberInAllBlocks() {#rownumberinallblocks}

Returns the ordinal number of the row in the data block. This function only considers the affected data blocks.

## neighbor {#neighbor}

The window function that provides access to a row at a specified offset which comes before or after the current row of a given column.

**Syntax**

``` sql
neighbor(column, offset[, default_value])
```

The result of the function depends on the affected data blocks and the order of data in the block.
If you make a subquery with ORDER BY and call the function from outside the subquery, you can get the expected result.

**Parameters**

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

## runningDifferenceStartingWithFirstValue {#runningdifferencestartingwithfirstvalue}

Same as for [runningDifference](../../sql-reference/functions/other-functions.md#other_functions-runningdifference), the difference is the value of the first row, returned the value of the first row, and each subsequent row returns the difference from the previous row.

## MACNumToString(num) {#macnumtostringnum}

Accepts a UInt64 number. Interprets it as a MAC address in big endian. Returns a string containing the corresponding MAC address in the format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form).

## MACStringToNum(s) {#macstringtonums}

The inverse function of MACNumToString. If the MAC address has an invalid format, it returns 0.

## MACStringToOUI(s) {#macstringtoouis}

Accepts a MAC address in the format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form). Returns the first three octets as a UInt64 number. If the MAC address has an invalid format, it returns 0.

## getSizeOfEnumType {#getsizeofenumtype}

Returns the number of fields in [Enum](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**Parameters:**

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

## blockSerializedSize {#blockserializedsize}

Returns size on disk (without taking into account compression).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**Parameters**

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

## toColumnTypeName {#tocolumntypename}

Returns the name of the class that represents the data type of the column in RAM.

``` sql
toColumnTypeName(value)
```

**Parameters:**

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

## dumpColumnStructure {#dumpcolumnstructure}

Outputs a detailed description of data structures in RAM

``` sql
dumpColumnStructure(value)
```

**Parameters:**

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

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

Outputs the default value for the data type.

Does not include default values for custom columns set by the user.

``` sql
defaultValueOfArgumentType(expression)
```

**Parameters:**

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

## replicate {#other-functions-replicate}

Creates an array with a single value.

Used for internal implementation of [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Parameters:**

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

## filesystemAvailable {#filesystemavailable}

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

## filesystemFree {#filesystemfree}

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

## filesystemCapacity {#filesystemcapacity}

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

## finalizeAggregation {#function-finalizeaggregation}

Takes state of aggregate function. Returns result of aggregation (finalized state).

## runningAccumulate {#runningaccumulate}

Accumulates states of an aggregate function for each row of a data block.

!!! warning "Warning"
    The state is reset for each new data block.

**Syntax**

``` sql
runningAccumulate(agg_state[, grouping]);
```

**Parameters**

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

## joinGet {#joinget}

The function lets you extract data from the table the same way as from a [dictionary](../../sql-reference/dictionaries/index.md).

Gets data from [Join](../../engines/table-engines/special/join.md#creating-a-table) tables using the specified join key.

Only supports tables created with the `ENGINE = Join(ANY, LEFT, <join_keys>)` statement.

**Syntax**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Parameters**

-   `join_storage_table_name` — an [identifier](../../sql-reference/syntax.md#syntax-identifiers) indicates where search is performed. The identifier is searched in the default database (see parameter `default_database` in the config file). To override the default database, use the `USE db_name` or specify the database and the table through the separator `db_name.db_table`, see the example.
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**Returned value**

Returns list of values corresponded to list of keys.

If certain doesn’t exist in source table then `0` or `null` will be returned based on [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) setting.

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

## modelEvaluate(model\_name, …) {#function-modelevaluate}

Evaluate external model.
Accepts a model name and model arguments. Returns Float64.

## throwIf(x\[, custom\_message\]) {#throwifx-custom-message}

Throw an exception if the argument is non zero.
custom\_message - is an optional parameter: a constant string, provides an error message

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identity {#identity}

Returns the same value that was used as its argument. Used for debugging and testing, allows to cancel using index, and get the query performance of a full scan. When query is analyzed for possible use of index, the analyzer doesn’t look inside `identity` functions.

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

## randomPrintableASCII {#randomascii}

Generates a string with a random set of [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) printable characters.

**Syntax**

``` sql
randomPrintableASCII(length)
```

**Parameters**

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

## randomString {#randomstring}

Generates a binary string of the specified length filled with random bytes (including zero bytes).

**Syntax**

``` sql
randomString(length)
```

**Parameters**

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


## randomFixedString {#randomfixedstring}

Generates a binary string of the specified length filled with random bytes (including zero bytes).

**Syntax**

``` sql
randomFixedString(length);
```

**Parameters**

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


## randomStringUTF8 {#randomstringutf8}

Generates a random string of a specified length. Result string contains valid UTF-8 code points. The value of code points may be outside of the range of assigned Unicode.

**Syntax**

``` sql
randomStringUTF8(length);
```

**Parameters**

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

[Original article](https://clickhouse.tech/docs/en/query_language/functions/other_functions/) <!--hide-->
