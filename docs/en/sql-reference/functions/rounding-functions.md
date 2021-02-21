---
toc_priority: 45
toc_title: Rounding
---

# Rounding Functions {#rounding-functions}

## floor(x\[, N\]) {#floorx-n}

Returns the largest round number that is less than or equal to `x`. A round number is a multiple of 1/10N, or the nearest number of the appropriate data type if 1 / 10N isn’t exact.
‘N’ is an integer constant, optional parameter. By default it is zero, which means to round to an integer.
‘N’ may be negative.

Examples: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` is any numeric type. The result is a number of the same type.
For integer arguments, it makes sense to round with a negative `N` value (for non-negative `N`, the function doesn’t do anything).
If rounding causes overflow (for example, floor(-128, -1)), an implementation-specific result is returned.

## ceil(x\[, N\]), ceiling(x\[, N\]) {#ceilx-n-ceilingx-n}

Returns the smallest round number that is greater than or equal to `x`. In every other way, it is the same as the `floor` function (see above).

## trunc(x\[, N\]), truncate(x\[, N\]) {#truncx-n-truncatex-n}

Returns the round number with largest absolute value that has an absolute value less than or equal to `x`‘s. In every other way, it is the same as the ’floor’ function (see above).

## round(x\[, N\]) {#rounding_functions-round}

Rounds a value to a specified number of decimal places.

The function returns the nearest number of the specified order. In case when given number has equal distance to surrounding numbers, the function uses banker’s rounding for float number types and rounds away from zero for the other number types.

``` sql
round(expression [, decimal_places])
```

**Parameters:**

-   `expression` — A number to be rounded. Can be any [expression](../../sql-reference/syntax.md#syntax-expressions) returning the numeric [data type](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   If `decimal-places > 0` then the function rounds the value to the right of the decimal point.
    -   If `decimal-places < 0` then the function rounds the value to the left of the decimal point.
    -   If `decimal-places = 0` then the function rounds the value to integer. In this case the argument can be omitted.

**Returned value:**

The rounded number of the same type as the input number.

### Examples {#examples}

**Example of use**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

``` text
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**Examples of rounding**

Rounding to the nearest number.

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

Banker’s rounding.

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**See Also**

-   [roundBankers](#roundbankers)

## roundBankers {#roundbankers}

Rounds a number to a specified decimal position.

-   If the rounding number is halfway between two numbers, the function uses banker’s rounding.

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   In other cases, the function rounds numbers to the nearest integer.

Using banker’s rounding, you can reduce the effect that rounding numbers has on the results of summing or subtracting these numbers.

For example, sum numbers 1.5, 2.5, 3.5, 4.5 with different rounding:

-   No rounding: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   Banker’s rounding: 2 + 2 + 4 + 4 = 12.
-   Rounding to the nearest integer: 2 + 3 + 4 + 5 = 14.

**Syntax**

``` sql
roundBankers(expression [, decimal_places])
```

**Parameters**

-   `expression` — A number to be rounded. Can be any [expression](../../sql-reference/syntax.md#syntax-expressions) returning the numeric [data type](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — Decimal places. An integer number.
    -   `decimal-places > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    -   `decimal-places < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    -   `decimal-places = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

**Returned value**

A value rounded by the banker’s rounding method.

### Examples {#examples-1}

**Example of use**

Query:

``` sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

Result:

``` text
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

**Examples of Banker’s rounding**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**See Also**

-   [round](#rounding_functions-round)

## roundToExp2(num) {#roundtoexp2num}

Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to the nearest (whole non-negative) degree of two.

## roundDuration(num) {#rounddurationnum}

Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to numbers from the set: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. This function is specific to Yandex.Metrica and used for implementing the report on session length.

## roundAge(num) {#roundagenum}

Accepts a number. If the number is less than 18, it returns 0. Otherwise, it rounds the number down to a number from the set: 18, 25, 35, 45, 55. This function is specific to Yandex.Metrica and used for implementing the report on user age.

## roundDown(num, arr) {#rounddownnum-arr}

Accepts a number and rounds it down to an element in the specified array. If the value is less than the lowest bound, the lowest bound is returned.

[Original article](https://clickhouse.tech/docs/en/query_language/functions/rounding_functions/) <!--hide-->
