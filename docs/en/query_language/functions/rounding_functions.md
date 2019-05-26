# Rounding functions

## floor(x\[, N\])

Returns the largest round number that is less than or equal to x. A round number is a multiple of 1/10N, or the nearest number of the appropriate data type if 1 / 10N isn't exact.
'N' is an integer constant, optional parameter. By default it is zero, which means to round to an integer.
'N' may be negative.

Examples: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` is any numeric type. The result is a number of the same type.
For integer arguments, it makes sense to round with a negative 'N' value (for non-negative 'N', the function doesn't do anything).
If rounding causes overflow (for example, floor(-128, -1)), an implementation-specific result is returned.

## ceil(x\[, N\]), ceiling(x\[, N\])

Returns the smallest round number that is greater than or equal to 'x'. In every other way, it is the same as the 'floor' function (see above).

## round(x[, N]) {#rounding_functions-round}

Rounds a value to a specified number of decimal places.

The function returns the nearest number of the specified order. In case when given number has equal distance to surrounding numbers the function returns the number having the nearest even digit (banker's rounding).

```
round(expression [, decimal_places])
```

**Parameters:**

- `expression` — A number to be rounded. Can be any [expression](../syntax.md#syntax-expressions) returning the numeric [data type](../../data_types/index.md#data_types).
- `decimal-places` — An integer value.
    - If `decimal-places > 0` then the function rounds the value to the right of the decimal point.
    - If `decimal-places < 0` then the function rounds the value to the left of the decimal point.
    - If `decimal-places = 0` then the function rounds the value to integer. In this case the argument can be omitted.

**Returned value:**

The rounded number of the same type as the input number.

### Examples

**Example of use**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```
```
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**Examples of rounding**

Rounding to the nearest number.

```
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

Banker's rounding.

```
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

## roundToExp2(num)

Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to the nearest (whole non-negative) degree of two.

## roundDuration(num)

Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to numbers from the set: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. This function is specific to Yandex.Metrica and used for implementing the report on session length

## roundAge(num)

Accepts a number. If the number is less than 18, it returns 0. Otherwise, it rounds the number down to a number from the set: 18, 25, 35, 45, 55. This function is specific to Yandex.Metrica and used for implementing the report on user age.

## roundDown(num, arr)

Accept a number, round it down to an element in the specified array. If the value is less than the lowest bound, the lowest bound is returned.

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/rounding_functions/) <!--hide-->
