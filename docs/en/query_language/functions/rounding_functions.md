# Rounding functions

## floor(x\[, N\])

Returns the largest round number that is less than or equal to x. A round number is a multiple of 1/10N, or the nearest number of the appropriate data type if 1 / 10N isn't exact.
'N' is an integer constant, optional parameter. By default it is zero, which means to round to an integer.
'N' may be negative.

Examples: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` is any numeric type. The result is a number of the same type.
For integer arguments, it makes sense to round with a negative 'N' value (for non-negative 'N', the function doesn't do anything).
If rounding causes overflow (for example, floor(-128, -1)), an implementation-specific result is returned.

## ceil(x\[, N\])

Returns the smallest round number that is greater than or equal to 'x'. In every other way, it is the same as the 'floor' function (see above).

## round(x\[, N\])

Implements [banker's rounding](https://en.wikipedia.org/wiki/Rounding#Round_half_to_even), i.e., rounding to the nearest even integer.

**Function arguments:**

- `x` — the number to be rounded. [Type](../../data_types/index.md#data_types) —  any number.
- `N`—  the position of the number after the decimal point to round the number to.

**Returned value:**

The rounded number of the same type as the input number `x`

**Example:**

```sql
SELECT
    number / 2 AS x,
    round(x)
FROM system.numbers
LIMIT 10
```

```
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
│ 1.5 │                        2 │
│   2 │                        2 │
│ 2.5 │                        2 │
│   3 │                        3 │
│ 3.5 │                        4 │
│   4 │                        4 │
│ 4.5 │                        4 │
└─────┴──────────────────────────┘
```

## roundToExp2(num)

Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to the nearest (whole non-negative) degree of two.

## roundDuration(num)

Accepts a number. If the number is less than one, it returns 0. Otherwise, it rounds the number down to numbers from the set: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. This function is specific to Yandex.Metrica and used for implementing the report on session length

## roundAge(num)

Accepts a number. If the number is less than 18, it returns 0. Otherwise, it rounds the number down to a number from the set: 18, 25, 35, 45, 55. This function is specific to Yandex.Metrica and used for implementing the report on user age.

