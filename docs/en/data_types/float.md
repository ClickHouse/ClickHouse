# Float32, Float64

[Floating point numbers](https://en.wikipedia.org/wiki/IEEE_754).

Types are equivalent to types of C:

- `Float32` - `float`
- `Float64`  - `double`

We recommend that you store data in integer form whenever possible. For example, convert fixed precision numbers to integer values, such as monetary amounts or page load times in milliseconds.

## Using Floating-point Numbers

- Computations with floating-point numbers might produce a rounding error.

```sql
SELECT 1 - 0.9
```

```
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

- The result of the calculation depends on the calculation method (the processor type and architecture of the computer system).
- Floating-point calculations might result in numbers such as infinity (`Inf`) and "not-a-number" (`NaN`). This should be taken into account when processing the results of calculations.
- When reading floating point numbers from rows, the result might not be the nearest machine-representable number.

## NaN and Inf

In contrast to standard SQL, ClickHouse supports the following categories of floating-point numbers:

- `Inf` – Infinity.

```sql
SELECT 0.5 / 0
```

```
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

- `-Inf` – Negative infinity.

```sql
SELECT -0.5 / 0
```

```
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

- `NaN` – Not a number.

```
SELECT 0 / 0
```

```
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

  See the rules for `NaN` sorting in the section [ORDER BY clause](../query_language/select.md#query_language-queries-order_by).

