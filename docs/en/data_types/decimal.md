<a name="data_type-decimal"></a>

# Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S)

Signed fixed point numbers that keep precision during add, subtract and multiply operations. For division least significant digits are discarded (not rounded).

## Параметры

- P - precision. Valid range: [ 1 : 38 ]. Determines how many decimal digits number can have (including fraction).
- S - scale. Valid range: [ 0 : P ]. Determines how many decimal digits fraction can have.

Depending on P parameter value Decimal(P, S) is a synonym for:
- P from [ 1 : 9 ] - for Decimal32(S)
- P from [ 10 : 18 ] - for Decimal64(S)
- P from [ 19 : 38 ] - for Decimal128(S)

## Decimal value ranges

- Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
- Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
- Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )

For example, Decimal32(4) can contain numbers from -99999.9999 to 99999.9999 with 0.0001 step.

## Internal representation

Internally data is represented as normal signed integers with respective bit width. Real value ranges that can be stored in memory are a bit larger than specified above, which are checked only on convertion from string.

Because modern CPU's do not support 128 bit integers natively, operations on Decimal128 are emulated. Because of this Decimal128 works signigicantly slower than Decimal32/Decimal64.

## Operations and result type

Binary operations on Decimal result in wider result type (with any order of arguments).

- Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)
- Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)
- Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)

Rules for scale:

- add, subtract: S = max(S1, S2).
- multuply: S = S1 + S2.
- divide: S = S1.

For similar operations between Decimal and integers, the result is Decimal of the same size as argument.

Operations between Decimal and Float32/Float64 are not defined. If you really need them, you can explicitly cast one of argument using toDecimal32, toDecimal64, toDecimal128 or toFloat32, toFloat64 builtins. Keep in mind that the result will lose precision and type conversion is computationally expensive operation. 

Some functions on Decimal return result as Float64 (for example, var or stddev). Intermediate calculations might still be performed in Decimal, which might lead to different results between Float64 and Decimal inputs with same values.

## Overflow checks

During calculations on Decimal, integer overflows might happen. Excessive digits in fraction are discarded (not rounded). Excessive digits in integer part will lead to exception.

```
SELECT toDecimal32(2, 4) AS x, x / 3
```
```
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

```
SELECT toDecimal32(4.2, 8) AS x, x * x
```
```
DB::Exception: Scale is out of bounds.
```

```
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```
```
DB::Exception: Decimal math overflow.
```

Overflow checks lead to operations slowdown. If it is known that overflows are not possible, it makes sense to disable checks using `decimal_check_overflow` setting. When checks are disabled and overflow happens, the result will be incorrect:

```
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```
```
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

Overflow checks happen not only on arithmetic operations, but also on value comparison:

```
SELECT toDecimal32(1, 8) < 100
```
```
DB::Exception: Can't compare.
```

[Original article](https://clickhouse.yandex/docs/en/data_types/decimal/) <!--hide-->
