<a name="data_type-decimal"></a>

# Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S)

有符号的定点数，在进行加减乘操作的过程中都会保留精度。对于除法，最低有效数字被丢弃（不舍入）。

## 参数

- P - 精度。范围值: [ 1 : 38 ]，决定可以有多少个十进制数字（包括分数）。
- S - 范围。范围值: [ 0 : P ]，决定小数的位数。

对于不同的 P 参数值 Decimal 表示，以下例子都是同义的：
- [ 1 : 9 ] 的 P - for Decimal32(S)
- [ 10 : 18 ] 的 P - for Decimal64(S)
- [ 19 : 38 ] 的 P - for Decimal128(S)

## 十进制值范围

- Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
- Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
- Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )

例如, Decimal32(4) 可以表示 -99999.9999 到 99999.9999 范围内步数为 0.0001 的值。

## 内部表示方式

数据采用与自身位宽相同的有符号整数存储。这个数在内存中实际范围会高于上述范围，从 String 转换到十进制数的时候会做对应的检查。

Decimal32/Decimal64 通常处理速度要高于Decimal128，这是因为当前通用CPU不支持128位的操作导致的。

## 运算以及结果的类型

对Decimal的二进制运算导致更宽的结果类型（具有任何参数顺序）。

- Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)
- Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)
- Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)

精度变化的规则:

- 加，减： S = max(S1, S2).
- 相乘： S = S1 + S2.
- 相除：S = S1.

对于 Decimal 和整数之间的类似操作，结果为一样参数值的 Decimal。

没有定义 Decimal 和 Float32/Float64 的操作。如果你真的需要他们，你可以某一个参数明确地转换为 toDecimal32，toDecimal64， toDecimal128 或 toFloat32， toFloat64。注意这个操作会丢失精度，并且类型转换是一个代价昂贵的操作。

有一些函数对 Decimal 进行操作后是返回 Float64 的（例如，var 或 stddev）。计算的结果可能仍在 Decimal 中执行，这可能导致 Float64 和具有相同值的 Decimal 输入计算后的结果不同。


## 溢出检查

在对 Decimal 计算的过程中，数值会有可能溢出。分数中的过多数字被丢弃（不是舍入的）。 整数中的过多数字将导致异常。

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

溢出检查会导致操作减慢。 如果已知溢出不可能，则使用`decimal_check_overflow`设置禁用检查是有意义的。 禁用检查并发生溢出时，结果将不正确：

```
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```
```
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

溢出检查不仅会在数学运算中进行，还会在值比较中进行：

```
SELECT toDecimal32(1, 8) < 100
```
```
DB::Exception: Can't compare.
```

[来源文章](https://clickhouse.yandex/docs/en/data_types/decimal/) <!--hide-->
