<a name="data_type-decimal"></a>

# Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S)

有符号的定点数，可在加，减和乘法运算期间保持精度对于除法，丢弃最低有效数字（不舍入）

## 参数

- P - 精度有效范围：[1:38]确定可以包含数字的小数位数（包括小数部分）
- S - 规模有效范围：[0：P]确定数字的小数部分中包含的小数位数

取决于P参数值，Decimal(P, S)也可以表示为：
- P from [ 1 : 9 ] - for Decimal32(S)
- P from [ 10 : 18 ] - for Decimal64(S)
- P from [ 19 : 38 ] - for Decimal128(S)

## Decimal值范围

- Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
- Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
- Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )

例如，Decimal32(4)可以包含-99999.9999至99999.9999的数字，步长为0.0001

## 内部介绍

在内部，数据表示为对应于数字容量的有符号整数可以存储在内存中的实值范围比上面指定的要大一些，仅仅在从字符串转换时检查

由于现代CPU不支持128位数字，因此Decimal128上的操作由软件模拟所以Decimal128的运算速度明显慢于Decimal32/Decimal64

## 运算和结果类型

两个Decimal之间的操作结果扩展为更宽的类型（无论参数的顺序如何）

- Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)
- Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)
- Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)

对于结果的小数部分（比例）的大小，以下规则适用：

- 加法，减法: S = max(S1, S2)
- 乘法: S = S1 + S2
- 除法: S = S1

对于Decimal和整数之间的操作，结果是Decimal，类似于参数

未定义Decimal和Float32/Float64之间的函数要执行此类操作，您可以使用：toDecimal32、toDecimal64、toDecimal128或toFloat32，toFloat64，需要显式地转换其中一个参数请记住，结果将失去精度，类型转换是昂贵的操作 

Decimal上的一些函数返回结果为Float64（例如，var或stddev） 对于其中一些，中间计算发生在Decimal中对于此类函数，尽管结果类型相同，但Float64和Decimal中相同数据的结果可能不同

## 溢出检查

在对Decimal类型执行操作时，可能会发生整数溢出额外的小数部分被丢弃（不是舍入的）额外的整数部分会导致异常

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

检查溢出会导致计算变慢如果您确定结果类型不可能溢出，则可以通过设置`decimal_check_overflow`来禁用溢出检查 在这种情况下，溢出将导致结果不正确：

```
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```
```
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

溢出不仅发生在算术运算上，还发生在比较运算上只有在绝对确定结果正确时才能禁用检查：

```
SELECT toDecimal32(1, 8) < 100
```
```
DB::Exception: Can't compare.
```
