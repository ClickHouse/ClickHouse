---
description: 'ClickHouse 中 Decimal 数据类型的文档，提供可配置精度的定点运算'
sidebar_label: 'Decimal'
sidebar_position: 6
slug: /sql-reference/data-types/decimal
title: 'Decimal, Decimal(P), Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S),
  Decimal256(S)'
doc_type: 'reference'
---

有符号定点数，在加法、减法和乘法运算中可保持精度。对于除法，小数末位会被截断 (不进行四舍五入) 。

<div id="parameters">
  ## 参数
</div>

* P - 精度。有效范围：[ 1 : 76 ]。决定数值可以有多少位十进制数字 (包括小数部分) 。默认精度为 10。
* S - 标度。有效范围：[ 0 : P ]。决定小数部分可以有多少位十进制数字。

Decimal(P) 等同于 Decimal(P, 0)。同样，Decimal 这一写法等同于 Decimal(10, 0)。

根据参数 P 的值，Decimal(P, S) 是以下类型的同义写法：

* P 在 [ 1 : 9 ] 范围内 - 对应 Decimal32(S)
* P 在 [ 10 : 18 ] 范围内 - 对应 Decimal64(S)
* P 在 [ 19 : 38 ] 范围内 - 对应 Decimal128(S)
* P 在 [ 39 : 76 ] 范围内 - 对应 Decimal256(S)

<div id="decimal-value-ranges">
  ## Decimal 值范围
</div>

* Decimal(P, S) - ( -1 * 10^(P - S), 1 * 10^(P - S) )
* Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
* Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
* Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )
* Decimal256(S) - ( -1 * 10^(76 - S), 1 * 10^(76 - S) )

例如，Decimal32(4) 可表示从 -99999.9999 到 99999.9999 的数值，步长为 0.0001。

<div id="internal-representation">
  ## 内部表示
</div>

在内部，数据以具有相应比特宽度的普通有符号整数表示。可存储在内存中的实际取值范围比上文指定的略大一些，只有在从字符串进行转换时才会检查这些范围。

由于现代 CPU 不原生支持 128 位和 256 位整数，因此 Decimal128 和 Decimal256 上的操作是通过模拟实现的。所以，Decimal128 和 Decimal256 的运行速度会明显慢于 Decimal32/Decimal64。

<div id="operations-and-result-type">
  ## 操作和结果类型
</div>

对 Decimal 进行二元运算时，结果类型会提升为更宽的类型 (与参数顺序无关) 。

* `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
* `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
* `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`
* `Decimal256(S1) <op> Decimal<32|64|128>(S2) -> Decimal256(S)`

标度规则：

* 加法、减法：S = max(S1, S2)。
* 乘法：S = S1 + S2。
* 除法：S = S1。

对于 Decimal 与整数之间的类似运算，结果为与参数位宽相同的 Decimal。

Decimal 与 Float32/Float64 之间的运算未定义。如果需要，可以使用 toDecimal32、toDecimal64、toDecimal128 或 toFloat32、toFloat64 内置函数，显式转换其中一个参数。请注意，结果会丢失精度，而且类型转换本身也是一项计算开销较高的操作。

某些作用于 Decimal 的函数会返回 Float64 类型的结果 (例如 var 或 stddev) 。中间计算仍可能以 Decimal 进行，这可能导致数值相同的 Float64 输入和 Decimal 输入得到不同的结果。

<div id="overflow-checks">
  ## 溢出检查
</div>

在 Decimal 计算过程中，可能会发生整数溢出。小数部分中超出的位数会被直接截去 (不进行四舍五入) ；整数部分中超出的位数则会导致异常。

:::warning
Decimal128 和 Decimal256 尚未实现溢出检查。发生溢出时会返回错误结果，不会抛出异常。
:::

```sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

```text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

```sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

```text
DB::Exception: Scale is out of bounds.
```

```sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
DB::Exception: Decimal math overflow.
```

溢出检查会拖慢操作速度。如果确定不会发生溢出，建议通过 `decimal_check_overflow` 设置禁用检查。禁用检查后，一旦发生溢出，结果将会不正确：

```sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

溢出检查不仅会出现在算术运算中，也会出现在值比较中：

```sql
SELECT toDecimal32(1, 8) < 100
```

```text
DB::Exception: Can't compare.
```

**另请参阅**

* [isDecimalOverflow](/zh/sql-reference/functions/other-functions#isDecimalOverflow)
* [countDigits](/zh/sql-reference/functions/other-functions#countDigits)