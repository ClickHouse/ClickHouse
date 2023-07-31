# Decimal(P,S),Decimal32(S),Decimal64(S),Decimal128(S) {#decimalp-s-decimal32s-decimal64s-decimal128s}

有符号的定点数，可在加、减和乘法运算过程中保持精度。对于除法，最低有效数字会被丢弃（不舍入）。

## 参数 {#can-shu}

-   P - 精度。有效范围：\[1:38\]，决定可以有多少个十进制数字（包括分数）。
-   S - 规模。有效范围：\[0：P\]，决定数字的小数部分中包含的小数位数。

对于不同的 P 参数值 Decimal 表示，以下例子都是同义的：
-P从\[1:9\]-对于Decimal32(S)
-P从\[10:18\]-对于Decimal64(小号)
-P从\[19:38\]-对于Decimal128（S)

## 十进制值范围 {#shi-jin-zhi-zhi-fan-wei}

-   Decimal32(S) - ( -1 \* 10^(9 - S),1\*10^(9-S) )
-   Decimal64(S) - ( -1 \* 10^(18 - S),1\*10^(18-S) )
-   Decimal128(S) - ( -1 \* 10^(38 - S),1\*10^(38-S) )

例如，Decimal32(4) 可以表示 -99999.9999 至 99999.9999 的数值，步长为0.0001。

## 内部表示方式 {#nei-bu-biao-shi-fang-shi}

数据采用与自身位宽相同的有符号整数存储。这个数在内存中实际范围会高于上述范围，从 String 转换到十进制数的时候会做对应的检查。

由于现代CPU不支持128位数字，因此 Decimal128 上的操作由软件模拟。所以 Decimal128 的运算速度明显慢于 Decimal32/Decimal64。

## 运算和结果类型 {#yun-suan-he-jie-guo-lei-xing}

对Decimal的二进制运算导致更宽的结果类型（无论参数的顺序如何）。

-   `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
-   `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
-   `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`

精度变化的规则：

-   加法，减法：S = max(S1, S2)。
-   乘法：S = S1 + S2。
-   除法：S = S1。

对于 Decimal 和整数之间的类似操作，结果是与参数大小相同的十进制。

未定义Decimal和Float32/Float64之间的函数。要执行此类操作，您可以使用：toDecimal32、toDecimal64、toDecimal128 或 toFloat32，toFloat64，需要显式地转换其中一个参数。注意，结果将失去精度，类型转换是昂贵的操作。

Decimal上的一些函数返回结果为Float64（例如，var或stddev）。对于其中一些，中间计算发生在Decimal中。对于此类函数，尽管结果类型相同，但Float64和Decimal中相同数据的结果可能不同。

## 溢出检查 {#yi-chu-jian-cha}

在对 Decimal 类型执行操作时，数值可能会发生溢出。分数中的过多数字被丢弃（不是舍入的）。整数中的过多数字将导致异常。

    SELECT toDecimal32(2, 4) AS x, x / 3

    ┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
    │ 2.0000 │                       0.6666 │
    └────────┴──────────────────────────────┘

    SELECT toDecimal32(4.2, 8) AS x, x * x

    DB::Exception: Scale is out of bounds.

    SELECT toDecimal32(4.2, 8) AS x, 6 * x

    DB::Exception: Decimal math overflow.

检查溢出会导致计算变慢。如果已知溢出不可能，则可以通过设置`decimal_check_overflow`来禁用溢出检查，在这种情况下，溢出将导致结果不正确：

    SET decimal_check_overflow = 0;
    SELECT toDecimal32(4.2, 8) AS x, 6 * x

    ┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
    │ 4.20000000 │                     -17.74967296 │
    └────────────┴──────────────────────────────────┘

溢出检查不仅发生在算术运算上，还发生在比较运算上：

    SELECT toDecimal32(1, 8) < 100

    DB::Exception: Can't compare.
