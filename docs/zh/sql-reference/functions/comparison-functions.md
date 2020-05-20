# 比较函数 {#bi-jiao-han-shu}

比较函数始终返回0或1（UInt8）。

可以比较以下类型：

-   数字
-   String 和 FixedString
-   日期
-   日期时间

以上每个组内的类型均可互相比较，但是对于不同组的类型间不能够进行比较。

例如，您无法将日期与字符串进行比较。您必须使用函数将字符串转换为日期，反之亦然。

字符串按字节进行比较。较短的字符串小于以其开头并且至少包含一个字符的所有字符串。

注意。直到1.1.54134版本，有符号和无符号数字的比较方式与C++相同。换句话说，在SELECT 9223372036854775807 ＆gt; -1 等情况下，您可能会得到错误的结果。 此行为在版本1.1.54134中已更改，现在在数学上是正确的。

## 等于，a=b和a==b运算符 {#equals-a-b-and-a-b-operator}

## notEquals,a! 运算符=b和a `<>` b {#notequals-a-operator-b-and-a-b}

## 少, `< operator` {#less-operator}

## 更大, `> operator` {#greater-operator}

## 出租等级, `<= operator` {#lessorequals-operator}

## 伟大的等级, `>= operator` {#greaterorequals-operator}

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/comparison_functions/) <!--hide-->
