---
sidebar_position: 36
sidebar_label: 比较函数
---

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

## 等于，a=b和a==b 运算符 {#equals-a-b-and-a-b-operator}

## 不等于，a!=b和a&lt;&gt;b 运算符 {#notequals-a-operator-b-and-a-b}

## 少, &lt; 运算符 {#less-operator}

## 大于, &gt; 运算符 {#greater-operator}

## 小于等于, &lt;= 运算符 {#lessorequals-operator}

## 大于等于, &gt;= 运算符 {#greaterorequals-operator}

[来源文章](https://clickhouse.com/docs/en/query_language/functions/comparison_functions/) <!--hide-->
