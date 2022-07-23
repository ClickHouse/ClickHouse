---
sidebar_position: 35
sidebar_label: 算术函数
---

# 算术函数 {#arithmetic-functions}

对于所有算术函数，结果类型为结果适合的最小数值类型（如果存在这样的类型）。最小数值类型是根据数值的位数，是否有符号以及是否是浮点类型而同时进行的。如果没有足够的位，则采用最高位类型。

例如:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

    ┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
    │ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
    └───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘

算术函数适用于UInt8，UInt16，UInt32，UInt64，Int8，Int16，Int32，Int64，Float32或Float64中的任何类型。

溢出的产生方式与C++相同。

## plus(a, b), a + b operator {#plusa-b-a-b-operator}

计算数值的总和。
您还可以将Date或DateTime与整数进行相加。在Date的情况下，和整数相加整数意味着添加相应的天数。对于DateTime，这意味着添加相应的秒数。

## minus(a, b), a - b operator {#minusa-b-a-b-operator}

计算数值之间的差，结果总是有符号的。

您还可以将Date或DateTime与整数进行相减。见上面的’plus’。

## multiply(a, b), a \* b operator {#multiplya-b-a-b-operator}

计算数值的乘积。

## divide(a, b), a / b operator {#dividea-b-a-b-operator}

计算数值的商。结果类型始终是浮点类型。
它不是整数除法。对于整数除法，请使用’intDiv’函数。
当除以零时，你得到’inf’，‘- inf’或’nan’。

## intDiv(a,b) {#intdiva-b}

计算数值的商，向下舍入取整（按绝对值）。
除以零或将最小负数除以-1时抛出异常。

## intDivOrZero(a,b) {#intdivorzeroa-b}

与’intDiv’的不同之处在于它在除以零或将最小负数除以-1时返回零。

## modulo(a, b), a % b operator {#modulo}

计算除法后的余数。
如果参数是浮点数，则通过删除小数部分将它们预转换为整数。
其余部分与C++中的含义相同。截断除法用于负数。
除以零或将最小负数除以-1时抛出异常。

## moduloOrZero(a, b) {#modulo-or-zero}

和[modulo](#modulo)不同之处在于，除以0时结果返回0

## negate(a), -a operator {#negatea-a-operator}

通过改变数值的符号位对数值取反，结果总是有符号的

## abs(a) {#arithm_func-abs}

计算数值（a）的绝对值。也就是说，如果a \< 0，它返回-a。对于无符号类型，它不执行任何操作。对于有符号整数类型，它返回无符号数。

## gcd(a,b) {#gcda-b}

返回数值的最大公约数。
除以零或将最小负数除以-1时抛出异常。

## lcm(a,b) {#lcma-b}

返回数值的最小公倍数。
除以零或将最小负数除以-1时抛出异常。

## max2 {#max2}

比较两个值并返回最大值。返回值转换为[Float64](../../sql-reference/data-types/float.md)。

**语法**

```sql
max2(value1, value2)
```

**参数**

-   `value1` — 第一个值，类型为[Int/UInt](../../sql-reference/data-types/int-uint.md)或[Float](../../sql-reference/data-types/float.md)。
-   `value2` — 第二个值，类型为[Int/UInt](../../sql-reference/data-types/int-uint.md)或[Float](../../sql-reference/data-types/float.md)。

**返回值**

-   两个值中的最大值。

类型: [Float](../../sql-reference/data-types/float.md)。

**示例**

查询语句:

```sql
SELECT max2(-1, 2);
```

结果:

```text
┌─max2(-1, 2)─┐
│           2 │
└─────────────┘
```

## min2 {#min2}

比较两个值并返回最小值。返回值类型转换为[Float64](../../sql-reference/data-types/float.md)。

**语法**

```sql
max2(value1, value2)
```

**参数**

-   `value1` — 第一个值，类型为[Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md)。
-   `value2` — 第二个值，类型为[Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md)。

**返回值**

-   两个值中的最小值。

类型: [Float](../../sql-reference/data-types/float.md)。

**示例**

查询语句:

```sql
SELECT min2(-1, 2);
```

结果:

```text
┌─min2(-1, 2)─┐
│          -1 │
└─────────────┘
```

[来源文章](https://clickhouse.com/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
