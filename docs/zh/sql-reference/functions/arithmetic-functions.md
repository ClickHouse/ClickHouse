# 算术函数 {#suan-zhu-han-shu}

对于所有算术函数，结果类型为结果适合的最小数字类型（如果存在这样的类型）。最小数字类型是根据数字的位数，是否有符号以及是否是浮点类型而同时进行的。如果没有足够的位，则采用最高位类型。

例如:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

    ┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
    │ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
    └───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘

算术函数适用于UInt8，UInt16，UInt32，UInt64，Int8，Int16，Int32，Int64，Float32或Float64中的任何类型。

溢出的产生方式与C++相同。

## 加(a,b),a+b {#plusa-b-a-b}

计算数字的总和。
您还可以将Date或DateTime与整数进行相加。在Date的情况下，添加的整数意味着添加相应的天数。对于DateTime，这意味这添加相应的描述。

## 减(a,b),a-b {#minusa-b-a-b}

计算数字之间的差，结果总是有符号的。

您还可以将Date或DateTime与整数进行相减。见上面的’plus’。

## 乘(a,b),a\*b {#multiplya-b-a-b}

计算数字的乘积。

## 除以(a,b),a/b {#dividea-b-a-b}

计算数字的商。结果类型始终是浮点类型。
它不是整数除法。对于整数除法，请使用’intDiv’函数。
当除以零时，你得到’inf’，‘- inf’或’nan’。

## intDiv(a,b) {#intdiva-b}

计算整数数字的商，向下舍入（按绝对值）。
除以零或将最小负数除以-1时抛出异常。

## intDivOrZero(a,b) {#intdivorzeroa-b}

与’intDiv’的不同之处在于它在除以零或将最小负数除以-1时返回零。

## 模(a,b),a%b {#moduloa-b-a-b}

计算除法后的余数。
如果参数是浮点数，则通过删除小数部分将它们预转换为整数。
其余部分与C++中的含义相同。截断除法用于负数。
除以零或将最小负数除以-1时抛出异常。

## 否定(a),-a {#negatea-a}

计算一个数字的
用反转符号计算一个数字。结果始终是签名的。
计算具有反向符号的数字。 结果始终签名。

## abs(a) {#arithm_func-abs}

计算数字（a）的绝对值。也就是说，如果a ＆lt; 0，它返回-a。对于无符号类型，它不执行任何操作。对于有符号整数类型，它返回无符号数。

## gcd(a,b) {#gcda-b}

返回数字的最大公约数。
除以零或将最小负数除以-1时抛出异常。

## lcm(a,b) {#lcma-b}

返回数字的最小公倍数。
除以零或将最小负数除以-1时抛出异常。

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
