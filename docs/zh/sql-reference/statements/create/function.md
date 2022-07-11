---
sidebar_position: 38
sidebar_label: FUNCTION
---

# CREATE FUNCTION {#create-function}

用一个lambda表达式创建用户自定义函数。该表达式必须由函数参数、常数、运算符或其他函数调用组成。

**语法**

```sql
CREATE FUNCTION name AS (parameter0, ...) -> expression
```

一个函数可以有任意数量的参数。

存在一些限制如下:

-   函数名在用户自定义函数和系统函数中必须是唯一的。
-   递归函数是不允许的。
-   函数所使用的所有变量必须在其参数列表中指定。

如果违反了任何限制，就会产生异常。

**示例**

查询:

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

结果:

``` text
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

在下面的查询中，[conditional function](../../../sql-reference/functions/conditional-functions.md)在用户自定义函数中被调用:

```sql
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

结果:

``` text
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```
