---
description: 'FUNCTION 文档'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION - 用户自定义函数 (UDF)'
doc_type: 'reference'
---

根据 lambda 表达式创建用户自定义函数 (UDF)。该表达式必须由函数参数、常量、运算符或其他函数调用构成。

**语法**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

函数可以有任意数量的参数。

但有以下几点限制：

* 函数名称在用户自定义函数和系统函数中必须保持唯一。
* 不允许使用递归函数。
* 函数中使用的所有变量都必须在其参数列表中声明。

如果违反其中任何一条限制，就会引发异常。

**示例**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

以下查询在用户自定义函数中调用了[条件函数](../../../sql-reference/functions/conditional-functions.md)：

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

替换现有 UDF：

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## 相关内容
</div>

<div id="executable-udfs">
  ### [可执行 UDF](/zh/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [ClickHouse Cloud 中的用户自定义函数](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
