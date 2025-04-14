---
slug: /zh/sql-reference/functions/null-functions
---
# Nullable处理函数 {#nullablechu-li-han-shu}

## isNull {#isnull}

检查参数是否为[NULL](../../sql-reference/syntax.md#null-literal)。

    isNull(x)

**参数**

-   `x` — 一个非复合数据类型的值。

**返回值**

-   `1` 如果`x`为`NULL`。
-   `0` 如果`x`不为`NULL`。

**示例**

存在以下内容的表

```response
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

对其进行查询

```sql
SELECT x FROM t_null WHERE isNull(y)
```

```response
┌─x─┐
│ 1 │
└───┘
```

## isNotNull {#isnotnull}

检查参数是否不为 [NULL](../../sql-reference/syntax.md#null-literal).

    isNotNull(x)

**参数:**

-   `x` — 一个非复合数据类型的值。

**返回值**

-   `0` 如果`x`为`NULL`。
-   `1` 如果`x`不为`NULL`。

**示例**

存在以下内容的表

```response
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

对其进行查询

```sql
SELECT x FROM t_null WHERE isNotNull(y)
```

```response
┌─x─┐
│ 2 │
└───┘
```

## 合并 {#coalesce}

检查从左到右是否传递了«NULL»参数并返回第一个非`'NULL`参数。

    coalesce(x,...)

**参数:**

-   任何数量的非复合类型的参数。所有参数必须与数据类型兼容。

**返回值**

-   第一个非’NULL\`参数。
-   `NULL`，如果所有参数都是’NULL\`。

**示例**

考虑可以指定多种联系客户的方式的联系人列表。

```response
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

`mail`和`phone`字段是String类型，但`icq`字段是`UInt32`，所以它需要转换为`String`。

从联系人列表中获取客户的第一个可用联系方式：

```sql
SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook
```

```response
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull {#ifnull}

如果第一个参数为«NULL»，则返回第二个参数的值。

    ifNull(x,alt)

**参数:**

-   `x` — 要检查«NULL»的值。
-   `alt` — 如果`x`为’NULL\`，函数返回的值。

**返回值**

-   价值 `x`，如果 `x` 不是 `NULL`.
-   价值 `alt`，如果 `x` 是 `NULL`.

**示例**

    SELECT ifNull('a', 'b')

    ┌─ifNull('a', 'b')─┐
    │ a                │
    └──────────────────┘

    SELECT ifNull(NULL, 'b')

    ┌─ifNull(NULL, 'b')─┐
    │ b                 │
    └───────────────────┘

## nullIf {#nullif}

如果参数相等，则返回`NULL`。

    nullIf(x, y)

**参数:**

`x`, `y` — 用于比较的值。 它们必须是类型兼容的，否则将抛出异常。

**返回值**

-   如果参数相等，则为`NULL`。
-   如果参数不相等，则为`x`值。

**示例**

    SELECT nullIf(1, 1)

    ┌─nullIf(1, 1)─┐
    │         ᴺᵁᴸᴸ │
    └──────────────┘

    SELECT nullIf(1, 2)

    ┌─nullIf(1, 2)─┐
    │            1 │
    └──────────────┘

## assumeNotNull {#assumenotnull}

将[可为空](../../sql-reference/functions/functions-for-nulls.md)类型的值转换为非`Nullable`类型的值。

    assumeNotNull(x)

**参数：**

-   `x` — 原始值。

**返回值**

-   如果`x`不为`NULL`，返回非`Nullable`类型的原始值。
-   如果`x`为`NULL`，则返回任意值。

**示例**

存在如下`t_null`表。

    SHOW CREATE TABLE t_null

    ┌─statement─────────────────────────────────────────────────────────────────┐
    │ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
    └───────────────────────────────────────────────────────────────────────────┘

    ┌─x─┬────y─┐
    │ 1 │ ᴺᵁᴸᴸ │
    │ 2 │    3 │
    └───┴──────┘

将列`y`作为`assumeNotNull`函数的参数。

    SELECT assumeNotNull(y) FROM t_null

    ┌─assumeNotNull(y)─┐
    │                0 │
    │                3 │
    └──────────────────┘

    SELECT toTypeName(assumeNotNull(y)) FROM t_null

    ┌─toTypeName(assumeNotNull(y))─┐
    │ Int8                         │
    │ Int8                         │
    └──────────────────────────────┘

## 可调整 {#tonullable}

将参数的类型转换为`Nullable`。

    toNullable(x)

**参数：**

-   `x` — 任何非复合类型的值。

**返回值**

-   输入的值，但其类型为`Nullable`。

**示例**

    SELECT toTypeName(10)

    ┌─toTypeName(10)─┐
    │ UInt8          │
    └────────────────┘

    SELECT toTypeName(toNullable(10))

    ┌─toTypeName(toNullable(10))─┐
    │ Nullable(UInt8)            │
    └────────────────────────────┘
