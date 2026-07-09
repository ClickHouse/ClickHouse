---
description: 'ClickHouse 中枚举数据类型的文档，它表示一组已命名的常量值'
sidebar_label: '枚举'
sidebar_position: 20
slug: /sql-reference/data-types/enum
title: '枚举'
doc_type: 'reference'
---

由命名值组成的枚举类型。

命名值可以声明为 `'string' = integer` 对，也可以仅声明为 `'string'` 名称。ClickHouse 仅存储数字，但支持通过名称对这些值进行操作。

ClickHouse 支持：

* 8 位 `Enum`。最多可包含 256 个在 `[-128, 127]` 范围内枚举的值。
* 16 位 `Enum`。最多可包含 65536 个在 `[-32768, 32767]` 范围内枚举的值。

插入数据时，ClickHouse 会自动选择 `Enum` 类型。你也可以使用 `Enum8` 或 `Enum16` 类型，以明确存储大小。

<div id="usage-examples">
  ## 使用示例
</div>

下面我们创建一个包含 `Enum8('hello' = 1, 'world' = 2)` 类型列的表：

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

同样，也可以省略编号。ClickHouse 会自动按顺序分配编号。默认从 1 开始分配。

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

你还可以为第一个名称指定合法的起始编号。

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

列 `x` 只能存储类型定义中列出的值：`'hello'` 或 `'world'`。如果你尝试保存其他任何值，ClickHouse 将引发异常。此 `Enum` 的 8 比特大小会自动选择。

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

当你从表中查询数据时，ClickHouse 会输出 `Enum` 里的字符串值。

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

如果您需要查看这些行对应的数值，必须将 `Enum` 值强制转换为整数类型。

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

在查询中创建枚举值时，还需要使用 `CAST`。

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

<div id="general-rules-and-usage">
  ## 通用规则和用法
</div>

每个值都会分配一个数字：`Enum8` 的范围为 `-128 ... 127`，`Enum16` 的范围为 `-32768 ... 32767`。所有字符串和数字都必须互不相同。允许空字符串。如果指定了此类型 (在表定义中) ，数字可以按任意顺序排列。不过，顺序并不重要。

`Enum` 中的字符串和数值都不能为 [NULL](../../sql-reference/syntax.md)。

`Enum` 可以包含在 [Nullable](../../sql-reference/data-types/nullable.md) 类型中。因此，如果你使用以下查询创建表

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

它不仅能存储 `'hello'` 和 `'world'`，也能存储 `NULL`。

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

在 RAM 中，`枚举` 列的存储方式与对应数值的 `Int8` 或 `Int16` 相同。

以文本形式读取时，ClickHouse 会将该值解析为字符串，并在 Enum 值集合中查找对应的字符串；如果未找到，则会抛出异常。以文本格式读取时，会读取该字符串并查找对应的数值；如果未找到，同样会抛出异常。
以文本形式写入时，会将该值写成对应的字符串。如果列数据包含无效内容 (即不属于有效集合的数字) ，则会抛出异常。以二进制形式读写时，其工作方式与 Int8 和 Int16 数据类型相同。
隐式默认值是数值编号最小的那个值。

在 `ORDER BY`、`GROUP BY`、`IN`、`DISTINCT` 等操作中，枚举 的行为与对应的数字相同。例如，ORDER BY 会按数值顺序对它们进行排序。相等运算符和比较运算符作用于 枚举 的方式，也与作用于其底层数值的方式相同。

枚举 值不能与数字比较。枚举 可以与常量字符串比较。如果用于比较的字符串不是该 枚举 的有效值，则会抛出异常。支持左侧为 枚举、右侧为字符串集合的 IN 运算符。这些字符串就是对应 枚举 的取值。

大多数数值和字符串操作都未为 枚举 值定义，例如给 枚举 加上一个数字，或将字符串拼接到 枚举 上。
不过，枚举 原生提供了 `toString` 函数，会返回其字符串值。

枚举 值也可以使用 `toT` 函数转换为数值类型，其中 T 为数值类型。当 T 对应该 枚举 的底层数值类型时，这种转换是零成本的。
如果仅更改值集合，则可以使用 ALTER 零成本地修改 枚举 类型。可以使用 ALTER 为 枚举 添加和删除成员 (只有当被删除的值从未在表中使用过时，删除才是安全的) 。作为保护措施，更改先前已定义的 枚举 成员的数值会抛出异常。

使用 ALTER，可以将 Enum8 改为 Enum16，反之亦然，就像将 Int8 改为 Int16 一样。

<div id="add-enum-values">
  ## 添加 ENUM 值
</div>

可以使用 ALTER [MODIFY COLUMN ADD ENUM VALUES](../../sql-reference/statements/alter/column.md#modify-column-add-enum-values) 这一语法糖为枚举添加新值

```sql
CREATE TABLE enum
(
    x Enum('One' = 1, 'Two', 'Three')
) ENGINE = Memory;
ALTER TABLE enum MODIFY COLUMN x ADD ENUM VALUES ('Zero' = 0, 'Four' = 4);
SHOW CREATE TABLE enum;
```

```text
┌─statement────────────────────────────────────────────────────────────────┐
│CREATE TABLE default.enum                                                 │
│(                                                                         │
│    `x` Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4)  │
│)                                                                         │
│ENGINE = Memory                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```