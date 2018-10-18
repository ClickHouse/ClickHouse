<a name="data_type-enum"></a>

# Enum8, Enum16

包括`Enum8`和`Enum16`类型 `Enum`保存`'string'= integer`的对应关系 在ClickHouse中，尽管用户使用的是字符串常量，但所有含有`Enum`数据类型的操作都是按照包含整数的值来执行 这在性能方面比使用`String`数据类型更有效

- `Enum8`用`'String'= Int8`对描述
- `Enum16`用`'String'= Int16`对描述

## 用法示例

创建一个带有一个枚举`Enum8('hello' = 1, 'world' = 2)`类型列：

```
CREATE TABLE t_enum
(
    x Enum8('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

这个`x`列只能存储类型定义中列出的值：`'hello'`或`'world'` 如果您尝试保存任何其他值，ClickHouse抛出异常

```
:) INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')

INSERT INTO t_enum VALUES

Ok.

3 rows in set. Elapsed: 0.002 sec.

:) insert into t_enum values('a')

INSERT INTO t_enum VALUES


Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum8('hello' = 1, 'world' = 2)
```

当您从表中查询数据时，ClickHouse从`Enum`中输出字符串值

```
SELECT * FROM t_enum

┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

如果需要看到对应行的数值，则必须将`Enum`值转换为整数类型

```
SELECT CAST(x, 'Int8') FROM t_enum

┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

在查询中创建枚举值，您还需要使用`CAST`

```
SELECT toTypeName(CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'))

┌─toTypeName(CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                              │
└──────────────────────────────────────────────────────┘
```

## 规则及用法

列中的值，`Enum8`的数值范围是`-128 ... 127`，`Enum16`的数值范围是`-32768 ... 32767`所有的字符串和数字必须是不同的允许空字符串如果指定了这种类型（在表定义中），则数字可以按任意顺序排列但是，顺序并不重要

`Enum`中的字符串和数值都不能是 [NULL](../query_language/syntax.md#null-literal)

`Enum`包含在 [Nullable](nullable.md#data_type-nullable) 类型中因此，如果您使用此查询创建一个表

```
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

不仅可以存储`'hello'`和`'world'`，还可以存储`NULL`

```
INSERT INTO t_enum_null Values('hello'),('world'),(NULL)
```

在内存中，`Enum`列的存储方式与相应数值的`Int8`或`Int16`相同

当以文本形式阅读时，ClickHouse将值解析为字符串，并从枚举值集中搜索对应的字符串如果未找到，则引发异常当以文本格式读取时，读取字符串并查找相应的数值如果未找到异常，则将抛出异常
当以文本形式写入时，它将值写入对应的字符串如果列数据包含垃圾数据（不是来自有效集合的数字），则引发异常当以二进制形式读写时，它的内部机制与Int8和Int16数据类型相同
隐式默认值是数值最小的值

在`ORDER BY`，`GROUP BY`，`IN`，`DISTINCT`等等中，枚举的行为与相应的数字相同例如，按数字排序对于等式运算符和比较运算符，枚举上的工作机制与它们在底层数值上的工作机制相同

枚举值无法与数字进行比较枚举可以与常量字符串进行比较如果与之比较的字符串不是有效Enum值，则将引发异常IN运算符支持左侧的Enum和右侧的一组字符串进行运算，字符串是相应枚举的值

大多数具有数字和字符串的操作没有意义，并且不适用于Enums；例如，您无法向Enum添加数字但是，Enum有一个原生的`toString`函数，它返回它的字符串值

同样对于Enum，定义了toT函数，其中T是数字类型当T与Enum列类型一致时，这种转换是零成本的如果只更改一组值，那么可以使用ALTER更改枚举类型，而不增加成本可以使用ALTER添加和删除枚举成员(只有在删除的值从未在表中使用时，删除才是安全的)作为保护措施，您无法更改现有字符串的数值，否则会抛出异常

使用ALTER，可以将Enum8换到Enum16，就像将Int8更改为Int16一样

