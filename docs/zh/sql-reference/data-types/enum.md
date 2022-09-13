# Enum8,Enum16 {#enum8-enum16}

包括 `Enum8` 和 `Enum16` 类型。`Enum` 保存 `'string'= integer` 的对应关系。在 ClickHouse 中，尽管用户使用的是字符串常量，但所有含有 `Enum` 数据类型的操作都是按照包含整数的值来执行。这在性能方面比使用 `String` 数据类型更有效。

-   `Enum8` 用 `'String'= Int8` 对描述。
-   `Enum16` 用 `'String'= Int16` 对描述。

## 用法示例 {#yong-fa-shi-li}

创建一个带有一个枚举 `Enum8('hello' = 1, 'world' = 2)` 类型的列：

    CREATE TABLE t_enum
    (
        x Enum8('hello' = 1, 'world' = 2)
    )
    ENGINE = TinyLog

这个 `x` 列只能存储类型定义中列出的值：`'hello'`或`'world'`。如果您尝试保存任何其他值，ClickHouse 抛出异常。

    :) INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')

    INSERT INTO t_enum VALUES

    Ok.

    3 rows in set. Elapsed: 0.002 sec.

    :) insert into t_enum values('a')

    INSERT INTO t_enum VALUES


    Exception on client:
    Code: 49. DB::Exception: Unknown element 'a' for type Enum8('hello' = 1, 'world' = 2)

当您从表中查询数据时，ClickHouse 从 `Enum` 中输出字符串值。

    SELECT * FROM t_enum

    ┌─x─────┐
    │ hello │
    │ world │
    │ hello │
    └───────┘

如果需要看到对应行的数值，则必须将 `Enum` 值转换为整数类型。

    SELECT CAST(x, 'Int8') FROM t_enum

    ┌─CAST(x, 'Int8')─┐
    │               1 │
    │               2 │
    │               1 │
    └─────────────────┘

在查询中创建枚举值，您还需要使用 `CAST`。

    SELECT toTypeName(CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'))

    ┌─toTypeName(CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'))─┐
    │ Enum8('a' = 1, 'b' = 2)                              │
    └──────────────────────────────────────────────────────┘

## 规则及用法 {#gui-ze-ji-yong-fa}

`Enum8` 类型的每个值范围是 `-128 ... 127`，`Enum16` 类型的每个值范围是 `-32768 ... 32767`。所有的字符串或者数字都必须是不一样的。允许存在空字符串。如果某个 Enum 类型被指定了（在表定义的时候），数字可以是任意顺序。然而，顺序并不重要。

`Enum` 中的字符串和数值都不能是 [NULL](../../sql-reference/data-types/enum.md)。

`Enum` 包含在 [可为空](nullable.md) 类型中。因此，如果您使用此查询创建一个表

    CREATE TABLE t_enum_nullable
    (
        x Nullable( Enum8('hello' = 1, 'world' = 2) )
    )
    ENGINE = TinyLog

不仅可以存储 `'hello'` 和 `'world'` ，还可以存储 `NULL`。

    INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)

在内存中，`Enum` 列的存储方式与相应数值的 `Int8` 或 `Int16` 相同。

当以文本方式读取的时候，ClickHouse 将值解析成字符串然后去枚举值的集合中搜索对应字符串。如果没有找到，会抛出异常。当读取文本格式的时候，会根据读取到的字符串去找对应的数值。如果没有找到，会抛出异常。

当以文本形式写入时，ClickHouse 将值解析成字符串写入。如果列数据包含垃圾数据（不是来自有效集合的数字），则抛出异常。Enum 类型以二进制读取和写入的方式与 `Int8` 和 `Int16` 类型一样的。

隐式默认值是数值最小的值。

在 `ORDER BY`，`GROUP BY`，`IN`，`DISTINCT` 等等中，Enum 的行为与相应的数字相同。例如，按数字排序。对于等式运算符和比较运算符，Enum 的工作机制与它们在底层数值上的工作机制相同。

枚举值不能与数字进行比较。枚举可以与常量字符串进行比较。如果与之比较的字符串不是有效Enum值，则将引发异常。可以使用 IN 运算符来判断一个 Enum 是否存在于某个 Enum 集合中，其中集合中的 Enum 需要用字符串表示。

大多数具有数字和字符串的运算并不适用于Enums；例如，Enum 类型不能和一个数值相加。但是，Enum有一个原生的 `toString` 函数，它返回它的字符串值。

Enum 值使用 `toT` 函数可以转换成数值类型，其中 T 是一个数值类型。若 `T` 恰好对应 Enum 的底层数值类型，这个转换是零消耗的。

Enum 类型可以被 `ALTER` 无成本地修改对应集合的值。可以通过 `ALTER` 操作来增加或删除 Enum 的成员（只要表没有用到该值，删除都是安全的）。作为安全保障，改变之前使用过的 Enum 成员将抛出异常。

通过 `ALTER` 操作，可以将 `Enum8` 转成 `Enum16`，反之亦然，就像 `Int8` 转 `Int16`一样。
