# 可为空（类型名称) {#data_type-nullable}

允许用特殊标记 ([NULL](../../sql-reference/data-types/nullable.md)) 表示«缺失值»，可以与 `TypeName` 的正常值存放一起。例如，`Nullable(Int8)` 类型的列可以存储 `Int8` 类型值，而没有值的行将存储 `NULL`。

对于 `TypeName`，不能使用复合数据类型 [阵列](array.md) 和 [元组](tuple.md)。复合数据类型可以包含 `Nullable` 类型值，例如`Array(Nullable(Int8))`。

`Nullable` 类型字段不能包含在表索引中。

除非在 ClickHouse 服务器配置中另有说明，否则 `NULL` 是任何 `Nullable` 类型的默认值。

## 存储特性 {#cun-chu-te-xing}

要在表的列中存储 `Nullable` 类型值，ClickHouse 除了使用带有值的普通文件外，还使用带有 `NULL` 掩码的单独文件。 掩码文件中的条目允许 ClickHouse 区分每个表行的 `NULL` 和相应数据类型的默认值。 由于附加了新文件，`Nullable` 列与类似的普通文件相比消耗额外的存储空间。

!!! 注意点 "注意点"
    使用 `Nullable` 几乎总是对性能产生负面影响，在设计数据库时请记住这一点

掩码文件中的条目允许ClickHouse区分每个表行的对应数据类型的«NULL»和默认值由于有额外的文件，«Nullable»列比普通列消耗更多的存储空间

## 用法示例 {#yong-fa-shi-li}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[来源文章](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
