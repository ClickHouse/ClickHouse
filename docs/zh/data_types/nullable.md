<a name="data_type-nullable"></a>

# Nullable(TypeName)

允许加上一个符号 ([NULL](../query_language/syntax.md#null-literal)) 表示“缺失值”和 `TypeName` 允许的正常值。 例如，`Nullable（Int8）` 类型列可以存储 `Int8` 类型值，而没有值的行将存储 `NULL` 。

对于`TypeName`，您不能使用复合数据类型 [Array](array.md#data_type is array) 和 [Tuple](tuple.md#data_type-tuple)。 复合数据类型可以包含 `Nullable` 类型值，例如 `Array (Nullable(Int8))`。


`Nullable` 类型字段不能包含在表索引中。

除非在 ClickHouse 服务器配置中另有说明，否则 `NULL` 是任何 `Nullable` 类型的默认值。

## 存储特性

要在表的列中存储 `Nullable` 类型值，ClickHouse 除了使用带有值的普通文件外，还使用带有 `NULL` 掩码的单独文件。 掩码文件中的条目允许 ClickHouse 区分每个表行的 `NULL` 和相应数据类型的默认值。 由于附加了新文件，`Nullable` 列与类似的普通文件相比消耗额外的存储空间。

!!! info "Note"
    使用 `Nullable` 几乎总是会对性能产生负面影响，在设计数据库时请记住这一点。
    

## 使用案例

```
:) CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog

CREATE TABLE t_null
(
    x Int8,
    y Nullable(Int8)
)
ENGINE = TinyLog

Ok.

0 rows in set. Elapsed: 0.012 sec.

:) INSERT INTO t_null VALUES (1, NULL)

INSERT INTO t_null VALUES

Ok.

1 rows in set. Elapsed: 0.007 sec.

:) SELECT x + y FROM t_null

SELECT x + y
FROM t_null

┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘

2 rows in set. Elapsed: 0.144 sec.
```

[来源文章](https://clickhouse.yandex/docs/en/data_types/nullable/) <!--hide-->
