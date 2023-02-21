---
sidebar_position: 40
sidebar_label: 关联表引擎
---

# 关联表引擎 {#join}

使用 [JOIN](../../../sql-reference/statements/select/join.md#select-join)操作的一种可选的数据结构。

!!! 注意 "Note"
    该文档和 [JOIN 语句](../../../sql-reference/statements/select/join.md#select-join) 无关.

## 建表语句 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

建表语句详情参见[创建表](../../../sql-reference/statements/create.md#create-table-query).

**引擎参数**

-   `join_strictness` – [JOIN 限制](../../../sql-reference/statements/select/join.md#select-join-types).
-   `join_type` – [JOIN 类型](../../../sql-reference/statements/select/join.md#select-join-types).
-   `k1[, k2, ...]` – 进行`JOIN` 操作时 `USING`语句用到的key列

使用`join_strictness` 和 `join_type` 参数时不需要用引号, 例如, `Join(ANY, LEFT, col1)`. 这些参数必须和进行join操作的表相匹配。否则，CH不会报错，但是可能返回错误的数据。

## 表用法 {#table-usage}

### 示例 {#example}

创建左关联表:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

创建 `Join` 右边的表:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

表关联:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

作为一种替换方式，可以从 `Join`表获取数据，需要设置好join的key字段值。

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### 数据查询及插入 {#selecting-and-inserting-data}

可以使用 `INSERT`语句向 `Join`引擎表中添加数据。如果表是通过指定 `ANY`限制参数来创建的，那么重复key的数据会被忽略。指定 `ALL`限制参数时，所有行记录都会被添加进去。

不能通过 `SELECT` 语句直接从表中获取数据。请使用下面的方式：
- 将表放在 `JOIN` 的右边进行查询
- 调用 [joinGet](../../../sql-reference/functions/other-functions.md#joinget)函数，就像从字典中获取数据一样来查询表。


### 使用限制及参数设置 {#join-limitations-and-settings}

创建表时，会应用下列设置参数：

-   [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)
-   [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [join_any_take_last_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)


`Join`表不能在 `GLOBAL JOIN`操作中使用

 `Join`表创建及 [查询](../../../sql-reference/statements/select/index.md)时，允许使用[join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)参数。如果使用不同的`join_use_nulls`设置，会导致表关联异常（取决于join的类型）。当使用函数 [joinGet](../../../sql-reference/functions/other-functions.md#joinget)时，请在建表和查询语句中使用相同的 `join_use_nulls` 参数设置。


## 数据存储 {#data-storage}

`Join`表的数据总是保存在内存中。当往表中插入行记录时，CH会将数据块保存在硬盘目录中，这样服务器重启时数据可以恢复。

如果服务器非正常重启，保存在硬盘上的数据块会丢失或被损坏。这种情况下，需要手动删除被损坏的数据文件。


[原始文档](https://clickhouse.com/docs/en/operations/table_engines/join/) <!--hide-->
