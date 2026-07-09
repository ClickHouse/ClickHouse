---
description: 'DESCRIBE TABLE 文档'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

返回表列信息。

**语法**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

`DESCRIBE` 语句会为表中的每个列返回一行，包含以下 [String](../../sql-reference/data-types/string.md) 值：

* `name` — 列名。
* `type` — 列类型。
* `default_type` — 列[默认表达式](/zh/sql-reference/statements/create/table)中使用的子句：`DEFAULT`、`MATERIALIZED` 或 `ALIAS`。如果没有默认表达式，则返回空字符串。
* `default_expression` — 在 `DEFAULT` 子句后指定的表达式。
* `comment` — [列注释](/zh/sql-reference/statements/alter/column#comment-column)。
* `codec_expression` — 应用于该列的 [codec](/zh/sql-reference/statements/create/table#column_compression_codec)。
* `ttl_expression` — [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 表达式。
* `is_subcolumn` — 对于内部子列，该标志的值为 `1`。只有在通过 [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) 设置启用子列描述时，此项才会包含在结果中。

[Nested](../../sql-reference/data-types/nested-data-structures/index.md) 数据结构中的所有列都会单独描述。每个列的名称都以前缀形式包含父列名称和一个点号。

要显示其他数据类型的内部子列，请使用 [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) 设置。

**示例**

```sql title="Query"
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

```text title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

第二个查询还会额外显示子列：

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

DESCRIBE 语句也可用于子查询或标量表达式：

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

或

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

这种用法会返回指定查询或子查询的结果列的元数据，有助于在执行前了解复杂查询的结构。

**另请参阅**

* [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) 设置。