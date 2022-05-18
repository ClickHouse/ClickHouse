---
sidebar_position: 42
sidebar_label: DESCRIBE
---

# DESCRIBE TABLE {#misc-describe-table}

Returns information about table columns.

**Syntax**

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

The `DESCRIBE` statement returns a row for each table column with the following [String](../../sql-reference/data-types/string.md) values:

-   `name` — A column name.
-   `type` — A column type.
-   `default_type` — A clause that is used in the column [default expression](../../sql-reference/statements/create/table.md#create-default-values): `DEFAULT`, `MATERIALIZED` or `ALIAS`. If there is no default expression, then empty string is returned.
-   `default_expression` — An expression specified after the `DEFAULT` clause.
-   `comment` — A [column comment](../../sql-reference/statements/alter/column.md#alter_comment-column).
-   `codec_expression` — A [codec](../../sql-reference/statements/create/table.md#codecs) that is applied to the column.
-   `ttl_expression` — A [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) expression.
-   `is_subcolumn` — A flag that equals `1` for internal subcolumns. It is included into the result only if subcolumn description is enabled by the [describe_include_subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) setting.

All columns in [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) data structures are described separately. The name of each column is prefixed with a parent column name and a dot.

To show internal subcolumns of other data types, use the [describe_include_subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) setting. 

**Example**

Query:

``` sql
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

Result:

``` text
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

The second query additionally shows subcolumns:

``` text
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

**See Also**

-   [describe_include_subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) setting.
