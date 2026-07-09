---
description: 'DESCRIBE TABLE のドキュメント'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

テーブルのカラムに関する情報を返します。

**構文**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

`DESCRIBE` ステートメントは、各テーブルのカラムについて、次の [String](../../sql-reference/data-types/string.md) 型の値を含む行を返します。

* `name` — カラム名。
* `type` — カラムの型。
* `default_type` — カラムの[デフォルト式](/ja/sql-reference/statements/create/table)で使用される句: `DEFAULT`、`MATERIALIZED`、または `ALIAS`。デフォルト式がない場合は、空文字列が返されます。
* `default_expression` — `DEFAULT` 句の後に指定される式。
* `comment` — [カラムコメント](/ja/sql-reference/statements/alter/column#comment-column)。
* `codec_expression` — そのカラムに適用される [codec](/ja/sql-reference/statements/create/table#column_compression_codec)。
* `ttl_expression` — [有効期限 (TTL)](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 式。
* `is_subcolumn` — 内部 subcolumn の場合は `1` になるフラグ。[describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) 設定で subcolumn の説明が有効になっている場合にのみ、結果に含まれます。

[Nested](../../sql-reference/data-types/nested-data-structures/index.md) データ構造内のすべてのカラムは、個別に記述されます。各カラム名には、親カラム名とドットがプレフィックスとして付きます。

他の data types の内部 subcolumns を表示するには、[describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) 設定を使用します。

**例**

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

2つ目のクエリでは、サブカラムも表示されます：

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

DESCRIBE ステートメントは、サブクエリやスカラー式にも使用できます。

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

または

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

この構文は、指定されたクエリまたはサブクエリの結果カラムに関するメタデータを返します。実行前に複雑なクエリの構造を把握するのに役立ちます。

**関連項目**

* [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns) 設定。