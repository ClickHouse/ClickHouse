---
description: 'MergeTree テーブル内のテキスト索引の Dictionary を表します。
  内部情報の確認に利用できます。'
sidebar_label: 'mergeTreeTextIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeTextIndex
title: 'mergeTreeTextIndex'
doc_type: 'reference'
---

MergeTree テーブル内のテキスト索引の Dictionary を表します。
トークンとそのポスティングリストメタデータを返します。
内部情報の確認に利用できます。

<div id="syntax">
  ## 構文
</div>

```sql
mergeTreeTextIndex(database, table, index_name)
```

<div id="arguments">
  ## 引数
</div>

| 引数           | 説明                      |
| ------------ | ----------------------- |
| `database`   | テキスト索引の読み取り元となるデータベース名。 |
| `table`      | テキスト索引の読み取り元となるテーブル名。   |
| `index_name` | 読み取り元のテキスト索引名。          |

<div id="returned_value">
  ## 戻り値
</div>

トークンとそのポスティングリストメタデータを含むテーブル。

<div id="usage-example">
  ## 使用例
</div>

```sql title="Query"
CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concatWithSeparator(' ', 'apple', 'banana') FROM numbers(500);
INSERT INTO tab SELECT 500 + number, concatWithSeparator(' ', 'cherry', 'date') FROM numbers(500);

SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_s);
```

```text title="Response"
   ┌─part_name─┬─token──┬─dictionary_compression─┬─cardinality─┬─num_posting_blocks─┬─has_embedded_postings─┬─has_raw_postings─┬─has_compressed_postings─┐
1. │ all_1_1_0 │ apple  │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
2. │ all_1_1_0 │ banana │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
3. │ all_2_2_0 │ cherry │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
4. │ all_2_2_0 │ date   │ front_coded            │         500 │                  1 │                     0 │                0 │                       0 │
   └───────────┴────────┴────────────────────────┴─────────────┴────────────────────┴───────────────────────┴──────────────────┴─────────────────────────┘
```