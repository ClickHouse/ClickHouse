---
description: 'MergeTree テーブルの索引ファイルおよびマークファイルの内容を表します。
  内部情報の確認に使用できます。'
sidebar_label: 'mergeTreeIndex'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeIndex
title: 'mergeTreeIndex'
doc_type: 'reference'
---

MergeTree テーブルの索引ファイルおよびマークファイルの内容を表します。内部情報の確認に使用できます。

<div id="syntax">
  ## 構文
</div>

```sql
mergeTreeIndex(database, table [, with_marks = true] [, with_minmax = true])
```

<div id="arguments">
  ## 引数
</div>

| 引数            | 説明                          |
| ------------- | --------------------------- |
| `database`    | 索引と marks の読み取り元となるデータベース名。 |
| `table`       | 索引と marks の読み取り元となるテーブル名。   |
| `with_marks`  | 結果に marks を持つカラムを含めるかどうか。   |
| `with_minmax` | 結果に min-max 索引を含めるかどうか。     |

<div id="returned_value">
  ## 戻り値
</div>

プライマリインデックスおよび min-max 索引 (有効な場合) の値を持つカラム、ソーステーブルのデータパーツ内に存在し得るすべてのファイルについて marks の値を持つカラム (有効な場合) 、および仮想カラムを含むテーブルオブジェクトです。

* `part_name` - データパーツの名前。
* `mark_number` - データパーツ内の現在の mark 番号。
* `rows_in_granule` - 現在のグラニュール内の行数。

marks カラムには、カラムがデータパーツ内に存在しない場合や、そのサブストリームのいずれかの marks が書き込まれていない場合 (たとえば compact パーツ) に、`(NULL, NULL)` の値が含まれることがあります。

<div id="usage-example">
  ## 使用例
</div>

```sql
CREATE TABLE test_table
(
    `id` UInt64,
    `n` UInt64,
    `arr` Array(UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 8;

INSERT INTO test_table SELECT number, number, range(number % 5) FROM numbers(5);

INSERT INTO test_table SELECT number, number, range(number % 5) FROM numbers(10, 10);
```

```sql
SELECT * FROM mergeTreeIndex(currentDatabase(), test_table, with_marks = true);
```

```text
┌─part_name─┬─mark_number─┬─rows_in_granule─┬─id─┬─id.mark─┬─n.mark──┬─arr.size0.mark─┬─arr.mark─┐
│ all_1_1_0 │           0 │               3 │  0 │ (0,0)   │ (42,0)  │ (NULL,NULL)    │ (84,0)   │
│ all_1_1_0 │           1 │               2 │  3 │ (133,0) │ (172,0) │ (NULL,NULL)    │ (211,0)  │
│ all_1_1_0 │           2 │               0 │  4 │ (271,0) │ (271,0) │ (NULL,NULL)    │ (271,0)  │
└───────────┴─────────────┴─────────────────┴────┴─────────┴─────────┴────────────────┴──────────┘
┌─part_name─┬─mark_number─┬─rows_in_granule─┬─id─┬─id.mark─┬─n.mark─┬─arr.size0.mark─┬─arr.mark─┐
│ all_2_2_0 │           0 │               3 │ 10 │ (0,0)   │ (0,0)  │ (0,0)          │ (0,0)    │
│ all_2_2_0 │           1 │               3 │ 13 │ (0,24)  │ (0,24) │ (0,24)         │ (0,24)   │
│ all_2_2_0 │           2 │               3 │ 16 │ (0,48)  │ (0,48) │ (0,48)         │ (0,80)   │
│ all_2_2_0 │           3 │               1 │ 19 │ (0,72)  │ (0,72) │ (0,72)         │ (0,128)  │
│ all_2_2_0 │           4 │               0 │ 19 │ (0,80)  │ (0,80) │ (0,80)         │ (0,160)  │
└───────────┴─────────────┴─────────────────┴────┴─────────┴────────┴────────────────┴──────────┘
```

```sql
DESCRIBE mergeTreeIndex(currentDatabase(), test_table, with_marks = true) SETTINGS describe_compact_output = 1;
```

```text
┌─name────────────┬─type─────────────────────────────────────────────────────────────────────────────────────────────┐
│ part_name       │ String                                                                                           │
│ mark_number     │ UInt64                                                                                           │
│ rows_in_granule │ UInt64                                                                                           │
│ id              │ UInt64                                                                                           │
│ id.mark         │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ n.mark          │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ arr.size0.mark  │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
│ arr.mark        │ Tuple(offset_in_compressed_file Nullable(UInt64), offset_in_decompressed_block Nullable(UInt64)) │
└─────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘
```