---
alias: []
description: 'Oneフォーマットに関するドキュメント'
input_format: true
keywords: ['One']
output_format: false
slug: /interfaces/formats/One
title: 'One'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 説明
</div>

`One` フォーマットは特殊な入力フォーマットで、ファイルからデータを一切読み取らず、[`UInt8`](../../sql-reference/data-types/int-uint.md) 型の `dummy` という名前のカラムと値 `0` を持つ 1 行だけを返します (`system.one` テーブルと同様です) 。
仮想カラム `_file/_path` と組み合わせて使用すると、実際のデータを読み取ることなく、すべてのファイルを一覧表示できます。

<div id="example-usage">
  ## 使用例
</div>

例:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

<div id="format-settings">
  ## フォーマット設定
</div>
