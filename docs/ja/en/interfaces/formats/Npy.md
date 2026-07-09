---
alias: []
description: 'Npy フォーマットに関するドキュメント'
input_format: true
keywords: ['Npy']
output_format: true
slug: /interfaces/formats/Npy
title: 'Npy'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`Npy` フォーマットは、`.npy` ファイルから NumPy の Array を ClickHouse に読み込むために設計されています。
NumPy のファイルフォーマットは、数値データの配列を効率的に保存するためのバイナリ形式です。
インポート時、ClickHouse は最上位の次元を、単一のカラムを持つ行の配列として扱います。

以下の表は、サポートされている Npy のデータ型と、それに対応する ClickHouse の型を示しています。

<div id="data_types-matching">
  ## データ型の対応
</div>

| Npy データ型 (`INSERT`) | ClickHouse データ型                                         | Npy データ型 (`SELECT`) |
| ------------------- | ------------------------------------------------------- | ------------------- |
| `i1`                | [Int8](/ja/sql-reference/data-types/int-uint.md)           | `i1`                |
| `i2`                | [Int16](/ja/sql-reference/data-types/int-uint.md)          | `i2`                |
| `i4`                | [Int32](/ja/sql-reference/data-types/int-uint.md)          | `i4`                |
| `i8`                | [Int64](/ja/sql-reference/data-types/int-uint.md)          | `i8`                |
| `u1`, `b1`          | [UInt8](/ja/sql-reference/data-types/int-uint.md)          | `u1`                |
| `u2`                | [UInt16](/ja/sql-reference/data-types/int-uint.md)         | `u2`                |
| `u4`                | [UInt32](/ja/sql-reference/data-types/int-uint.md)         | `u4`                |
| `u8`                | [UInt64](/ja/sql-reference/data-types/int-uint.md)         | `u8`                |
| `f2`, `f4`          | [Float32](/ja/sql-reference/data-types/float.md)           | `f4`                |
| `f8`                | [Float64](/ja/sql-reference/data-types/float.md)           | `f8`                |
| `S`, `U`            | [String](/ja/sql-reference/data-types/string.md)           | `S`                 |
|                     | [FixedString](/ja/sql-reference/data-types/fixedstring.md) | `S`                 |

<div id="example-usage">
  ## 使用例
</div>

<div id="saving-an-array-in-npy-format-using-python">
  ### Pythonを使用して配列を .npy 形式で保存する
</div>

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

<div id="reading-a-numpy-file-in-clickhouse">
  ### ClickHouseでNumPyファイルを読み込む
</div>

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

<div id="selecting-data">
  ### データの選択
</div>

`clickhouse-client` で次のコマンドを実行すると、ClickHouseテーブルからデータを選択し、Npy フォーマット のファイルに保存できます。

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

<div id="format-settings">
  ## フォーマット設定
</div>
