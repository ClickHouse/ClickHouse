---
alias: []
description: 'Buffersフォーマットに関するドキュメント'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'リファレンス'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`Buffers` は、コンシューマーとプロデューサーの両方がすでにスキーマとカラムの順序を把握している場合に、一時的なデータ交換のために使われる非常にシンプルなバイナリフォーマットです。

[Native](./Native.md) とは異なり、カラム名、カラム型、追加のメタデータは**保存しません**。

このフォーマットでは、データはバイナリ形式で [ブロック](/ja/development/architecture#block) 単位に書き込みと読み取りが行われます。Buffers は [Native](./Native.md) フォーマットと同じカラムごとのバイナリ表現を使用し、同じ Native フォーマット設定に従います。

各ブロックについて、次の順序で書き込まれます。

1. カラム数 (UInt64、リトルエンディアン)。
2. 行数 (UInt64、リトルエンディアン)。
3. 各カラムについて:

* シリアライズされたカラムデータの合計バイトサイズ (UInt64、リトルエンディアン)。
* [Native](./Native.md) フォーマットとまったく同じシリアライズ済みカラムデータのバイト列。

<div id="example-usage">
  ## 使用例
</div>

ファイルへの書き込み:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

明示的なカラム型で読み戻します：

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

同じカラム型のテーブルがある場合は、直接データを投入できます。

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

次のテーブルを確認します。

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

<div id="format-settings">
  ## フォーマット設定
</div>
