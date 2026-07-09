---
description: '指定された構造でサーバーに送信されたデータを、別の構造を持つテーブルに効率的に変換して挿入できる
  テーブル関数。'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` - 指定された構造でサーバーに送信されたデータを、別の構造を持つテーブルに効率的に変換して挿入できる
テーブル関数です。

`structure` - 次の形式でサーバーに送信されるデータの構造です: `'column1_name column1_type, column2_name column2_type, ...'`。
たとえば、`'id UInt32, name String'` です。

この関数は `INSERT SELECT` クエリでのみ使用でき、使用できるのは 1 回だけですが、それ以外は通常のテーブル関数と同様に動作します
(たとえば、サブクエリなどで使用できます) 。

データは通常の `INSERT` クエリと同じように送信でき、利用可能な任意の[フォーマット](/ja/sql-reference/formats)
で渡すことができます。そのフォーマットはクエリの末尾で指定する必要があります (通常の `INSERT SELECT` とは異なります) 。

この関数の主な特徴は、サーバーがクライアントからデータを受信すると同時に、それを `SELECT` 句内の式の一覧に従って変換し、
ターゲットテーブルに挿入することです。転送されたすべてのデータを保持する一時テーブルは作成されません。

<div id="examples">
  ## 例
</div>

* `test` テーブルの構造が `(a String, b String)` で、
  `data.csv` 内のデータの構造がそれとは異なる `(col1 String, col2 Date, col3 Int32)` であるとします。`data.csv` から `test` テーブルにデータを挿入する際に、同時に変換を行うクエリは次のようになります。

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* `data.csv` にテーブル `test` と同じ構造の `test_structure` データが含まれている場合、以下の2つのクエリは同等です。

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```