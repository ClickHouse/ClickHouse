---
description: 'RawBLOBフォーマットに関するドキュメント'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## 説明
</div>

`RawBLOB` フォーマットは、入力データ全体を 1 つの値として読み込みます。解析できるのは、[`String`](/ja/sql-reference/data-types/string.md) 型またはそれに類する型の単一のフィールドを持つテーブルのみです。
結果は、区切り文字やエスケープ処理を行わないバイナリ形式で出力されます。複数の値を出力すると、このフォーマットは曖昧になり、データを再度読み込めなくなります。

<div id="raw-formats-comparison">
  ### Rawフォーマットの比較
</div>

以下は、`RawBLOB` と [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md) フォーマットの比較です。

`RawBLOB`:

* データはバイナリ形式で出力され、エスケープは行われません。
* 値の間に区切り文字はありません。
* 各値の末尾に改行はありません。

`TabSeparatedRaw`:

* データはエスケープなしで出力されます。
* 各行にはタブ区切りの値が含まれます。
* 各行では、最後の値の後に改行が入ります。

以下は、`RawBLOB` と [RowBinary](./RowBinary/RowBinary.md) フォーマットの比較です。

`RawBLOB`:

* String フィールドは長さのプレフィックスなしで出力されます。

`RowBinary`:

* String フィールドは、varintフォーマットの長さ (符号なし [LEB128] (https://en.wikipedia.org/wiki/LEB128)) に続けて、文字列のバイト列として表現されます。

空のデータが `RawBLOB` への入力として渡されると、ClickHouse は例外をスローします:

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## 使用例
</div>

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

<div id="format-settings">
  ## フォーマット設定
</div>
