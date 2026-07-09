---
description: 'Log に関するドキュメント'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Log テーブルエンジン'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Log テーブルエンジン
</div>

<CloudNotSupportedBadge />

このエンジンは `Log` エンジンファミリーに属します。`Log` エンジンの共通プロパティとそれぞれの違いについては、[Log Engine Family](../../../engines/table-engines/log-family/index.md) の記事を参照してください。

`Log` は、カラムファイルに加えて小さな「マーク」ファイルを持つ点で [TinyLog](../../../engines/table-engines/log-family/tinylog.md) と異なります。これらの マーク は各データブロックごとに書き込まれ、指定した行数をスキップするために、ファイルのどの位置から読み始めるかを示すオフセットを含みます。これにより、テーブルデータを複数のスレッドで読み取ることが可能になります。
同時実行のデータアクセスでは、読み取り操作は同時に実行できますが、書き込み操作は読み取りおよび他の書き込みをブロックします。
`Log` エンジンは索引をサポートしていません。同様に、テーブルへの書き込みに失敗すると、そのテーブルは破損し、読み取り時にエラーが返されます。`Log` エンジンは、一時データや一度だけ書き込むテーブル、テストやデモ用途に適しています。

<div id="table_engines-log-creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

[CREATE TABLE](/ja/sql-reference/statements/create/table) クエリの詳しい説明を参照してください。

<div id="table_engines-log-writing-the-data">
  ## データの書き込み
</div>

`Log` エンジンは、各カラムを個別のファイルに書き込むことで、データを効率的に保存します。各テーブルについて、`Log` エンジンは指定されたストレージパスに次のファイルを書き込みます。

* `<column>.bin`: 各カラム用のデータファイルで、シリアライズおよび圧縮されたデータが含まれます。
  `__marks.mrk`: マークファイルで、挿入された各データブロックのオフセットと行数を保存します。マークは、読み取り時にエンジンが関係のないデータブロックをスキップできるようにすることで、クエリを効率的に実行できるようにするために使われます。

<div id="writing-process">
  ### 書き込みプロセス
</div>

`Log` テーブルにデータが書き込まれると、次の処理が行われます。

1. データはシリアライズされ、圧縮されたブロックにまとめられます。
2. 各カラムについて、圧縮されたデータが対応する `<column>.bin` ファイルに追記されます。
3. 新たに挿入されたデータのオフセットと行数を記録するため、対応するエントリが `__marks.mrk` ファイルに追加されます。

<div id="table_engines-log-reading-the-data">
  ## データの読み取り
</div>

マークファイルにより、ClickHouse はデータの読み取りを並列化できます。つまり、`SELECT` クエリが返す行の順序は一定ではありません。行をソートするには、`ORDER BY` 句を使用します。

<div id="table_engines-log-example-of-use">
  ## 使用例
</div>

テーブルの作成:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

データの挿入：

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

2 つの `INSERT` クエリを使って、`<column>.bin` ファイル内に 2 つのデータブロックを作成しました。

ClickHouse はデータを選択する際に複数のスレッドを使用します。各スレッドはそれぞれ別のデータブロックを読み取り、処理が完了した時点で結果の行を個別に返します。そのため、出力内の行ブロックの順序は、入力内の同じブロックの順序と一致しないことがあります。たとえば、次のようになります。

```sql
SELECT * FROM log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

結果をソートします (デフォルトは昇順) :

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```