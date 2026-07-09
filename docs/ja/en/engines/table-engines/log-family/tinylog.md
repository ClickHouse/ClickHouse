---
description: 'TinyLog テーブルエンジンに関するドキュメント'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'TinyLog テーブルエンジン'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # TinyLog テーブルエンジン
</div>

<CloudNotSupportedBadge />

このエンジンは、ログエンジンファミリーに属します。ログエンジンに共通する特性とその違いについては、[Log Engine Family](../../../engines/table-engines/log-family/index.md)を参照してください。

このテーブルエンジンは通常、書き込みを一度だけ行う方式で使用されます。つまり、データを一度書き込み、その後は必要に応じて何度でも読み取ります。たとえば、`TinyLog` 型のテーブルは、小さなバッチで処理される中間データに使用できます。なお、多数の小さなテーブルにデータを保存するのは非効率です。

クエリは単一ストリームで実行されます。つまり、このエンジンは比較的小さなテーブル (最大で約 1,000,000 行) 向けです。開く必要があるファイル数が少なく、[Log](../../../engines/table-engines/log-family/log.md) エンジンよりもシンプルなため、小さなテーブルを多数扱う場合には、このテーブルエンジンの使用が適しています。

<div id="characteristics">
  ## 特徴
</div>

* **よりシンプルな構造**: TinyLog は Log エンジンと異なり、マークファイルを使用しません。これにより複雑さは軽減されますが、大規模なデータセットに対するパフォーマンス最適化の余地は限られます。
* **単一ストリームクエリ**: TinyLog テーブルに対するクエリは単一ストリームで実行されるため、通常は最大 1,000,000 行程度の比較的小規模なテーブルに適しています。
* **小規模テーブルで効率的**: TinyLog エンジンはシンプルなため、多数の小規模なテーブルを管理する場合に適しています。Log エンジンと比べて必要なファイル操作が少なくて済みます。

TinyLog は Log エンジンと異なり、マークファイルを使用しません。これにより複雑さは軽減されますが、より大規模なデータセットに対するパフォーマンス最適化の余地は限られます。

<div id="table_engines-tinylog-creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

[CREATE TABLE](/ja/sql-reference/statements/create/table) クエリの詳しい説明を参照してください。

<div id="table_engines-tinylog-writing-the-data">
  ## データの書き込み
</div>

`TinyLog` エンジンは、すべてのカラムを 1 つのファイルに格納します。`INSERT` クエリが実行されるたびに、ClickHouse はデータブロックをテーブルファイルの末尾に追記し、カラムを 1 つずつ書き込みます。

ClickHouse は各テーブルについて、次のファイルを書き込みます。

* `<column>.bin`: 各カラム用のデータファイルで、シリアライズおよび圧縮されたデータが格納されます。

`TinyLog` エンジンは、`ALTER UPDATE` および `ALTER DELETE` をサポートしていません。

<div id="table_engines-tinylog-example-of-use">
  ## 使用例
</div>

テーブルを作成する例:

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

データの挿入：

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

2 つの `INSERT` クエリを使用して、`<column>.bin` ファイル内に 2 つのデータブロックを作成しました。

ClickHouse はデータの選択に単一ストリームを使用します。そのため、出力における行ブロックの順序は、入力における同じブロックの順序と一致します。たとえば、次のとおりです。

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```