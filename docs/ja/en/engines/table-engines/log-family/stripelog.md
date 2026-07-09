---
description: 'StripeLog テーブルエンジンのドキュメント'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'StripeLog テーブルエンジン'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # StripeLog テーブルエンジン
</div>

<CloudNotSupportedBadge />

このエンジンは、Logエンジンファミリーに属しています。Logエンジンに共通する特性とその違いについては、[Log Engine Family](../../../engines/table-engines/log-family/index.md)の記事を参照してください。

このエンジンは、少量のデータ (100万行未満) を扱う多数のテーブルに書き込む必要がある場合に使用します。たとえばこのテーブルは、原子的な処理が必要な、変換対象の受信データバッチを保存するために使用できます。ClickHouse server では、この種類のテーブルを10万インスタンス運用できます。多数のテーブルが必要な場合は、このテーブルエンジンを [Log](./log.md) より優先して使用してください。ただし、その分読み取り効率は低下します。

<div id="table_engines-stripelog-creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

[CREATE TABLE](/ja/sql-reference/statements/create/table)クエリの詳しい説明を参照してください。

<div id="table_engines-stripelog-writing-the-data">
  ## データの書き込み
</div>

`StripeLog` エンジンは、すべてのカラムを 1 つのファイルに格納します。`INSERT` クエリごとに、ClickHouse はデータブロックをテーブルファイルの末尾に追記し、カラムを 1 つずつ書き込みます。

テーブルごとに、ClickHouse は次のファイルを書き込みます。

* `data.bin` — データファイル。
* `index.mrk` — マークファイル。マークには、挿入された各データブロック内の各カラムのオフセットが含まれます。

`StripeLog` エンジンは、`ALTER UPDATE` および `ALTER DELETE` 操作をサポートしていません。

<div id="table_engines-stripelog-reading-the-data">
  ## データの読み取り
</div>

マークファイルにより、ClickHouse はデータの読み取りを並列化できます。そのため、`SELECT` クエリが返す行の順序は一定ではありません。行をソートするには、`ORDER BY` 句を使用してください。

<div id="table_engines-stripelog-example-of-use">
  ## 使用例
</div>

テーブルの作成:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

データの挿入：

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

`data.bin` ファイル内に 2 つのデータブロックを作成するため、2 つの `INSERT` クエリを使用しました。

ClickHouse は、データを選択する際に複数のスレッドを使用します。各スレッドはそれぞれ別のデータブロックを読み取り、処理が完了すると結果の行を個別に返します。そのため、ほとんどの場合、出力される行ブロックの順序は、入力時の同じブロックの順序と一致しません。たとえば、次のようになります。

```sql
SELECT * FROM stripe_log_table
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

結果をソートする (デフォルトでは昇順) ：

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```