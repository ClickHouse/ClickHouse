---
slug: /ja/engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: StripeLog
---

# Stripelog

このエンジンはログエンジンファミリーに属します。ログエンジンの共通の特性とその違いについては、[Log Engine Family](../../../engines/table-engines/log-family/index.md)の記事を参照してください。

このエンジンは、小量のデータ（100万行未満）を持つ多くのテーブルを書き込む必要があるシナリオで使用します。

## テーブルの作成 {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) クエリの詳細な説明を参照してください。

## データの書き込み {#table_engines-stripelog-writing-the-data}

`StripeLog`エンジンはすべてのカラムを1つのファイルに保存します。各`INSERT`クエリに対して、ClickHouseはデータブロックをテーブルファイルの末尾に追加し、カラムを一つずつ書き込みます。

各テーブルに対してClickHouseは以下のファイルを書き込みます：

- `data.bin` — データファイル。
- `index.mrk` — マークを含むファイル。マークは、挿入された各データブロックの各カラムのオフセットを含みます。

`StripeLog`エンジンは`ALTER UPDATE`および`ALTER DELETE`操作をサポートしていません。

## データの読み取り {#table_engines-stripelog-reading-the-data}

マークを含むファイルにより、ClickHouseはデータの読み取りを並列化することができます。これにより、`SELECT`クエリが行を予測不能な順序で返します。行をソートするには`ORDER BY`句を使用してください。

## 使用例 {#table_engines-stripelog-example-of-use}

テーブルの作成：

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

データの挿入：

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

`data.bin`ファイル内に2つのデータブロックを作成するために、2つの`INSERT`クエリを使用しました。

ClickHouseはデータを選択する際に複数のスレッドを使用します。各スレッドは別々のデータブロックを読み取って、それぞれ完了時に結果の行を独立して返します。その結果、出力のブロックの順序は入力の同じブロックの順序にはほとんどの場合一致しません。例：

``` sql
SELECT * FROM stripe_log_table
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

結果のソート（デフォルトで昇順）：

``` sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```
