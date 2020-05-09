---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 32
toc_title: "\u30B9\u30C8\u30EA\u30C3\u30D7\u30ED\u30B0"
---

# ストリップログ {#stripelog}

このエン ログエンジ [丸太エンジン家族](log-family.md) 記事。

少量のデータ（1万行未満）を含む多数のテーブルを作成する必要がある場合は、このエンジンをシナリオで使用します。

## テーブルの作成 {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

の詳細な説明を参照してください [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

## データの書き込み {#table_engines-stripelog-writing-the-data}

その `StripeLog` エンジンの店舗のすべての列を一つのファイルです。 それぞれの `INSERT` クエリ、ClickHouseは、列を一つずつ書き込み、テーブルファイルの最後にデータブロックを追加します。

各テーブルclickhouseに書き込み中のファイル:

-   `data.bin` — Data file.
-   `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

その `StripeLog` エンジンはサポートしません `ALTER UPDATE` と `ALTER DELETE` オペレーション

## データの読み込み {#table_engines-stripelog-reading-the-data}

ファイルをマークでclickhouseを並列化したデータです。 これは、 `SELECT` クエリは、予期しない順序で行を返します。 を使用 `ORDER BY` 行をソートする句。

## 使用例 {#table_engines-stripelog-example-of-use}

テーブルの作成:

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

データの挿入:

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

私たちは二つの `INSERT` データブロックを作成するためのクエリ `data.bin` ファイル。

ClickHouse利用は、複数のスレッド選択時のデータです。 各スレッドを読み込み、別のデータブロックを返しますよ列として自立で終了します。 結果として、出力の行のブロックの順序は、ほとんどの場合、入力の同じブロックの順序と一致しません。 例えば:

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

結果の並べ替え(デフォルトでは昇順):

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

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/stripelog/) <!--hide-->
