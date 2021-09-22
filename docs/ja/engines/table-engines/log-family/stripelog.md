---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "\u30B9\u30C8\u30EA\u30C3\u30D7\u30ED\u30B0"
---

# ストリップログ {#stripelog}

このエンジンはログエンジンの系列に属します。 ログエンジンの共通のプロパティとその違いを参照してください [ログエンジン家族](index.md) 記事だ

少量のデータ(1万行未満)で多数のテーブルを記述する必要がある場合に、このエンジンを使用します。

## テーブルの作成 {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

の詳細な説明を見て下さい [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

## データの書き込み {#table_engines-stripelog-writing-the-data}

その `StripeLog` engineはすべての列を一つのファイルに格納します。 それぞれのため `INSERT` query,ClickHouseは、データブロックをテーブルファイルの最後に追加し、列を一つずつ書き込みます。

各テーブルClickHouseに書き込み中のファイル:

-   `data.bin` — Data file.
-   `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

その `StripeLog` エンジンはサポートしません `ALTER UPDATE` と `ALTER DELETE` 作戦だ

## データの読み取り {#table_engines-stripelog-reading-the-data}

ファイルをマークでClickHouseを並列化したデータです。 これは、 `SELECT` queryは、予測不可能な順序で行を返します。 使用する `ORDER BY` 行をソートする句。

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

我々は二つを使用 `INSERT` データブロックを作成するためのクエリ `data.bin` ファイル

ClickHouse利用は、複数のスレッド選択時のデータです。 各スレッドは、個別のデータブロックを読み取り、終了時に結果の行を個別に返します その結果、出力内の行のブロックの順序は、ほとんどの場合、入力内の同じブロックの順序と一致しません。 例えば:

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

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/stripelog/) <!--hide-->
