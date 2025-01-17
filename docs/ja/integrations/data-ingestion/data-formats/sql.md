---
sidebar_label: SQL ダンプ
slug: /ja/integrations/data-formats/sql
---

# ClickHouseでのSQLデータの挿入とダンプ

ClickHouseは、さまざまな方法でOLTPデータベースインフラストラクチャに簡単に統合することができます。その一つの方法は、他のデータベースとClickHouse間でSQLダンプを利用してデータを転送することです。

## SQLダンプの作成

データは[SQLInsert](/docs/ja/interfaces/formats.md/#sqlinsert)を使用してSQL形式でダンプできます。ClickHouseはデータを`INSERT INTO <table name> VALUES(...)`の形式で書き込み、`output_format_sql_insert_table_name`設定オプションをテーブル名として使用します。

```sql
SET output_format_sql_insert_table_name = 'some_table';
SELECT * FROM some_data
INTO OUTFILE 'dump.sql'
FORMAT SQLInsert
```

カラム名は[`output_format_sql_insert_include_column_names`](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names)オプションを無効にすることで省略できます。

```sql
SET output_format_sql_insert_include_column_names = 0
```

今度は[dump.sql](assets/dump.sql)ファイルを他のOLTPデータベースに供給できます。

```bash
mysql some_db < dump.sql
```

ここでは、`some_db` MySQLデータベースに`some_table`テーブルが存在すると仮定しています。

一部のDBMSは、1回のバッチで処理できる値の量に制限がある場合があります。デフォルトではClickHouseは65kの値のバッチを作成しますが、[`output_format_sql_insert_max_batch_size`](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size)オプションでこれを変更できます。

```sql
SET output_format_sql_insert_max_batch_size = 1000;
```

### 一連の値のエクスポート

ClickHouseには、`INSERT INTO table VALUES`部分を省略し、一連の値のみを返す[Values](/docs/ja/interfaces/formats.md/#data-format-values)形式があります。

```sql
SELECT * FROM some_data LIMIT 3 FORMAT Values
```
```response
('Bangor_City_Forest','2015-07-01',34),('Alireza_Afzal','2017-02-01',24),('Akhaura-Laksam-Chittagong_Line','2015-09-01',30)
```

## SQLダンプからのデータの挿入

SQLダンプを読み取るためには、[MySQLDump](/docs/ja/interfaces/formats.md/#mysqldump)を使用します。

```sql
SELECT *
FROM file('dump.sql', MySQLDump)
LIMIT 5
```
```response
┌─path───────────────────────────┬──────month─┬─hits─┐
│ Bangor_City_Forest             │ 2015-07-01 │   34 │
│ Alireza_Afzal                  │ 2017-02-01 │   24 │
│ Akhaura-Laksam-Chittagong_Line │ 2015-09-01 │   30 │
│ 1973_National_500              │ 2017-10-01 │   80 │
│ Attachment                     │ 2017-09-01 │ 1356 │
└────────────────────────────────┴────────────┴──────┘
```

デフォルトでClickHouseは未知のカラムをスキップします（これは[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)オプションによって制御されます）し、ダンプに複数のテーブルが含まれる場合、最初に見つかったテーブルのデータを処理します。DDL文はスキップされます。MySQLダンプからテーブルにデータを読み込むためには（例： [mysql.sql](assets/mysql.sql) ファイル）以下のようにします。

```sql
INSERT INTO some_data
FROM INFILE 'mysql.sql' FORMAT MySQLDump
```

また、MySQLダンプファイルから自動的にテーブルを作成することもできます。

```sql
CREATE TABLE table_from_mysql
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT *
FROM file('mysql.sql', MySQLDump)
```

ここでは、ClickHouseが自動的に推測した構造に基づいて`table_from_mysql`という名前のテーブルを作成しました。ClickHouseはデータに基づいてタイプを検出するか、利用可能な場合はDDLを使用します。

```sql
DESCRIBE TABLE table_from_mysql;
```
```response
┌─name──┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ path  │ Nullable(String) │              │                    │         │                  │                │
│ month │ Nullable(Date32) │              │                    │         │                  │                │
│ hits  │ Nullable(UInt32) │              │                    │         │                  │                │
└───────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## その他のフォーマット

ClickHouseは、さまざまなシナリオやプラットフォームに対応するために、多くのフォーマットをテキストとバイナリの両方でサポートしています。以下の記事でさらに多くのフォーマットやその扱い方について学んでください。

- [CSVとTSVフォーマット](csv-tsv.md)
- [Parquet](parquet.md)
- [JSONフォーマット](/docs/ja/integrations/data-ingestion/data-formats/json/intro.md)
- [正規表現とテンプレート](templates-regex.md)
- [ネイティブとバイナリフォーマット](binary.md)
- **SQLフォーマット**

また、[clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)も確認してください。これは、ClickHouseサーバーを使わずにローカル/リモートファイル上でSQLで作業するためのポータブルなフル機能のツールです。
