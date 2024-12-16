---
sidebar_label: Parquet
sidebar_position: 3
slug: /ja/integrations/data-formats/parquet
---

# ClickHouseでのParquetの取り扱い

Parquetは、データを列指向で効率的に保存するためのファイル形式です。ClickHouseはParquetファイルの読み書きをサポートしています。

:::tip
クエリでファイルパスを参照する場合、ClickHouseがどの場所から読み取ろうとするかは、使用しているClickHouseのバリアントに依存します。

[`clickhouse-local`](/docs/ja/operations/utilities/clickhouse-local.md)を使用している場合、ClickHouse Localを起動した場所に相対的な位置から読み取ります。
ClickHouse Serverまたは`clickhouse client`を介してClickHouse Cloudを使用している場合、サーバー上の`/var/lib/clickhouse/user_files/`ディレクトリに相対的な位置から読み取ります。
:::

## Parquetからのインポート

データを読み込む前に、[file()](/docs/ja/sql-reference/functions/files.md/#file)関数を使用して[例のparquetファイル](assets/data.parquet)の構造を調べることができます。

```sql
DESCRIBE TABLE file('data.parquet', Parquet);
```

第2引数として[Parquet](/docs/ja/interfaces/formats.md/#data-format-parquet)を使用したので、ClickHouseはファイル形式を認識します。これによりカラムと型が表示されます。

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ path │ Nullable(String) │              │                    │         │                  │                │
│ date │ Nullable(String) │              │                    │         │                  │                │
│ hits │ Nullable(Int64)  │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

データを実際にインポートする前に、SQLの力を利用してファイルを調べることもできます。

```sql
SELECT *
FROM file('data.parquet', Parquet)
LIMIT 3;
```

```response
┌─path──────────────────────┬─date───────┬─hits─┐
│ Akiba_Hebrew_Academy      │ 2017-08-01 │  241 │
│ Aegithina_tiphia          │ 2018-02-01 │   34 │
│ 1971-72_Utah_Stars_season │ 2016-10-01 │    1 │
└───────────────────────────┴────────────┴──────┘
```

:::tip
`file()`および`INFILE`/`OUTFILE`のための明示的な形式設定を省略することができます。その場合、ClickHouseはファイル拡張子に基づいて形式を自動的に検出します。
:::

## 既存テーブルへのインポート

Parquetデータをインポートするためのテーブルを作成してみましょう。

```sql
CREATE TABLE sometable
(
    `path` String,
    `date` Date,
    `hits` UInt32
)
ENGINE = MergeTree
ORDER BY (date, path);
```

次に、`FROM INFILE`句を使用してデータをインポートできます。

```sql
INSERT INTO sometable
FROM INFILE 'data.parquet' FORMAT Parquet;

SELECT *
FROM sometable
LIMIT 5;
```

```response
┌─path──────────────────────────┬───────date─┬─hits─┐
│ 1988_in_philosophy            │ 2015-05-01 │   70 │
│ 2004_Green_Bay_Packers_season │ 2015-05-01 │  970 │
│ 24_hours_of_lemans            │ 2015-05-01 │   37 │
│ 25604_Karlin                  │ 2015-05-01 │   20 │
│ ASCII_ART                     │ 2015-05-01 │    9 │
└───────────────────────────────┴────────────┴──────┘
```

ClickHouseがParquetの文字列（`date`カラムで）を`Date`型に自動的に変換することに注目してください。これは、ClickHouseがターゲットテーブルの型に基づいて型キャストを自動的に行うためです。

## ローカルファイルをリモートサーバーに挿入

ローカルのParquetファイルをリモートのClickHouseサーバーに挿入したい場合は、以下のようにファイルの内容を`clickhouse-client`にパイプすることで行えます。

```sql
clickhouse client -q "INSERT INTO sometable FORMAT Parquet" < data.parquet
```

## Parquetファイルから新しいテーブルを作成

ClickHouseはParquetファイルのスキーマを読み取るため、即座にテーブルを作成することができます。

```sql
CREATE TABLE imported_from_parquet
ENGINE = MergeTree
ORDER BY tuple() AS
SELECT *
FROM file('data.parquet', Parquet)
```

これにより、指定されたParquetファイルから自動的にテーブルを作成および入力します。

```sql
DESCRIBE TABLE imported_from_parquet;
```

```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ path │ Nullable(String) │              │                    │         │                  │                │
│ date │ Nullable(String) │              │                    │         │                  │                │
│ hits │ Nullable(Int64)  │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

デフォルトでは、ClickHouseはカラム名、型、および値に厳密です。しかし、インポート中に存在しないカラムやサポートされていない値をスキップすることもできます。これらは[Parquet設定](/docs/ja/interfaces/formats.md/#parquet-format-settings)で管理できます。

## Parquet形式へのエクスポート

:::tip
`INTO OUTFILE`を使用してClickHouse Cloudでエクスポートする際には、ファイルが書き込まれるマシンで`clickhouse client`でコマンドを実行する必要があります。
:::

任意のテーブルやクエリ結果をParquetファイルにエクスポートするには、`INTO OUTFILE`句を使用できます。

```sql
SELECT *
FROM sometable
INTO OUTFILE 'export.parquet'
FORMAT Parquet
```

これにより、作業ディレクトリに`export.parquet`ファイルが作成されます。

## ClickHouseとParquetデータ型

ClickHouseとParquetのデータ型はほとんどが同じですが、[若干異なる部分](/docs/ja/interfaces/formats.md/#data-types-matching-parquet)もあります。例えば、ClickHouseは`DateTime`型をParquetの`int64`としてエクスポートします。これを再びClickHouseにインポートすると、次のように数値が表示されます（[time.parquetファイル](assets/time.parquet)）。

```sql
SELECT * FROM file('time.parquet', Parquet);
```

```response
┌─n─┬───────time─┐
│ 0 │ 1673622611 │
│ 1 │ 1673622610 │
│ 2 │ 1673622609 │
│ 3 │ 1673622608 │
│ 4 │ 1673622607 │
└───┴────────────┘
```

この場合、[型変換](/docs/ja/sql-reference/functions/type-conversion-functions.md)を使用することができます。

```sql
SELECT
    n,
    toDateTime(time)                 <--- int から時刻へ
FROM file('time.parquet', Parquet);
```

```response
┌─n─┬────toDateTime(time)─┐
│ 0 │ 2023-01-13 15:10:11 │
│ 1 │ 2023-01-13 15:10:10 │
│ 2 │ 2023-01-13 15:10:09 │
│ 3 │ 2023-01-13 15:10:08 │
│ 4 │ 2023-01-13 15:10:07 │
└───┴─────────────────────┘
```

## 詳細な情報

ClickHouseは、多くの形式、テキスト、バイナリをサポートしており、さまざまなシナリオやプラットフォームに対応しています。以下の記事で多様な形式とそれらの取り扱い方法を探ってみてください。

- [CSV and TSV形式](csv-tsv.md)
- [Avro, Arrow and ORC](arrow-avro-orc.md)
- [JSON形式](/docs/ja/integrations/data-ingestion/data-formats/json/intro.md)
- [Regexとテンプレート](templates-regex.md)
- [ネイティブとバイナリ形式](binary.md)
- [SQL形式](sql.md)

また、[clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)も確認してみてください。ClickHouseサーバーがなくてもローカル/リモートファイルで作業できるポータブルなフル機能のツールです。
