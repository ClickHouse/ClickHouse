---
sidebar_label: Snowflake
sidebar_position: 20
slug: /ja/migrations/snowflake
description: SnowflakeからClickHouseへの移行
keywords: [migrate, migration, migrating, data, etl, elt, snowflake]
---

# SnowflakeからClickHouseへの移行

このガイドでは、SnowflakeからClickHouseへのデータ移行方法を紹介します。

SnowflakeとClickHouse間でのデータ移行には、転送用の中間ストレージとしてS3などのオブジェクトストアを使用する必要があります。この移行プロセスでは、Snowflakeの`COPY INTO`コマンドとClickHouseの`INSERT INTO SELECT`コマンドを使用します。

## 1. Snowflakeからのデータエクスポート

<img src={require('./images/migrate_snowflake_clickhouse.png').default} class="image" alt="Migrating from Snowflake to ClickHouse" style={{width: '600px', marginBottom: '20px', textAlign: 'left'}}/>

上記の図に示されているように、Snowflakeからデータをエクスポートするには外部ステージを使用する必要があります。

次のようなスキーマを持つSnowflakeテーブルをエクスポートする場合を考えてみましょう。

```sql
CREATE TABLE MYDATASET (
   timestamp TIMESTAMP,
   some_text varchar,
   some_file OBJECT,
   complex_data VARIANT,
) DATA_RETENTION_TIME_IN_DAYS = 0;
```

このテーブルのデータをClickHouseデータベースに移動するには、まずこのデータを外部ステージにコピーする必要があります。データのコピー時には、タイプ情報の共有、精度の保持、効率的な圧縮、分析で一般的なネスト構造のネイティブサポートが可能なParquet形式を推奨します。

以下の例では、Parquetと希望するファイルオプションを表す名前付きファイルフォーマットをSnowflakeで作成します。そしてどのバケットにデータセットをコピーするかを指定し、最後にデータセットをバケットにコピーします。

```sql
CREATE FILE FORMAT my_parquet_format TYPE = parquet;

-- S3バケットへのコピーを指定する外部ステージを作成します
CREATE OR REPLACE STAGE external_stage 
URL='s3://mybucket/mydataset'
CREDENTIALS=(AWS_KEY_ID='<key>' AWS_SECRET_KEY='<secret>')
FILE_FORMAT = my_parquet_format;

-- すべてのファイルに"mydataset"プレフィックスを付け、ファイルの最大サイズを150MBに指定します。
-- `header=true`パラメータはカラム名を取得するために必要です
COPY INTO @external_stage/mydataset from mydataset max_file_size=157286400 header=true;
```

約5TBのデータセットで最大ファイルサイズ150MB、同じAWS `us-east-1`地域にある2X-Large Snowflake warehouseを使用した場合、S3バケットへのデータコピーは約30分かかります。

## 2. ClickHouseへのインポート

データが中間のオブジェクトストレージにステージングされたら、ClickHouseの[s3 テーブル関数](/docs/ja/sql-reference/table-functions/s3)などの機能を使用して、データをテーブルに挿入できます。

以下の例では、AWS S3用の[s3 テーブル関数](/docs/ja/sql-reference/table-functions/s3)を使用していますが、Google Cloud Storageには[gcs テーブル関数](/docs/ja/sql-reference/table-functions/gcs)、Azure Blob Storageには[azureBlobStorage テーブル関数](/docs/ja/sql-reference/table-functions/azureBlobStorage)を使用できます。

次のテーブルのターゲットスキーマを想定しています：

```sql
CREATE TABLE default.mydataset
(
	`timestamp` DateTime64(6),
	`some_text` String,
	`some_file` Tuple(filename String, version String),
	`complex_data` Tuple(name String, description String),
)
ENGINE = MergeTree
ORDER BY (timestamp)
```

この場合、S3からClickHouseテーブルにデータを挿入するために`INSERT INTO SELECT`コマンドを使用できます：

```sql
INSERT INTO mydataset
SELECT
	timestamp,
	some_text,
	JSONExtract(
		ifNull(some_file, '{}'),
		'Tuple(filename String, version String)'
	) AS some_file,
	JSONExtract(
		ifNull(complex_data, '{}'),
		'Tuple(filename String, description String)'
	) AS complex_data,
FROM s3('https://mybucket.s3.amazonaws.com/mydataset/mydataset*.parquet')
SETTINGS input_format_null_as_default = 1, -- 値がnullの場合にデフォルトとしてカラムを挿入する
input_format_parquet_case_insensitive_column_matching = 1 -- ソースデータとターゲットテーブル間のカラムマッチングが大文字小文字を区別しない
```

:::note ネスト構造のカラムについての注意
オリジナルのSnowflakeテーブルスキーマにおける`VARIANT`および`OBJECT`カラムは、デフォルトでJSON文字列として出力され、ClickHouseに挿入する際にキャストする必要があります。

`some_file`のようなネスト構造は、Snowflakeによるコピー時にJSON文字列に変換されます。ClickHouseにインポートする際には、これらの構造を[JSONExtract関数](/docs/ja/sql-reference/functions/json-functions#jsonextractjson-indices_or_keys-return_type)を使用してClickHouse挿入時にTuplesに変換する必要があります。
:::

## 3. 正しいデータエクスポートのテスト

データが正しく挿入されたかどうかをテストするには、単に新しいテーブルに対して`SELECT`クエリを実行します：

```sql
SELECT * FROM mydataset limit 10;
```

## さらなる読み物とサポート

このガイドに加えて、[SnowflakeとClickHouseの比較](https://clickhouse.com/blog/clickhouse-vs-snowflake-for-real-time-analytics-comparison-migration-guide)に関するブログ記事を読むことをお勧めします。

SnowflakeからClickHouseへのデータ転送に問題がある場合は、support@clickhouse.comまでお気軽にお問い合わせください。
