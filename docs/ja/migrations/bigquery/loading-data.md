---
sidebar_label: データのロード
title: BigQuery から ClickHouse へのデータのロード
slug: /ja/migrations/bigquery/loading-data
description: BigQuery から ClickHouse へのデータのロード方法
keywords: [移行, データ, etl, elt, bigquery]
---

_このガイドは、ClickHouse Cloud およびセルフマネージドの場合は ClickHouse v23.5+ に対応しています。_

このガイドでは、[BigQuery](https://cloud.google.com/bigquery) から ClickHouse へのデータ移行の方法を説明します。

まず、テーブルを [Google のオブジェクトストア（GCS）](https://cloud.google.com/storage) にエクスポートし、そのデータを [ClickHouse Cloud](https://clickhouse.com/cloud) にインポートします。この手順は、BigQuery から ClickHouse にエクスポートしたい各テーブルに対して繰り返す必要があります。

## ClickHouseへのデータエクスポートにはどのくらい時間がかかりますか？

BigQuery から ClickHouse へのデータエクスポートは、データセットのサイズに依存します。参考までに、このガイドを使用して [4TB の公開 Ethereum データセット](https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics) を BigQuery から ClickHouse にエクスポートするのに約1時間かかります。

| テーブル                                                                                                 | 行数          | エクスポートファイル数 | データサイズ | BigQuery エクスポート | スロット時間      | ClickHouse インポート |
| --------------------------------------------------------------------------------------------------- | ------------- | ----------------- | --------- | --------------- | --------------- | ----------------- |
| [blocks](https://github.com/ClickHouse/examples/blob/main/ethereum/schemas/blocks.md)             | 16,569,489    | 73             | 14.53GB   | 23 秒         | 37 分          | 15.4 秒         |
| [transactions](https://github.com/ClickHouse/examples/blob/main/ethereum/schemas/transactions.md) | 1,864,514,414 | 5169           | 957GB     | 1 分 38 秒    | 1 日 8 時間      | 18 分 5 秒    |
| [traces](https://github.com/ClickHouse/examples/blob/main/ethereum/schemas/traces.md)             | 6,325,819,306 | 17,985         | 2.896TB   | 5 分 46 秒    | 5 日 19 時間    | 34 分 55 秒   |
| [contracts](https://github.com/ClickHouse/examples/blob/main/ethereum/schemas/contracts.md)       | 57,225,837    | 350            | 45.35GB   | 16 秒          | 1 時間 51 分     | 39.4 秒         |
| 合計                                                                                                 | 8.26 billion  | 23,577         | 3.982TB   | 8 分 3 秒     | \> 6 日 5 時間 | 53 分 45 秒   |

## 1. GCS へのテーブルデータのエクスポート

このステップでは、[BigQuery SQL ワークスペース](https://cloud.google.com/bigquery/docs/bigquery-web-ui) を利用して SQL コマンドを実行します。以下では、`EXPORT DATA` ステートメントを使用して、`mytable` という BigQuery テーブルを GCS バケットにエクスポートします。

```sql
DECLARE export_path STRING;
DECLARE n INT64;
DECLARE i INT64;
SET i = 0;

-- n を x billion 行に設定することを推奨します。例えば、5 billion 行なら n = 5
SET n = 100;

WHILE i < n DO
  SET export_path = CONCAT('gs://mybucket/mytable/', i,'-*.parquet');
  EXPORT DATA
    OPTIONS (
      uri = export_path,
      format = 'PARQUET',
      overwrite = true
    )
  AS (
    SELECT * FROM mytable WHERE export_id = i
  );
  SET i = i + 1;
END WHILE;
```

上記のクエリでは、BigQuery テーブルを [Parquet データフォーマット](https://parquet.apache.org/) にエクスポートしています。また、`uri` パラメータに `*` 文字を使用しています。これにより、エクスポートが 1GB を超える場合、出力が複数のファイルに分散され、数値が増加するサフィックスが付与されます。

このアプローチには以下の利点があります：

- Google は 1 日あたり最大 50TB を無料で GCS にエクスポートできます。ユーザは GCS ストレージの料金のみを支払います。
- エクスポートは複数のファイルを自動的に生成し、それぞれ最大 1GB のテーブルデータに制限されます。これにより、Import 時に並列化が可能になるので ClickHouse にとって有益です。
- Parquet は列指向フォーマットのため、圧縮されており、BigQuery がより速くエクスポートし、ClickHouse がより速くクエリ できるため、より良い交換フォーマットになります。

## 2. GCS から ClickHouse へのデータインポート

エクスポートが完了したら、このデータを ClickHouse テーブルにインポートすることができます。以下のコマンドを実行するには、[ClickHouse SQL コンソール](/docs/ja/integrations/sql-clients/sql-console) または [`clickhouse-client`](/docs/ja/integrations/sql-clients/cli) を使用してください。

最初に、ClickHouse にテーブルを[作成](/docs/ja/sql-reference/statements/create/table)する必要があります：

```sql
-- BigQuery テーブルに STRUCT 型のカラムが含まれている場合、
-- そのカラムを ClickHouse の Nested 型のカラムにマップする設定を有効にする必要があります。
SET input_format_parquet_import_nested = 1;

CREATE TABLE default.mytable
(
	`timestamp` DateTime64(6),
	`some_text` String
)
ENGINE = MergeTree
ORDER BY (timestamp);
```

テーブルを作成したら、エクスポートを加速するために、クラスター内に複数の ClickHouse レプリカがある場合は、設定 `parallel_distributed_insert_select` を有効にします。ClickHouse ノードが1つしかない場合は、この手順をスキップできます：

```sql
SET parallel_distributed_insert_select = 1;
```

最後に、`SELECT` クエリの結果に基づいてテーブルにデータを挿入する [`INSERT INTO SELECT` コマンド](/docs/ja/sql-reference/statements/insert-into#inserting-the-results-of-select) を使用して、GCS から ClickHouse テーブルにデータを挿入できます。

データを `INSERT` するためには、[s3Cluster 関数](/docs/ja/sql-reference/table-functions/s3Cluster) を使用して GCS バケットからデータを取得します。GCS は [Amazon S3](https://aws.amazon.com/s3/) と互換性があります。ClickHouse ノードが1つしかない場合は、`s3Cluster` 関数の代わりに [s3 テーブル関数](/ja/sql-reference/table-functions/s3) を使うことができます。

```sql
INSERT INTO mytable
SELECT
    timestamp,
    ifNull(some_text, '') as some_text
FROM s3Cluster(
    'default',
    'https://storage.googleapis.com/mybucket/mytable/*.parquet.gz',
    '<ACCESS_ID>',
    '<SECRET>'
);
```

上記のクエリで使用されている `ACCESS_ID` と `SECRET` は、GCS バケットに関連付けられた [HMAC キー](https://cloud.google.com/storage/docs/authentication/hmackeys) です。

:::note NULL 可能なカラムをエクスポートする場合は `ifNull` を使用
上記のクエリでは、[`ifNull` 関数](/docs/ja/sql-reference/functions/functions-for-nulls#ifnull) を使用して `some_text` カラムのデフォルト値を指定して ClickHouse テーブルにデータを挿入しています。ClickHouse のカラムを [`Nullable`](/docs/ja/sql-reference/data-types/nullable) にすることもできますが、パフォーマンスに悪影響を及ぼす可能性があるため推奨されません。

別の方法として、`SET input_format_null_as_default=1` を設定すると、欠落している値や NULL 値がそれぞれのカラムのデフォルト値に置き換えられます（そのデフォルトが指定されている場合）。
:::

## 3. データエクスポートの成功確認

データが正しく挿入されたかどうかを確認するには、新しいテーブルで `SELECT` クエリを実行してください：

```sql
SELECT * FROM mytable limit 10;
```

他の BigQuery テーブルをエクスポートするには、上記の手順を追加の各テーブルに対して繰り返してください。

## さらに詳しい情報とサポート

このガイドに加えて、[ClickHouse を使用して BigQuery を高速化し増分インポートを扱う方法](https://clickhouse.com/blog/clickhouse-bigquery-migrating-data-for-realtime-queries)を示すブログ記事もお勧めです。

BigQuery から ClickHouse へのデータ転送に問題がある場合は、support@clickhouse.com までお問い合わせください。
