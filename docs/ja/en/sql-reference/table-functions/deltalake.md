---
description: 'Amazon S3 内の Delta Lake テーブルに対する読み取り専用のテーブル形式インターフェイスを提供します。'
sidebar_label: 'deltaLake'
sidebar_position: 45
slug: /sql-reference/table-functions/deltalake
title: 'deltaLake'
doc_type: 'reference'
---

Amazon S3、Azure Blob Storage、またはローカルにマウントされたファイルシステム上の [Delta Lake](https://github.com/delta-io/delta) テーブルに対するテーブル形式インターフェイスを提供し、読み取りと書き込みの両方をサポートします (v25.10 以降) 。

<div id="syntax">
  ## 構文
</div>

`deltaLake` は `deltaLakeS3` の別名で、互換性維持のためにサポートされています。

```sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

deltaLakeLocal(path, [,format])
```

<div id="arguments">
  ## 引数
</div>

このテーブル関数の引数は、それぞれ `s3`、`azureBlobStorage`、`HDFS`、`file` テーブル関数の引数と同じです。
`format` 引数は、Delta Lake テーブル内のデータファイルのフォーマットを指定します。

オプションの `extra_credentials` パラメータを使用すると、ClickHouse Cloud でロールベースアクセス用の `role_arn` を渡せます。設定手順については [Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。

<div id="returned_value">
  ## 戻り値
</div>

指定された Delta Lake テーブルのデータを読み書きするための、指定された構造を持つテーブルを返します。

<div id="examples">
  ## 例
</div>

<div id="reading-data">
  ### データの読み取り
</div>

`https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/` にある S3 ストレージ上のテーブルを考えます。
ClickHouse でこのテーブルからデータを読み取るには、次を実行します。

```sql title="Query"
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

```response title="Response"
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

<div id="inserting-data">
  ### データの挿入
</div>

`S3` ストレージ上の `s3://ch-docs-s3-bucket/people_10k/` にあるテーブルを例に考えます。
Delta Lake への書き込みはベータ機能で、デフォルトでは無効です。次の設定で有効にしてください (`allow_delta_lake_writes` はバージョン 26.7 から利用できます。それ以前のバージョンでは `allow_experimental_delta_lake_writes` を使用してください) :

```sql title="Query"
SET allow_delta_lake_writes=1
```

次に、以下のように記述します。

```sql title="Query"
INSERT INTO TABLE FUNCTION deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>') VALUES (10001, 'John', 'Smith', 'Male', 30)
```

```response title="Response"
Query id: 09069b47-89fa-4660-9e42-3d8b1dde9b17

Ok.

1 row in set. Elapsed: 3.426 sec.
```

テーブルを再度読み取ることで、insert が成功したことを確認できます。

```sql title="Query"
SELECT *
FROM deltaLake('s3://ch-docs-s3-bucket/people_10k/', '<access_key>', '<secret>')
WHERE (firstname = 'John') AND (lastname = 'Smith')
```

```response title="Response"
Query id: 65032944-bed6-4d45-86b3-a71205a2b659

   ┌────id─┬─firstname─┬─lastname─┬─gender─┬─age─┐
1. │ 10001 │ John      │ Smith    │ Male   │  30 │
   └───────┴───────────┴──────────┴────────┴─────┘
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`. ファイルサイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`. 時刻が不明な場合、値は `NULL` です。
* `_etag` — ファイルの etag。型: `LowCardinality(String)`. etag が不明な場合、値は `NULL` です。

<div id="related">
  ## 関連
</div>

* [DeltaLake エンジン](/ja/engines/table-engines/integrations/deltalake.md)
* [DeltaLake クラスターテーブル関数](/ja/sql-reference/table-functions/deltalakeCluster.md)