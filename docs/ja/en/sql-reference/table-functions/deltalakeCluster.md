---
description: 'deltaLake テーブル関数の拡張です。'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

これは [deltaLake](/ja/sql-reference/table-functions/deltalake.md) テーブル関数の拡張です。

指定したクラスター内の多数のノードから、Amazon S3 上の [Delta Lake](https://github.com/delta-io/delta) テーブルのファイルを並列に処理できます。イニシエーターはクラスター内のすべてのノードへの接続を確立し、各ファイルを動的に振り分けます。worker ノードは、処理する次のタスクをイニシエーターに問い合わせて処理します。これを、すべてのタスクが完了するまで繰り返します。

<div id="syntax">
  ## 構文
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster` は `deltaLakeCluster` の別名で、どちらも S3 向けです。

<div id="arguments">
  ## 引数
</div>

* `cluster_name` — リモートおよびローカルのserverへのアドレス群と接続parameterを構築するために使用されるクラスター名。
* その他すべての引数の説明は、対応する [deltaLake](/ja/sql-reference/table-functions/deltalake.md) テーブル関数 の引数の説明と同じです。
* オプションの `extra_credentials` parameter を使用して、ClickHouse Cloud でロールベースのアクセスに用いる `role_arn` を渡すことができます。設定手順については、[Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。

<div id="returned_value">
  ## 戻り値
</div>

S3 上の指定した Delta Lake テーブルについて、クラスターからデータを読み取るための、指定した構造のテーブル。

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

* [deltaLake エンジン](/ja/engines/table-engines/integrations/deltalake.md)
* [deltaLake テーブル関数](/ja/sql-reference/table-functions/deltalake.md)