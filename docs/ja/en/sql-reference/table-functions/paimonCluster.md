---
description: '指定したクラスター内の多数のノードから Apache Paimon のファイルを並列に処理できるようにする、paimon テーブル関数の拡張。'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # paimonCluster テーブル関数
</div>

<ExperimentalBadge />

これは [paimon](/ja/sql-reference/table-functions/paimon.md) テーブル関数の拡張です。

指定したクラスター内の多数のノードで、Apache [Paimon](https://paimon.apache.org/) のファイルを並列に処理できます。イニシエーターでは、クラスター内のすべてのノードへの接続を確立し、各ファイルを動的に振り分けます。ワーカーノードでは、次に処理するタスクをイニシエーターに問い合わせて処理します。これを、すべてのタスクが完了するまで繰り返します。

<div id="syntax">
  ## 構文
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## 引数
</div>

* `cluster_name` — リモートおよびローカルのサーバーへのアドレスのセットと接続パラメータの構築に使用されるクラスター名。
* そのほかのすべての引数の説明は、同等の [paimon](/ja/sql-reference/table-functions/paimon.md) テーブル関数における引数の説明と同一です。
* オプションの `extra_credentials` パラメータを使用すると、ClickHouse Cloud でロールベースのアクセス用の `role_arn` を渡すことができます。設定手順については、[Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。

**戻り値**

指定した Paimon table 内のクラスターからデータを読み取るための、指定した構造を持つテーブル。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`. ファイルサイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`. 時刻が不明な場合、値は `NULL` です。
* `_etag` — ファイルの etag。型: `LowCardinality(String)`. etag が不明な場合、値は `NULL` です。

**関連項目**

* [Paimon テーブル関数](/ja/sql-reference/table-functions/paimon.md)