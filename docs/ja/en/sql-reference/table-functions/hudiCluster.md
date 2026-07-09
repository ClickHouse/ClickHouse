---
description: 'hudi テーブル関数の拡張です。指定したクラスター内の多数のノードで、
  Amazon S3 上の Apache Hudi テーブルのファイルを並列に処理できます。'
sidebar_label: 'hudiCluster'
sidebar_position: 86
slug: /sql-reference/table-functions/hudiCluster
title: 'hudiCluster テーブル関数'
doc_type: 'reference'
---

これは [hudi](/ja/sql-reference/table-functions/hudi.md) テーブル関数の拡張です。

指定したクラスター内の多数のノードで、Amazon S3 上の Apache [Hudi](https://hudi.apache.org/) テーブルのファイルを並列に処理できます。イニシエーターでは、クラスター内のすべてのノードへの接続を確立し、各ファイルを動的に振り分けます。ワーカーノードでは、次に処理するタスクをイニシエーターに問い合わせて処理します。これを、すべてのタスクが完了するまで繰り返します。

<div id="syntax">
  ## 構文
</div>

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## 引数
</div>

| 引数                                           | 説明                                                                                                                                                                                                                                                                     |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                               | リモートおよびローカルのサーバーへのアドレスと接続パラメーターのセットを構築するために使用されるクラスター名。                                                                                                                                                                                                                |
| `url`                                        | S3 内の既存の Hudi テーブルへのパスを含むバケットURL。                                                                                                                                                                                                                                      |
| `aws_access_key_id`, `aws_secret_access_key` | [AWS](https://aws.amazon.com/) アカウントのユーザー用の長期認証情報です。これらを使用してリクエストを認証できます。これらのパラメーターは省略可能です。認証情報が指定されていない場合は、ClickHouse の設定にあるものが使用されます。詳細については、[Using S3 for Data Storage](/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) を参照してください。 |
| `format`                                     | ファイルの[フォーマット](/ja/interfaces/formats)。                                                                                                                                                                                                                                    |
| `structure`                                  | テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                                                                                                                                                                          |
| `compression`                                | このパラメーターは省略可能です。サポートされる値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、圧縮はファイル拡張子から自動判別されます。                                                                                                                                                          |
| `extra_credentials`                          | このパラメーターは省略可能です。ClickHouse Cloud でロールベースアクセス用の `role_arn` を渡すために使用します。設定手順については [Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。                                                                                                                                  |

<div id="returned_value">
  ## 戻り値
</div>

S3 内の指定した Hudi テーブルについて、クラスターからデータを読み取るための、指定した構造を持つテーブル。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`。ファイルサイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。
* `_etag` — ファイルの etag。型: `LowCardinality(String)`。etag が不明な場合、値は `NULL` です。

<div id="related">
  ## 関連
</div>

* [Hudiエンジン](/ja/engines/table-engines/integrations/hudi.md)
* [Hudiテーブル関数](/ja/sql-reference/table-functions/hudi.md)