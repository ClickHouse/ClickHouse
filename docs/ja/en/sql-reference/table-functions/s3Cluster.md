---
description: '指定したクラスター内の多数のノードを使って、Amazon S3 および Google Cloud Storage
  のファイルを並列に処理できるようにする、s3 テーブル関数の拡張です。'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'reference'
---

これは [s3](/ja/sql-reference/table-functions/s3.md) テーブル関数の拡張です。

指定したクラスター内の多数のノードを使って、[Amazon S3](https://aws.amazon.com/s3/) および Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) のファイルを並列に処理できます。イニシエーターでは、クラスター内のすべてのノードへの接続を確立し、S3 のファイルパス内のアスタリスクを展開して、各ファイルを動的に割り当てます。ワーカーノードでは、次に処理する task をイニシエーターに問い合わせて処理します。これを、すべての tasks が完了するまで繰り返します。

<div id="syntax">
  ## 構文
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## 引数
</div>

| Argument                                | Description                                                                                                                                                                                                                                  |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                          | リモートおよびローカルのサーバーへのアドレスセットと接続パラメーターの構築に使用されるクラスター名。                                                                                                                                                                                           |
| `url`                                   | ファイル、または複数のファイルへのパス。readonly モードでは、次のワイルドカードをサポートします: `*`, `**`, `?`, `{'abc','def'}` および `{N..M}`。ここで `N`, `M` は数値、`abc`, `def` は文字列です。詳細は [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path) を参照してください。 |
| `NOSIGN`                                | 認証情報の代わりにこの keyword を指定すると、すべてのリクエストに署名が行われません。                                                                                                                                                                                              |
| `access_key_id` and `secret_access_key` | 指定したエンドポイントで使用する認証情報を指定するキーです。省略可能です。                                                                                                                                                                                                        |
| `session_token`                         | 指定したキーとともに使用するセッショントークン。キーを渡す場合は省略可能です。                                                                                                                                                                                                      |
| `format`                                | ファイルの [フォーマット](/ja/sql-reference/formats) です。                                                                                                                                                                                                   |
| `structure`                             | テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                                                                                                                                                |
| `compression_method`                    | このパラメーターは省略可能です。サポートされる値: `none`, `gzip` または `gz`, `brotli` または `br`, `xz` または `LZMA`, `zstd` または `zst`。デフォルトでは、ファイル拡張子から圧縮方式を自動判定します。                                                                                                       |
| `headers`                               | このパラメーターは省略可能です。S3 リクエストにヘッダーを渡せます。形式 `headers(key=value)` で指定します。例: `headers('x-amz-request-payer' = 'requester')`。使用例は [here](/ja/sql-reference/table-functions/s3#accessing-requester-pays-buckets) を参照してください。                               |
| `extra_credentials`                     | 省略可能です。`roleARN` はこのパラメーターで渡せます。例は [here](/ja/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role) を参照してください。                                                                                                     |

引数は [名前付きコレクション](/ja/operations/named-collections.md) を使用して渡すこともできます。この場合、`url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` は同様に機能し、さらにいくつかの追加パラメーターがサポートされます:

| Argument                      | Description                                                                                                                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `filename`                    | 指定した場合は url に追加されます。                                                                                                                                                                 |
| `use_environment_credentials` | デフォルトで有効です。環境変数 `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED` を使用して追加パラメーターを渡せます。 |
| `no_sign_request`             | デフォルトでは無効です。                                                                                                                                                                         |
| `expiration_window_seconds`   | デフォルト値は 120 です。                                                                                                                                                                      |

<div id="returned_value">
  ## 戻り値
</div>

指定したファイル内のデータの読み書きに使用する、指定した構造のテーブル。

<div id="examples">
  ## 例
</div>

`cluster_simple` クラスターのすべてのノードを使用して、`/root/data/clickhouse` および `/root/data/database/` フォルダー内のすべてのファイルからデータを取得します。

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

クラスター `cluster_simple` 内のすべてのファイルに含まれる行の総数をカウントします。

:::tip
ファイル一覧に先頭が 0 の数値範囲が含まれている場合は、各桁ごとに波かっこを使った構文を使用するか、`?` を使用してください。
:::

本番環境での利用には、[名前付きコレクション](/ja/operations/named-collections.md)の使用を推奨します。以下に例を示します。

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## プライベートバケットおよびパブリックバケットへのアクセス
</div>

ユーザーは、s3 関数について[こちら](/ja/sql-reference/table-functions/s3#accessing-public-buckets)に記載されているものと同じ方法を使用できます。

<div id="optimizing-performance">
  ## パフォーマンスの最適化
</div>

`s3` 関数のパフォーマンス最適化について詳しくは、[詳細ガイド](/ja/integrations/s3/performance)を参照してください。

<div id="related">
  ## 関連
</div>

* [S3エンジン](../../engines/table-engines/integrations/s3.md)
* [S3テーブル関数](../../sql-reference/table-functions/s3.md)