---
slug: /ja/sql-reference/table-functions/s3Cluster
sidebar_position: 181
sidebar_label: s3Cluster
title: "s3Cluster テーブル関数"
---
これは [s3](/docs/ja/sql-reference/table-functions/s3.md) テーブル関数の拡張です。

指定したクラスターの多くのノードから並列で [Amazon S3](https://aws.amazon.com/s3/) および [Google Cloud Storage](https://cloud.google.com/storage/) のファイルを処理できます。イニシエータでは、クラスター内のすべてのノードに接続を作成し、S3ファイルパス内のアスタリスクを展開し、各ファイルを動的にディスパッチします。ワーカーノードでは、次の処理タスクについてイニシエータに問い合わせ、それを処理します。このプロセスはすべてのタスクが終了するまで繰り返されます。

**構文**

``` sql
s3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,structure] [,compression_method])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

**引数**

- `cluster_name` — リモートおよびローカルサーバーへのアドレスセットと接続パラメータを構築するために使用されるクラスターの名前。
- `url` — ファイルまたは一連のファイルへのパス。読み取り専用モードで次のワイルドカードをサポート: `*`, `**`, `?`, `{'abc','def'}` および `{N..M}` ここで `N`, `M` — 数字, `abc`, `def` — 文字列。詳細については [パス内のワイルドカード](../../engines/table-engines/integrations/s3.md#wildcards-in-path) を参照してください。
- `NOSIGN` — このキーワードが資格情報の代わりに指定された場合、すべてのリクエストは署名されません。
- `access_key_id` および `secret_access_key` — 指定されたエンドポイントで使用するための資格情報を指定するキー。オプション。
- `session_token` - 指定されたキーと共に使用するセッショントークン。キーを渡す場合はオプション。
- `format` — ファイルの[形式](../../interfaces/formats.md#formats)。
- `structure` — テーブルの構造。形式 `'column1_name column1_type, column2_name column2_type, ...'`。
- `compression_method` — パラメータはオプションです。サポートされる値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮方法を自動検出します。

引数は [named collections](/docs/ja/operations/named-collections.md) を使用して渡すこともできます。この場合、`url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` は同じ方法で動作し、いくつかの追加パラメータがサポートされます：

 - `filename` — 指定されている場合、urlに追加されます。
 - `use_environment_credentials` — デフォルトで有効で、環境変数 `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED` を使用して追加パラメータを渡すことができます。
 - `no_sign_request` — デフォルトでは無効です。
 - `expiration_window_seconds` — デフォルト値は 120 です。

**返される値**

指定されたファイルにデータを読み書きするための、指定された構造のテーブル。

**例**

`cluster_simple` クラスター内のすべてのノードを使用して、`/root/data/clickhouse` および `/root/data/database/` フォルダー内のすべてのファイルからデータを選択:

``` sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'minio123',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

`cluster_simple` クラスター内のすべてのファイルの行の合計をカウント:

:::tip
ファイルリストに先頭にゼロが付いた数字の範囲が含まれる場合は、各桁ごとに波括弧を使用するか `?` を使用してください。
:::

本番ユースケースでは、[named collections](/docs/ja/operations/named-collections.md) の使用を推奨します。以下は例です:
``` sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'minio123';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

## プライベートおよびパブリックバケットへのアクセス

ユーザーは、s3関数のドキュメントに記載されている方法を使用できます [こちら](/docs/ja/sql-reference/table-functions/s3#accessing-public-buckets).

## パフォーマンスの最適化

s3関数のパフォーマンスを最適化する詳細については、[詳細ガイド](/docs/ja/integrations/s3/performance) を参照してください。


**参考**

- [S3エンジン](../../engines/table-engines/integrations/s3.md)
- [s3 テーブル関数](../../sql-reference/table-functions/s3.md)
