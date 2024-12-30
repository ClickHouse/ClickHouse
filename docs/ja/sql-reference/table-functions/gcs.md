---
slug: /ja/sql-reference/table-functions/gcs
sidebar_position: 70
sidebar_label: gcs
keywords: [gcs, bucket]
---

# gcs テーブル関数

[Google Cloud Storage](https://cloud.google.com/storage/) からデータを `SELECT` および `INSERT` するためのテーブルライクなインターフェースを提供します。[`Storage Object User` IAM ロール](https://cloud.google.com/storage/docs/access-control/iam-roles) が必要です。

これは [s3 テーブル関数](../../sql-reference/table-functions/s3.md) の別名です。

クラスターに複数のレプリカがある場合、[s3Cluster 関数](../../sql-reference/table-functions/s3Cluster.md)（GCS と共に動作します）を使用して挿入を並列化できます。

**構文**

``` sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method])
gcs(named_collection[, option=value [,..]])
```

:::tip GCS
GCS テーブル関数は、GCS XML API と HMAC キーを使用して Google Cloud Storage に統合します。エンドポイントと HMAC の詳細については、[Google の相互運用性ドキュメント](https://cloud.google.com/storage/docs/interoperability)を参照してください。

:::

**パラメータ**

- `url` — ファイルへのバケットパス。読み取り専用モードで以下のワイルドカードをサポートします：`*`, `**`, `?`, `{abc, def}` と `{N..M}` （`N`, `M` — 数字, `'abc'`, `'def'` — 文字列）。
  :::note GCS
  GCS パスは、Google XML API のエンドポイントが JSON API と異なるため、この形式です：
  ```
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
  ```
  であり、~~https://storage.cloud.google.com~~ ではありません。
  :::
- `NOSIGN` — 資格情報の代わりにこのキーワードが提供された場合、すべてのリクエストは署名されません。
- `hmac_key` と `hmac_secret` — 指定されたエンドポイントで使用する資格情報を指定するキー。オプション。
- `format` — ファイルの[フォーマット](../../interfaces/formats.md#formats)。
- `structure` — テーブルの構造。形式 `'column1_name column1_type, column2_name column2_type, ...'`。
- `compression_method` — オプションのパラメータ。サポートされる値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮方法を自動検出します。

引数は[名前付きコレクション](/docs/ja/operations/named-collections.md)を使用して渡すこともできます。この場合、`url`、`format`、`structure`、`compression_method` は同様に機能し、いくつかの追加パラメータがサポートされます：

 - `access_key_id` — `hmac_key`, オプション。
 - `secret_access_key` — `hmac_secret`, オプション。
 - `filename` — 指定された場合、URLに追加されます。
 - `use_environment_credentials` — デフォルトで有効になっており、環境変数 `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`、`AWS_CONTAINER_CREDENTIALS_FULL_URI`、`AWS_CONTAINER_AUTHORIZATION_TOKEN`、`AWS_EC2_METADATA_DISABLED` を使用して追加のパラメータを渡すことができます。
 - `no_sign_request` — デフォルトでは無効。
 - `expiration_window_seconds` — デフォルト値は 120。

**返される値**

指定されたファイルでデータを読み書きするための特定の構造を持つテーブル。

**例**

GCS ファイル `https://storage.googleapis.com/my-test-bucket-768/data.csv` から最初の2行を選択する例：

``` sql
SELECT *
FROM gcs('https://storage.googleapis.com/my-test-bucket-768/data.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

`gzip` 圧縮方法を使用した同様の例：

``` sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## 使用方法

GCS上の以下のURIを持つ複数のファイルがあるとします：

-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_1.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_2.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_3.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/some_prefix/some_file_4.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_1.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_2.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_3.csv'
-   'https://storage.googleapis.com/my-test-bucket-768/another_prefix/some_file_4.csv'

番号が1から3までのファイル内の行数をカウントします：

``` sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'name String, value UInt32')
```

``` text
┌─count()─┐
│      18 │
└─────────┘
```

これら2つのディレクトリ内のすべてのファイルの総行数をカウントします：

``` sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'name String, value UInt32')
```

``` text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
先頭にゼロを含む数字の範囲を含むファイルリストを持っている場合は、個別の数字用にブレースを使用するか、`?` を使用してください。
:::

ファイル名が `file-000.csv`、`file-001.csv`、...、`file-999.csv` のファイル内の総行数をカウントします：

``` sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

``` text
┌─count()─┐
│      12 │
└─────────┘
```

ファイル `test-data.csv.gz` にデータを挿入：

``` sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

既存のテーブルからファイル `test-data.csv.gz` にデータを挿入：

``` sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

グロブ ** は再帰的なディレクトリトラバーサルに使用できます。以下の例では、`my-test-bucket-768` ディレクトリ内のすべてのファイルを再帰的に取得します：

``` sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

以下は `my-test-bucket` ディレクトリ内の任意のフォルダーから `test-data.csv.gz` ファイルを再帰的に取得する例です：

``` sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

実用化のケースでは [名前付きコレクション](/docs/ja/operations/named-collections.md) を使用することをお勧めします。以下がその例です：
``` sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

## パーティション書き込み

データを `GCS` テーブルに挿入する際に `PARTITION BY` 式を指定すると、各パーティション値に対して個別のファイルが作成されます。データを個別のファイルに分割することで、読み取り操作の効率を向上させます。

**例**

1. キーにパーティションIDを使用して個別のファイルを作成：

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```
この結果、データは `file_x.csv`、`file_y.csv`、`file_z.csv` の3つのファイルに書き込まれます。

2. パーティションIDをバケット名に使用して、異なるバケットにファイルを作成：

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```
この結果、データは異なるバケットにある3つのファイルに書き込まれます：`my_bucket_1/file.csv`、`my_bucket_10/file.csv`、および `my_bucket_20/file.csv`。

**関連項目**

-   [S3テーブル関数](s3.md)
-   [S3エンジン](../../engines/table-engines/integrations/s3.md)
