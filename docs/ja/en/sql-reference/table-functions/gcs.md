---
description: '`SELECT` と `INSERT` で Google
  Cloud Storage のデータを扱うためのテーブル形式インターフェイスを提供します。`Storage Object User` IAM ロールが必要です。'
keywords: ['gcs', 'バケット']
sidebar_label: 'gcs'
sidebar_position: 70
slug: /sql-reference/table-functions/gcs
title: 'gcs'
doc_type: 'reference'
---

[Google Cloud Storage](https://cloud.google.com/storage/) のデータを `SELECT` および `INSERT` するためのテーブル形式インターフェイスを提供します。[`Storage Object User` IAM ロール](https://cloud.google.com/storage/docs/access-control/iam-roles)が必要です。

これは [S3 テーブル関数](../../sql-reference/table-functions/s3.md) のエイリアスです。

クラスターに複数のレプリカがある場合は、`INSERT` を並列化するために、代わりに [s3Cluster function](../../sql-reference/table-functions/s3Cluster.md) (GCS でも動作します) を使用できます。

<div id="syntax">
  ## 構文
</div>

```sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method])
gcs(named_collection[, option=value [,..]])
```

:::tip GCS
GCS Table Function は、GCS XML API と HMAC キーを使用して Google Cloud Storage と連携します。
エンドポイントと HMAC の詳細については、[Google の相互運用性に関するドキュメント](https://cloud.google.com/storage/docs/interoperability)を参照してください。
:::

<div id="arguments">
  ## 引数
</div>

| 引数                           | 説明                                                                                                                                         |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `url`                        | ファイルへの バケット パスです。readonly モードでは、次の wildcards をサポートしています: `*`, `**`, `?`, `{abc,def}`、`{N..M}`。ここで、`N`、`M` は数値、`'abc'`、`'def'` は文字列です。    |
| `NOSIGN`                     | credentials の代わりにこの keyword を指定すると、すべてのリクエストに署名が行われません。                                                                                    |
| `hmac_key` and `hmac_secret` | 指定した endpoint で使用する credentials を指定するキーです。省略可能です。                                                                                          |
| `format`                     | ファイルの[フォーマット](/ja/sql-reference/formats)です。                                                                                                   |
| `structure`                  | テーブルの構造です。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                                            |
| `compression_method`         | この parameter は省略可能です。サポートされる値は `none`、`gzip` または `gz`、`brotli` または `br`、`xz` または `LZMA`、`zstd` または `zst` です。デフォルトでは、ファイル拡張子から圧縮方式を自動判別します。 |

:::note GCS
Google XML API の endpoint は JSON API とは異なるため、GCS パスは次の形式になります:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

また、~~https://storage.cloud.google.com~~ ではありません。
:::

引数は、[名前付きコレクション](/ja/operations/named-collections.md)を使って渡すこともできます。この場合、`url`、`format`、`structure`、`compression_method` は同様に機能し、さらにいくつかの追加パラメーターがサポートされます。

| Parameter                     | Description                                                                                                                                                                        |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `access_key_id`               | `hmac_key`。省略可能です。                                                                                                                                                                 |
| `secret_access_key`           | `hmac_secret`。省略可能です。                                                                                                                                                              |
| `filename`                    | 指定した場合、URL に追加されます。                                                                                                                                                                |
| `use_environment_credentials` | デフォルトで有効です。環境変数 `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`、`AWS_CONTAINER_CREDENTIALS_FULL_URI`、`AWS_CONTAINER_AUTHORIZATION_TOKEN`、`AWS_EC2_METADATA_DISABLED` を使用して追加のパラメーターを渡せます。 |
| `no_sign_request`             | デフォルトでは無効です。                                                                                                                                                                       |
| `expiration_window_seconds`   | デフォルト値は 120 です。                                                                                                                                                                    |

<div id="returned_value">
  ## 戻り値
</div>

指定したファイル内のデータの読み書きに使用する、指定した構造のテーブル。

<div id="examples">
  ## 例
</div>

GCS ファイル `https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz` から先頭 2 行を選択します。圧縮方式は `.gz` というファイル拡張子から自動的に判別されます：

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

上記と同じクエリですが、自動検出に頼る代わりに、`gzip` の圧縮方式を明示的に指定しています。

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="usage">
  ## 使用例
</div>

GCS 上に、次の URI を持つ複数のファイルがあるとします。

* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;4.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;4.csv&#39;

末尾が 1 ～ 3 の数字で終わるファイルに含まれる行数を数えます。

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

これら2つのディレクトリ内にあるすべてのファイルの行数の合計を数えます:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
ファイル一覧に先頭ゼロ付きの数値範囲が含まれている場合は、各桁ごとに波かっこを使った構文を使用するか、`?` を使用してください。
:::

`file-000.csv`、`file-001.csv`、...、`file-999.csv` という名前のファイル内の行数の合計を数えます:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

`test-data.csv.gz` ファイルにデータを挿入します:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

既存のテーブルからファイル `test-data.csv.gz` にデータを挿入します:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

`**` のglobは、ディレクトリを再帰的に走査する際に使用できます。以下の例では、`my-test-bucket-768` ディレクトリ配下のすべてのファイルを再帰的に取得します。

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

以下では、`my-test-bucket` ディレクトリ内の任意のフォルダにある、すべての `test-data.csv.gz` ファイルから再帰的にデータを取得します：

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

本番環境では、[名前付きコレクション](/ja/operations/named-collections.md) の使用を推奨します。例を以下に示します。

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

<div id="partitioned-write">
  ## パーティション別書き込み
</div>

`GCS` テーブルにデータを挿入する際に `PARTITION BY` 式を指定すると、パーティション値ごとに個別のファイルが作成されます。データを個別のファイルに分割することで、読み取り操作の効率向上に役立ちます。

**例**

1. キーに パーティション ID を使用すると、個別のファイルが作成されます。

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```

その結果、データは `file_x.csv`、`file_y.csv`、`file_z.csv` の 3 つのファイルに書き込まれます。

2. バケット名にパーティション ID を含めると、異なるバケットにファイルが作成されます。

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```

その結果、データは異なる3つのバケット内にある `my_bucket_1/file.csv`、`my_bucket_10/file.csv`、`my_bucket_20/file.csv` の3つのファイルに書き込まれます。

<div id="related">
  ## 関連項目
</div>

* [S3 テーブル関数](s3.md)
* [S3 エンジン](../../engines/table-engines/integrations/s3.md)