---
slug: /ja/sql-reference/table-functions/deltalake
sidebar_position: 45
sidebar_label: deltaLake
---

# deltaLake テーブル関数

Amazon S3 にある [Delta Lake](https://github.com/delta-io/delta) テーブルへの読み取り専用テーブルのようなインターフェースを提供します。

## 構文

``` sql
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

## 引数

- `url` — S3 にある既存の Delta Lake テーブルへのパスを含むバケットURL。
- `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) アカウントユーザー用の長期資格情報です。これを使用してリクエストを認証できます。これらのパラメータはオプションです。資格情報が指定されていない場合、ClickHouse の設定から使用されます。詳細は [Using S3 for Data Storage](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) を参照してください。
- `format` — ファイルの[フォーマット](/docs/ja/interfaces/formats.md/#formats)。
- `structure` — テーブルの構造。フォーマットは `'column1_name column1_type, column2_name column2_type, ...'` です。
- `compression` — パラメータはオプションです。サポートされている値：`none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮が自動検出されます。

**返り値**

S3 に指定された Delta Lake テーブルからデータを読み取るために指定された構造のテーブル。

**例**

S3 にあるテーブル `https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/` から行を選択:

``` sql
SELECT
    URL,
    UserAgent
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/')
WHERE URL IS NOT NULL
LIMIT 2
```

``` response
┌─URL───────────────────────────────────────────────────────────────────┬─UserAgent─┐
│ http://auto.ria.ua/search/index.kz/jobinmoscow/detail/55089/hasimages │         1 │
│ http://auto.ria.ua/search/index.kz/jobinmoscow.ru/gosushi             │         1 │
└───────────────────────────────────────────────────────────────────────┴───────────┘
```

**関連情報**

- [DeltaLake エンジン](/docs/ja/engines/table-engines/integrations/deltalake.md)
