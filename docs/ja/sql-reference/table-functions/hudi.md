---
slug: /ja/sql-reference/table-functions/hudi
sidebar_position: 85
sidebar_label: hudi
---

# hudi テーブル関数

Amazon S3 にある Apache [Hudi](https://hudi.apache.org/) テーブルに対して、読み取り専用のテーブルライクなインターフェースを提供します。

## 構文

``` sql
hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])
```

## 引数

- `url` — S3 に存在する Hudi テーブルへのパスを含むバケットの URL。
- `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) アカウントユーザーの長期的なクレデンシャル。これを使用してリクエストを認証することができます。これらのパラメータはオプションです。クレデンシャルが指定されていない場合、ClickHouse の設定から使用されます。詳細については、[S3 をデータストレージとして使用する](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)を参照してください。
- `format` — ファイルの[フォーマット](/docs/ja/interfaces/formats.md/#formats)。
- `structure` — テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'`。
- `compression` — このパラメータはオプションです。サポートされている値は: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮が自動検出されます。

**返される値**

指定された S3 上の Hudi テーブル内のデータを読み取るための、指定された構造のテーブル。

**関連項目**

- [Hudi エンジン](/docs/ja/engines/table-engines/integrations/hudi.md)
