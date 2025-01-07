---
slug: /ja/sql-reference/table-functions/iceberg
sidebar_position: 90
sidebar_label: iceberg
---

# iceberg テーブル関数

Amazon S3、Azure、HDFS、またはローカルに保存されたApache [Iceberg](https://iceberg.apache.org/) テーブルに対して、読み取り専用のテーブルライクなインターフェースを提供します。

## 構文

``` sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

## 引数

引数の説明は、テーブル関数 `s3`、`azureBlobStorage`、`HDFS`、および `file` 内の引数の説明と一致します。
`format` は Iceberg テーブル内のデータファイルのフォーマットを表します。

**返される値**
指定された Iceberg テーブル内のデータを読み込むための、指定された構造を持つテーブル。

**例**

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse は現在、`icebergS3`、`icebergAzure`、`icebergHDFS`、`icebergLocal` テーブル関数と `IcebergS3`、`icebergAzure`、`IcebergHDFS`、`IcebergLocal` テーブルエンジンを通じて、Iceberg フォーマットの v1 および v2 の読み取りをサポートしています。
:::

## 名前付きコレクションの定義

URLと資格情報を保存するための名前付きコレクションの設定例を次に示します：

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

**エイリアス**

テーブル関数 `iceberg` は現在 `icebergS3` のエイリアスです。

**関連項目**

- [Iceberg エンジン](/docs/ja/engines/table-engines/integrations/iceberg.md)
