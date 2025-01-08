---
slug: /ja/engines/table-engines/integrations/iceberg
sidebar_position: 90
sidebar_label: Iceberg
---

# Iceberg テーブルエンジン

このエンジンは、Amazon S3、Azure、HDFS そしてローカルに保存された Apache [Iceberg](https://iceberg.apache.org/) テーブルと既存のテーブルへの読み取り専用の統合を提供します。

## テーブルの作成

Iceberg テーブルはストレージにすでに存在している必要があることに注意してください。このコマンドは、新しいテーブルを作成するためのDDLパラメータを受け取りません。

``` sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

**エンジンの引数**

引数の説明は、`S3`、`AzureBlobStorage`、`HDFS` そして `File` の各エンジンの引数説明と一致します。
`format` は、Iceberg テーブル内のデータファイルの形式を表します。

エンジンパラメータは、[Named Collections](../../../operations/named-collections.md) を使用して指定できます。

**例**

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Named Collectionsを使用:

``` xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')
```

**エイリアス**

テーブルエンジン `Iceberg` は現在 `IcebergS3` のエイリアスです。

### データキャッシュ {#data-cache}

`Iceberg` テーブルエンジンおよびテーブル関数は、`S3`、`AzureBlobStorage`、`HDFS` ストレージと同様にデータキャッシュをサポートしています。詳細は[こちら](../../../engines/table-engines/integrations/s3.md#data-cache)をご覧ください。

## 参照

- [Iceberg テーブル関数](/docs/ja/sql-reference/table-functions/iceberg.md)
