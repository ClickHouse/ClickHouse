---
slug: /ja/engines/table-engines/integrations/deltalake
sidebar_position: 40
sidebar_label: DeltaLake
---

# DeltaLake テーブルエンジン

このエンジンは、Amazon S3 に存在する既存の [Delta Lake](https://github.com/delta-io/delta) テーブルへの読み取り専用の統合を提供します。

## テーブル作成

Delta Lake テーブルはすでに S3 に存在している必要があることに注意してください。このコマンドは新しいテーブルを作成するための DDL パラメータを受け取りません。

``` sql
CREATE TABLE deltalake
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,])
```

**エンジンパラメータ**

- `url` — 既存の Delta Lake テーブルへのパスを持つバケットの URL。
- `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) アカウントユーザーの長期的な認証情報。これを使用してリクエストを認証できます。このパラメータはオプションです。認証情報が指定されていない場合、設定ファイルから使用されます。

エンジンパラメータは、[Named Collections](/docs/ja/operations/named-collections.md) を使用して指定できます。

**例**

```sql
CREATE TABLE deltalake ENGINE=DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Named collections を使用する：

``` xml
<clickhouse>
    <named_collections>
        <deltalake_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123<access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </deltalake_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE deltalake ENGINE=DeltaLake(deltalake_conf, filename = 'test_table')
```

### データキャッシュ {#data-cache}

`Iceberg` テーブルエンジンおよびテーブル関数は、`S3`、`AzureBlobStorage`、`HDFS` ストレージと同様にデータキャッシングをサポートします。[こちら](../../../engines/table-engines/integrations/s3.md#data-cache)を参照してください。

## 参照

- [deltaLake テーブル関数](../../../sql-reference/table-functions/deltalake.md)
