---
slug: /ja/engines/table-engines/integrations/hudi
sidebar_position: 86
sidebar_label: Hudi
---

# Hudi テーブルエンジン

このエンジンは、Amazon S3 に存在する Apache [Hudi](https://hudi.apache.org/) テーブルとの読み取り専用の統合機能を提供します。

## テーブルの作成

Hudi テーブルはすでに S3 に存在している必要があり、このコマンドは新しいテーブルを作成するための DDL パラメータを受け取りません。

``` sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,])
```

**エンジンパラメータ**

- `url` — 既存の Hudi テーブルへのパスを含むバケットの URL。
- `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) アカウントユーザーのための長期資格情報。これを使用してリクエストを認証できます。このパラメータはオプションです。資格情報が指定されていない場合、設定ファイルから使用されます。

エンジンパラメータは [Named Collections](/docs/ja/operations/named-collections.md) を使用して指定できます。

**例**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Named Collections を使用する場合:

``` xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123<access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

## 参照

- [hudi テーブル関数](/docs/ja/sql-reference/table-functions/hudi.md)
