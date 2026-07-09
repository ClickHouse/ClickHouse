---
description: 'このエンジンは、Amazon S3 上の既存の Apache Hudi
  テーブルとの読み取り専用のインテグレーションを提供します。'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Hudi テーブルエンジン'
doc_type: 'reference'
---

このエンジンは、Amazon S3 上の既存の Apache [Hudi](https://hudi.apache.org/) テーブルとの読み取り専用のインテグレーションを提供します。

<div id="create-table">
  ## テーブルの作成
</div>

Hudiテーブルは、あらかじめ S3 上に存在している必要があります。このコマンドでは、新しいテーブルを作成するための DDL パラメータは受け取れません。

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**エンジンパラメータ**

* `url` — 既存のHudiテーブルへのパスを含むBucket URL。
* `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/)アカウントのユーザー用の長期的な認証情報です。これらを使用してリクエストを認証できます。このパラメータは任意です。認証情報が指定されていない場合は、設定ファイルの認証情報が使用されます。
* `extra_credentials` - 任意です。ClickHouse Cloudでロールベースアクセス用の`role_arn`を渡すために使用します。設定手順については[Secure S3](/ja/cloud/data-sources/secure-s3)を参照してください。

エンジンパラメータは、[名前付きコレクション](/ja/operations/named-collections.md)を使用して指定できます。

**例**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

named collections を使用する場合:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## 関連項目
</div>

* [hudi テーブル関数](/ja/sql-reference/table-functions/hudi.md)