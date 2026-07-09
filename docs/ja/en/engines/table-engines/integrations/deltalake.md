---
description: 'このエンジンは、Amazon S3 上の既存の Delta Lake テーブルとの読み取り専用インテグレーションを提供します。'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'DeltaLake テーブルエンジン'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="deltalake-table-engine">
  # DeltaLake テーブルエンジン
</div>

このエンジンは、S3、GCP、Azure Storage 上の既存の [Delta Lake](https://github.com/delta-io/delta) テーブルとのインテグレーションを提供し、読み取りと書き込みの両方に対応しています (v25.10以降) 。

<div id="create-table">
  ## DeltaLake テーブルを作成する
</div>

DeltaLake テーブルを作成するには、そのテーブルが S3、GCP、または Azure ストレージ上にあらかじめ存在している必要があります。以下のコマンドでは、新しいテーブルを作成するための DDL パラメータは指定できません。

<Tabs>
  <TabItem value="S3" label="S3" default>
    **構文**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
    ```

    **エンジンパラメータ**

    * `url` — 既存の Delta Lake テーブルへのパスを含むバケット URL。
    * `aws_access_key_id`, `aws_secret_access_key` - [AWS](https://aws.amazon.com/) アカウントユーザーの長期認証情報。リクエストの認証に使用できます。このパラメータは省略可能です。認証情報を指定しない場合は、設定ファイルの値が使用されます。
    * `extra_credentials` - 省略可能。ClickHouse Cloud でロールベースアクセス用の `role_arn` を渡すために使用します。設定手順については [Secure S3](/ja/cloud/data-sources/secure-s3) を参照してください。

    エンジンパラメータは、[名前付きコレクション](/ja/operations/named-collections.md) を使用して指定することもできます。

    **例**

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
    ```

    名前付きコレクションを使用する場合:

    ```xml
    <clickhouse>
        <named_collections>
            <deltalake_conf>
                <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
                <access_key_id>ABC123</access_key_id>
                <secret_access_key>Abc+123</secret_access_key>
            </deltalake_conf>
        </named_collections>
    </clickhouse>
    ```

    ```sql
    CREATE TABLE deltalake
    ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
    ```
  </TabItem>

  <TabItem value="GCP" label="GCP" default>
    **構文**

    ```sql
    -- HTTPS URL を使用する（推奨）
    CREATE TABLE table_name
    ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<access_key_id>', '<secret_access_key>')
    ```

    :::note[サポートされていない gsutil URI]
    `gs://clickhouse-docs-example-bucket` のような gsutil URI はサポートされていません。`https://storage.googleapis.com` で始まる URL を使用してください。
    :::

    **引数**

    * `url` — Delta Lake テーブルを指す GCS バケット URL。`https://storage.googleapis.com/<bucket>/<path>/`
      形式 (GCS XML API エンドポイント) を使用する必要があります。あるいは、自動変換される `gs://<bucket>/<path>/` も使用できます。
    * `access_key_id` — GCS Access Key。Google Cloud Console → Cloud Storage → Settings → Interoperability から作成します。
    * `secret_access_key` — GCS secret。

    **名前付きコレクション**

    名前付きコレクションも使用できます。
    たとえば次のとおりです。

    ```sql
    CREATE NAMED COLLECTION gcs_creds AS
    access_key_id = '<access_key>',
    secret_access_key = '<secret>';

    CREATE TABLE gcpDeltaLake
    ENGINE = DeltaLake(gcs_creds, url = 'https://storage.googleapis.com/<bucket>/<path>')
    ```
  </TabItem>

  <TabItem value="Azure" label="Azure" default>
    **構文**

    ```sql
    CREATE TABLE table_name
    ENGINE = DeltaLake(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ```

    **引数**

    * `connection_string` — Azure の接続文字列
    * `storage_account_url` — Azure ストレージアカウントの URL (例: https://account.blob.core.windows.net)
    * `container_name` — Azure コンテナー名
    * `blobpath` — コンテナー内の Delta Lake テーブルへのパス
    * `account_name` — Azure ストレージアカウント名
    * `account_key` — Azure ストレージアカウントのキー
  </TabItem>
</Tabs>

<div id="insert-data">
  ## DeltaLake テーブルエンジンを使用したテーブルへのデータの書き込み
</div>

DeltaLake テーブルエンジンを使用してテーブルを作成したら、次のようにそのテーブルへデータを挿入できます。

```sql
SET allow_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
テーブルエンジンを使用した書き込みは、delta kernel 経由でのみサポートされています。
Azure への書き込みはまだサポートされていませんが、S3 と GCS では利用できます。

Delta Lake への書き込みはベータ機能であり、`SET allow_delta_lake_writes = 1` で有効にする必要があります (バージョン 26.7 以降で利用可能です。26.7 より前のバージョンでは `SET allow_experimental_delta_lake_writes = 1` を使用してください) 。
:::

<div id="data-cache">
  ### データキャッシュ
</div>

`DeltaLake` テーブルエンジンとテーブル関数は、`S3`、`AzureBlobStorage`、`HDFS` ストレージと同様に、データキャッシュに対応しています。詳細については、[&quot;S3 テーブルエンジン&quot;](../../../engines/table-engines/integrations/s3.md#data-cache)を参照してください。

<div id="see-also">
  ## 関連項目
</div>

* [deltaLake テーブル関数](../../../sql-reference/table-functions/deltalake.md)