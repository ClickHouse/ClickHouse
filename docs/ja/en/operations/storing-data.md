---
description: 'highlight-next-line のドキュメント'
sidebar_label: 'データ保存用の外部ディスク'
sidebar_position: 68
slug: /operations/storing-data
title: 'データ保存用の外部ディスク'
doc_type: 'guide'
---

ClickHouse で処理されるデータは通常、ClickHouse server が稼働している
マシンのローカルファイルシステムに保存されます。そのため、大容量のディスクが必要になり、
コストが高くなる場合があります。データをローカルに保存しないようにするため、
以下のようなストレージオプションがサポートされています。

1. [Amazon S3](https://aws.amazon.com/s3/) オブジェクトストレージ
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs)
3. 非対応: Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

<br />

:::note
ClickHouse は外部テーブルエンジンもサポートしています。これは、
このページで説明している外部ストレージオプションとは異なり、
Parquet などの一般的なファイルフォーマットで保存されたデータを
読み取るためのものです。このページでは、
ClickHouse の `MergeTree` ファミリーまたは `Log` ファミリーのテーブル向けの
ストレージ構成について説明します。

1. `Amazon S3` ディスクに保存されたデータを扱うには、[S3](/ja/engines/table-engines/integrations/s3.md) テーブルエンジンを使用します。
2. Azure Blob Storage に保存されたデータを扱うには、[AzureBlobStorage](/ja/engines/table-engines/integrations/azureBlobStorage.md) テーブルエンジンを使用します。
3. Hadoop Distributed File System (非対応) 内のデータを扱うには、[HDFS](/ja/engines/table-engines/integrations/hdfs.md) テーブルエンジンを使用します。
   :::

<div id="configuring-external-storage">
  ## 外部ストレージを設定する
</div>

[`MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree.md) および [`Log`](/ja/engines/table-engines/log-family/log.md)
ファミリーのテーブルエンジンでは、それぞれ `s3`、`azure_blob_storage`、`hdfs` (未サポート) タイプのディスクを使用して、データを `S3`、`AzureBlobStorage`、`HDFS` (未サポート) に保存できます。

ディスク設定には、次の項目が必要です。

1. `type` セクション。値は `s3`、`azure_blob_storage`、`hdfs` (未サポート) 、`local_blob_storage`、`web` のいずれかである必要があります。
2. 各外部ストレージタイプに応じた設定。

ClickHouse バージョン 24.1 以降では、新しい設定オプションを使用できます。
この場合、次の指定が必要です。

1. `type` を `object_storage` に設定する
2. `object_storage_type`。値は `s3`、`azure_blob_storage` (`24.3` 以降では単に `azure` も可) 、`hdfs` (未サポート) 、`local_blob_storage` (`24.3` 以降では単に `local` も可) 、`web` のいずれかである必要があります。

<br />

必要に応じて `metadata_type` を指定することもできます (デフォルトは `local`) 。また、`plain`、`web`、および `24.4` 以降では `plain_rewritable` に設定することも可能です。
`plain` メタデータタイプの使用方法は [plain storage section](/ja/operations/storing-data#plain-storage) で説明されています。`web` メタデータタイプは `web` オブジェクトストレージタイプでのみ使用できます。`local` メタデータタイプではメタデータファイルがローカルに保存されます (各メタデータファイルには、オブジェクトストレージ内のファイルへのマッピングと、それらに関する追加のメタ情報が含まれます) 。

例えば:

```xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

は以下の設定と同等です (バージョン `24.1` 以降) :

```xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

以下の設定：

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

は次のようになります:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

ストレージ構成全体の例は次のとおりです:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

バージョン24.1以降では、次のように記述することもできます:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

すべての`MergeTree`テーブルで特定の種類のストレージをデフォルトにするには、
次のセクションを設定ファイルに追加します。

```xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

特定のテーブルに対して特定のストレージポリシーを設定したい場合は、
テーブルの作成時に設定でそれを定義できます。

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

`storage_policy` の代わりに `disk` を使用することもできます。この場合、
設定ファイルに `storage_policy` セクションを設ける必要はなく、`disk`
セクションだけで十分です。

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

<div id="refresh-parts-interval-and-table-disk">
  ## refresh_parts_interval and table_disk
</div>

この設定は、パーツが外部から書き込まれる可能性があり、ストレージ上のメタデータ検出をリフレッシュする必要がある、非 Replicated MergeTree テーブルを対象としています。

MergeTree 設定 `refresh_parts_interval` を使うと、基盤ストレージ上のデータパーツ一覧を定期的にリフレッシュできます (たとえば、外部で書き込まれたパーツを取り込むため) 。ここで重要なのは、**レプリカ間で共有されるメタデータ** と **レプリカローカルなメタデータ** (たとえば、レプリカごとにローカルメタデータを持つ S3) との違いです。新しいパーツがすべてのレプリカから見えるようになるのは、メタデータが共有されている場合に限られます。オブジェクトストレージ を使っているだけでは、メタデータが共有されていることにはなりません。

* **オブジェクトストレージ (たとえば `disk = 's3'`) を使っていても、メタデータが共有されているとは限りません。** メタデータが各レプリカにローカル保存される場合 (デフォルト) 、各レプリカは オブジェクトストレージ 内のブロブへのポインタをそれぞれ独立して管理します。あるレプリカで行われた変更は、ほかのレプリカからは見えません。この場合、各レプリカが読み取るメタデータはレプリカローカルであるため、`refresh_parts_interval` を使っても新しいパーツがレプリカ間で見えるようにはなりません。

* **パーツの自動リフレッシュには、filesystem のメタデータが共有されている必要があります** (または、リフレッシュが適用可能な、テーブル所有の readonly メタデータをそのテーブルが使用している必要があります) 。`table_disk = true` をテーブルローカルなディスクと組み合わせて設定すること (たとえば `SETTINGS disk = disk(type=object_storage, ...), table_disk = true`) は、正しいセマンティクスを得る方法の 1 つです。この場合、テーブルがメタデータのライフサイクルを管理し、ストレージは readonly として扱われるため、`refresh_parts_interval` が動作し、外部で追加されたパーツを検出できます。

* **グローバルに定義されたディスク** (たとえば `storage_configuration` 内の `disk = 's3'`) を使い、デフォルトのローカルメタデータを使用している場合、各レプリカはそれぞれ独自のメタデータ状態を持ちます。ブロブが S3 に保存されていても、`refresh_parts_interval` の観点ではそのストレージは共有されているとは見なされないため、ClickHouse の外部または別のレプリカで作成された新しいパーツは検出されません。

パーツを自動的にリフレッシュするには、メタデータが共有されていることを確認するか、上記のように `table_disk = true` を指定したテーブルレベルのディスクを使用してください。レプリカローカルなメタデータのまま `refresh_parts_interval` のみに依存しても、期待どおりにパーツはリフレッシュされません。

:::note
`refresh_parts_interval` は ReplicatedMergeTree テーブルでは使用されません。
レプリケートテーブルでは、パーツはすでにレプリケーションの仕組みによって同期されます。
この設定が適用されるのは、パーツが外部から書き込まれ、メタデータのリフレッシュが必要な、レプリケーションされていない MergeTree テーブルのみです。
:::

<div id="dynamic-configuration">
  ## 動的構成
</div>

構成ファイル内で事前に定義された
ディスクを使わずにストレージ構成を指定することも可能で、その場合は
`CREATE`/`ATTACH` クエリの設定で構成できます。

次のクエリ例は、上記の動的ディスク構成を基に、
URL に保存されたテーブルのデータを cache するためにローカルディスクを使用する方法を
示しています。

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  -- highlight-end
```

以下の例では、外部ストレージに cache を追加します。

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
-- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
-- highlight-end
```

以下で強調表示されている設定では、`type=web` のディスクが
`type=cache` のディスク内にネストされていることに注目してください。

:::note
この例では `type=web` を使用していますが、ローカルディスクを含め、任意の disk type を動的に設定できます。
ローカルディスクでは、path 引数を
server config parameter `custom_local_disks_base_directory` の配下に指定する必要があります。このパラメータには
デフォルト値がないため、ローカルディスクを使用する場合はこれも設定してください。
:::

config ベースの設定と SQL で定義した設定を組み合わせることも
可能です:

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  -- highlight-end
```

ここで `web` はサーバー設定ファイルで定義されています:

```xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

<div id="s3-storage">
  ### S3ストレージの使用
</div>

<div id="required-parameters-s3">
  #### パラメータ
</div>

| パラメータ               | 説明                                                                                                                                                         |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`          | `path` または `virtual hosted` [styles](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html) 形式の S3 エンドポイント URL。データ保存用のバケットとルートパスを含める必要があります。 |
| `access_key_id`     | 認証に使用する S3 アクセスキー ID。                                                                                                                                      |
| `secret_access_key` | 認証に使用する S3 シークレットアクセスキー。                                                                                                                                   |

<div id="required-parameters-s3">
  #### パラメータ
</div>

| パラメータ                                                                                                               | 説明                                                                                                                                                                                                                     | デフォルト値                                   |
| ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `region`                                                                                                            | S3 のリージョン名。                                                                                                                                                                                                            | *                                        |
| `support_batch_delete`                                                                                              | バッチ削除がサポートされているかどうかを確認するかを制御します。Google Cloud Storage (GCS) を使用する場合、GCS はバッチ削除をサポートしていないため、`false` に設定してください。                                                                                                           | `true`                                   |
| `use_environment_credentials`                                                                                       | 環境変数 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN` が存在する場合、そこから AWS 認証情報を読み取ります。注意: 環境認証情報はすべての S3 ディスクで共有されます。ディスクごとに異なる認証情報を使用するには、代わりに各ディスクで `access_key_id` と `secret_access_key` を明示的に指定してください。 | `false`                                  |
| `use_insecure_imds_request`                                                                                         | `true` の場合、Amazon EC2のメタデータから認証情報を取得する際に、安全でない IMDS リクエストを使用します。                                                                                                                                                       | `false`                                  |
| `expiration_window_seconds`                                                                                         | 有効期限ベースの認証情報が期限切れかどうかを確認する際の猶予期間 (Seconds) 。                                                                                                                                                                           | `120`                                    |
| `proxy`                                                                                                             | S3 endpoint 用のプロキシ設定です。`proxy` ブロック内の各 `uri` 要素には、プロキシの URL を指定する必要があります。                                                                                                                                              | -                                        |
| `connect_timeout_ms`                                                                                                | ソケット接続のタイムアウト時間 (Milliseconds) 。                                                                                                                                                                                       | `10000` (10 seconds)                     |
| `request_timeout_ms`                                                                                                | リクエストのタイムアウトをMilliseconds単位で指定します。                                                                                                                                                                                     | `5000` (5 Seconds)                       |
| `retry_attempts`                                                                                                    | 失敗したリクエストに対する再試行回数。                                                                                                                                                                                                    | `10`                                     |
| `single_read_retries`                                                                                               | 読み取り中に接続が切断された場合の再試行回数。                                                                                                                                                                                                | `4`                                      |
| `min_bytes_for_seek`                                                                                                | シーケンシャル読み取りの代わりにseek操作を使用する最小バイト数。                                                                                                                                                                                     | `1 MB`                                   |
| `metadata_path`                                                                                                     | S3のメタデータファイルを保存するローカルファイルシステムのパス。                                                                                                                                                                                      | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`                                                                                                 | `true` の場合、起動時のディスクアクセス確認をスキップします。                                                                                                                                                                                     | `false`                                  |
| `header`                                                                                                            | 指定したHTTP headerをリクエストに追加します。複数回指定できます。                                                                                                                                                                                 | *                                        |
| `server_side_encryption_customer_key_base64`                                                                        | SSE-C 暗号化された S3 オブジェクトにアクセスするために必要なヘッダー。                                                                                                                                                                               | -                                        |
| `server_side_encryption_kms_key_id`                                                                                 | [SSE-KMS encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html) で暗号化された S3 オブジェクトにアクセスするために必要なヘッダーです。空文字列を指定すると、AWS 管理の S3 キーが使用されます。                                              | *                                        |
| `server_side_encryption_kms_encryption_context`                                                                     | SSE-KMS の暗号化コンテキスト用ヘッダー (`server_side_encryption_kms_key_id` と併用) 。                                                                                                                                                    | -                                        |
| `server_side_encryption_kms_bucket_key_enabled`                                                                     | SSE-KMS で S3 バケットキーを有効にします (`server_side_encryption_kms_key_id` と併用) 。                                                                                                                                                 | bucket レベルの設定に準拠                         |
| `s3_max_put_rps`                                                                                                    | スロットリングが発生するまでの 1 秒あたりの PUT リクエストの最大数。                                                                                                                                                                                 | `0` (無制限)                                |
| `s3_max_put_burst`                                                                                                  | RPS 制限に達するまでに同時実行できる PUT リクエストの最大数。                                                                                                                                                                                    | `s3_max_put_rps`と同じ                      |
| `s3_max_get_rps`                                                                                                    | スロットリングが発生するまでの 1 秒あたりの GET リクエストの最大数。                                                                                                                                                                                 | `0` (無制限)                                |
| `s3_max_get_burst`                                                                                                  | RPS 制限に達するまでに同時実行できる GET リクエストの最大数。                                                                                                                                                                                    | `s3_max_get_rps` と同様                     |
| `read_resource`                                                                                                     | [スケジューリング](/ja/operations/workload-scheduling.md)対象の読み取りリクエストに使用するリソース名。                                                                                                                                                  | 空文字列 (無効)                                |
| `write_resource`                                                                                                    | [スケジューリング](/ja/operations/workload-scheduling.md)する書き込みリクエストのリソース名。                                                                                                                                                       | 空文字列 (無効)                                |
| `key_template`                                                                                                      | オブジェクトキーの生成フォーマットを、[re2](https://github.com/google/re2/wiki/Syntax) 構文で定義します。`storage_metadata_write_full_object_key` フラグが必要です。`endpoint` の `root path` とは併用できません。`key_compatibility_prefix` が必要です。                    | *                                        |
| `key_compatibility_prefix`                                                                                          | `key_template` と併用する場合は必須です。古いメタデータのバージョンを読み取るために、`endpoint` で以前使用していた `root path` を指定します。                                                                                                                             | -                                        |
| `read_only`                                                                                                         | ディスクからの読み取りのみを許可します。                                                                                                                                                                                                   | *                                        |
| :::note                                                                                                             |                                                                                                                                                                                                                        |                                          |
| Google Cloud Storage (GCS) も、タイプ `s3` を使用することでサポートされます。詳細は [GCS をバックエンドとする MergeTree](/ja/integrations/gcs) を参照してください。 |                                                                                                                                                                                                                        |                                          |
| :::                                                                                                                 |                                                                                                                                                                                                                        |                                          |

<div id="plain-storage">
  ### Plain Storage の使用
</div>

`22.10` では、新しいディスクタイプ `s3_plain` が導入されました。これは、一度書き込んだら変更できないストレージを提供します。
この設定パラメータは、`s3` ディスクタイプと同じです。
`s3` ディスクタイプとは異なり、データをそのまま保存します。つまり、
ランダムに生成されたブロブ名を使う代わりに、通常のファイル名を使用し
(ClickHouse がローカルディスクにファイルを保存するのと同じ方式です) 、ローカルには
メタデータを保存しません。たとえば、必要な情報は
`s3` 上のデータから導き出されます。

このディスクタイプでは、テーブルの静的なバージョンを保持できます。これは、
既存データに対してマージを実行できず、新しい
データの挿入もできないためです。このディスクタイプのユースケースの 1 つは、その上にバックアップを作成することです。これは
`BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')` を使って実行できます。その後、
`RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')`
を実行するか、`ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'` を使用できます。

設定:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

`24.1` 以降では、`plain` メタデータタイプを使用して任意のオブジェクトストレージディスク (`s3`、`azure`、`hdfs` (未サポート) 、`local`) を設定できます。

設定:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

<div id="s3-plain-rewritable-storage">
  ### S3 Plain Rewritable Storage の使用
</div>

新しいディスクタイプ `s3_plain_rewritable` は `24.4` で導入されました。
`s3_plain` ディスクタイプと同様に、メタデータファイル用の
追加ストレージは必要ありません。代わりに、
メタデータは S3 に保存されます。
`s3_plain` ディスクタイプとは異なり、`s3_plain_rewritable` では マージ を実行でき、
`INSERT` 操作もサポートされます。
[ミューテーション](/ja/sql-reference/statements/alter#mutations) とテーブルのレプリケーションはサポートされていません。

このディスクタイプのユースケースの 1 つは、レプリケーションなしの `MergeTree` テーブルです。`s3` ディスクタイプも
レプリケーションなしの `MergeTree` テーブルに適していますが、テーブルのローカルメタデータが
不要で、利用できる操作が限られていても問題ない場合は、
`s3_plain_rewritable` ディスクタイプを選択できます。これは、たとえば
システムテーブルで有用です。

設定:

```xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

に等しい

```xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

`24.5` 以降では、`plain_rewritable` メタデータタイプを使用して、
任意のオブジェクトストレージディスク
(`s3`、`azure`、`local`) を設定できます。

<div id="azure-blob-storage">
  ### Azure Blob Storage を使用する
</div>

`MergeTree` ファミリーのテーブルエンジンでは、タイプ `azure_blob_storage` のディスクを使用して、[Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) にデータを保存できます。

設定例:

```xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

<div id="azure-blob-storage-connection-parameters">
  #### 接続パラメーター
</div>

| パラメーター                     | 説明                                                                                                                          | デフォルト値              |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `storage_account_url` (必須) | Azure Blob Storage アカウントのURL。例: `http://account.blob.core.windows.net` または `http://azurite1:10000/devstoreaccount1`。        | -                   |
| `container_name`           | 対象のコンテナー名。                                                                                                                  | `default-container` |
| `container_already_exists` | コンテナー作成時の動作を制御します: <br />- `false`: 新しいコンテナーを作成します <br />- `true`: 既存のコンテナーに直接接続します <br />- 未設定: コンテナーが存在するか確認し、必要に応じて作成します | -                   |

認証パラメーター (ディスクは利用可能なすべての認証方式 **および** Managed Identity Credential を試行します) :

| パラメーター              | 説明                                                 |
| ------------------- | -------------------------------------------------- |
| `connection_string` | 接続文字列を使用した認証に使用します。                                |
| `account_name`      | Shared Key を使用した認証に使用します (`account_key` とともに使用) 。  |
| `account_key`       | Shared Key を使用した認証に使用します (`account_name` とともに使用) 。 |

<div id="azure-blob-storage-limit-parameters">
  #### 制限パラメータ
</div>

| Parameter                            | Description                                 |
| ------------------------------------ | ------------------------------------------- |
| `s3_max_single_part_upload_size`     | Blob Storage への単一ブロックアップロードの最大サイズ。          |
| `min_bytes_for_seek`                 | シーク可能な範囲の最小サイズ。                             |
| `max_single_read_retries`            | Blob Storage からデータの chunk を読み取る試行回数の上限。     |
| `max_single_download_retries`        | Blob Storage から読み取り可能なバッファをダウンロードする試行回数の上限。 |
| `thread_pool_size`                   | `IDiskRemote` のインスタンス化に使用するスレッド数の上限。        |
| `s3_max_inflight_parts_for_one_file` | 単一オブジェクトに対して同時に実行できる PUT リクエスト数の上限。         |

<div id="azure-blob-storage-other-parameters">
  #### その他のパラメータ
</div>

| パラメータ                            | 説明                                                                 | デフォルト値                                   |
| -------------------------------- | ------------------------------------------------------------------ | ---------------------------------------- |
| `metadata_path`                  | Blob Storage のメタデータファイルを保存するローカルファイルシステム上のパス。                      | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`              | `true` の場合、起動時のディスクアクセスチェックをスキップします。                               | `false`                                  |
| `read_resource`                  | [scheduling](/ja/operations/workload-scheduling.md) の読み取りリクエスト用リソース名。 | 空文字列 (無効)                                |
| `write_resource`                 | [scheduling](/ja/operations/workload-scheduling.md) の書き込みリクエスト用リソース名。 | 空文字列 (無効)                                |
| `metadata_keep_free_space_bytes` | メタデータ用ディスクで確保しておく空き容量。                                             | -                                        |

動作する設定例は、結合テストディレクトリにあります (たとえば [test&#95;merge&#95;tree&#95;azure&#95;blob&#95;storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) や [test&#95;azure&#95;blob&#95;storage&#95;zero&#95;copy&#95;replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml) を参照してください) 。

:::note ゼロコピー レプリケーションは本番環境向けではありません
ゼロコピー レプリケーションは、ClickHouse バージョン 22.8 以降ではデフォルトで無効化されています。この機能は本番環境での使用を推奨していません。
:::

<div id="using-hdfs-storage-unsupported">
  ## HDFS ストレージの使用 (サポート対象外)
</div>

この設定例では、次のように構成されています。

* ディスクのタイプは `hdfs` です (サポート対象外)
* データは `hdfs://hdfs1:9000/clickhouse/` に配置されています

なお、HDFS はサポート対象外のため、使用時に問題が発生する可能性があります。問題が発生した場合は、修正内容を含むプルリクエストをお送りください。

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
                <skip_access_check>true</skip_access_check>
            </hdfs>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>
</clickhouse>
```

HDFS は一部の特殊なケースでは動作しない可能性がある点にご留意ください。

<div id="encrypted-virtual-file-system">
  ### データ暗号化を使用する
</div>

[S3](/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)、[HDFS](#using-hdfs-storage-unsupported) (未サポート) の外部ディスク、またはローカルディスクに保存されるデータは暗号化できます。暗号化モードを有効にするには、設定ファイルで type `encrypted` のディスクを定義し、データの保存先となるディスクを選択する必要があります。`encrypted` ディスクでは、書き込まれるすべてのファイルがその場で暗号化され、`encrypted` ディスクからファイルを読み取る際には自動的に復号されます。そのため、`encrypted` ディスクは通常のディスクと同じように使用できます。

ディスク設定の例:

```xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

たとえば、ClickHouseがあるテーブルのデータをファイル `store/all_1_1_0/data.bin` として `disk1` に書き込む場合、実際にはこのファイルは物理ディスク上のパス `/path1/store/all_1_1_0/data.bin` に書き込まれます。

同じファイルを `disk2` に書き込む場合、実際には暗号化モードで、物理ディスク上のパス `/path1/path2/store/all_1_1_0/data.bin` に書き込まれます。

<div id="required-parameters-encrypted-disk">
  ### パラメータ
</div>

| パラメータ  | 型      | 説明                                                                    |
| ------ | ------ | --------------------------------------------------------------------- |
| `type` | String | 暗号化されたディスクを作成するには、`encrypted` に設定する必要があります。                           |
| `disk` | String | 基盤となるストレージに使用するディスクの種類。                                               |
| `key`  | Uint64 | 暗号化および復号に使用するキーです。`key_hex` を使って16進数で指定できます。`id` 属性を使うと、複数のキーを指定できます。 |

<div id="required-parameters-encrypted-disk">
  ### パラメータ
</div>

| パラメータ            | 型      | デフォルト         | 説明                                                                                                                         |
| ---------------- | ------ | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `path`           | String | ルートディレクトリ     | データの保存先となるディスク上の場所。                                                                                                        |
| `current_key_id` | String | -             | 暗号化に使用するキー ID。指定したすべてのキーを復号に使用できます。                                                                                        |
| `algorithm`      | Enum   | `AES_128_CTR` | 使用する暗号化アルゴリズム。オプション: <br />- `AES_128_CTR` (16 バイトキー)  <br />- `AES_192_CTR` (24 バイトキー)  <br />- `AES_256_CTR` (32 バイトキー)  |

ディスク設定の例:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

<div id="using-local-cache">
  ### ローカル cache の使用
</div>

バージョン 22.3 以降では、ストレージ構成内のディスクに対してローカル cache を構成できます。
バージョン 22.3〜22.7 では、cache は `s3` ディスクタイプでのみサポートされます。バージョン 22.8 以降では、cache は S3、Azure、Local、Encrypted など、任意のディスクタイプでサポートされます。
バージョン 23.5 以降では、cache はリモートディスクタイプでのみサポートされます: S3、Azure、HDFS (非サポート) 。
cache では `LRU` cache policy を使用します。

バージョン 22.8 以降の構成例:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>cache</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

22.8より前のバージョン向けの設定例:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

File Cache **ディスク設定**:

これらの設定は、ディスク設定セクションで定義する必要があります。

| Parameter                             | Type    | Default    | Description                                                                                                                       |
| ------------------------------------- | ------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `path`                                | String  | -          | **必須**。cache を保存するディレクトリのパス。                                                                                                      |
| `max_size`                            | Size    | -          | **必須**。cache の最大サイズ。バイト単位または可読形式 (例: `10Gi`) で指定します。上限に達すると、ファイルは LRU ポリシーに従って追い出されます。`ki`、`Mi`、`Gi` フォーマットをサポートします (v22.10 以降) 。 |
| `cache_on_write_operations`           | Boolean | `false`    | `INSERT` クエリおよびバックグラウンドマージに対するライトスルー cache を有効にします。クエリごとに `enable_filesystem_cache_on_write_operations` で上書きできます。                 |
| `enable_filesystem_query_cache_limit` | Boolean | `false`    | `max_query_cache_size` に基づくクエリ単位の cache サイズ制限を有効にします。                                                                             |
| `enable_cache_hits_threshold`         | Boolean | `false`    | 有効にすると、データは複数回読み取られた後にのみ cache されます。                                                                                              |
| `cache_hits_threshold`                | Integer | `0`        | データが cache されるまでに必要な読み取り回数 (`enable_cache_hits_threshold` が必要) 。                                                                  |
| `enable_bypass_cache_with_threshold`  | Boolean | `false`    | 大きな読み取り範囲では cache をバイパスします。                                                                                                       |
| `bypass_cache_threshold`              | Size    | `256Mi`    | cache のバイパスをトリガーする読み取り範囲のサイズ (`enable_bypass_cache_with_threshold` が必要) 。                                                         |
| `max_file_segment_size`               | Size    | `8Mi`      | 1 つの cache ファイルの最大サイズ。バイト単位または可読形式で指定します。                                                                                         |
| `max_elements`                        | Integer | `10000000` | cache ファイルの最大数。                                                                                                                   |
| `load_metadata_threads`               | Integer | `16`       | 起動時に cache のメタデータを読み込むためのスレッド数。                                                                                                   |
| `use_split_cache`                     | Boolean | `false`    | ファイルを system 用と data 用に分離して使用します。                                                                                                 |
| `split_cache_ratio`                   | Double  | `0.1`      | split&#95;cache における、cache 全体のサイズに対する system セグメントの比率。                                                                            |

> **注**: サイズ値は `ki`、`Mi`、`Gi` などの単位をサポートします (例: `10Gi`) 。

<div id="file-cache-query-profile-settings">
  ## File Cache のクエリ/プロファイル設定
</div>

| 設定                                                                      | 型    | デフォルト                   | 説明                                                                                                                           |
| ----------------------------------------------------------------------- | ---- | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `enable_filesystem_cache`                                               | ブール値 | `true`                  | `cache` ディスクタイプを使用している場合でも、クエリごとの cache の使用を有効/無効にします。                                                                       |
| `read_from_filesystem_cache_if_exists_otherwise_bypass_cache`           | ブール値 | `false`                 | 有効にすると、データが存在する場合にのみ cache を使用し、新しいデータは cache しません。                                                                          |
| `enable_filesystem_cache_on_write_operations`                           | ブール値 | `false` (Cloud: `true`) | ライトスルー cache を有効にします。cache 設定で `cache_on_write_operations` が必要です。                                                            |
| `enable_filesystem_cache_log`                                           | ブール値 | `false`                 | `system.filesystem_cache_log` への詳細な cache 使用ログを有効にします。                                                                       |
| `filesystem_cache_allow_background_download`                            | ブール値 | `true`                  | 一部のみダウンロードされたセグメントのダウンロードをバックグラウンドで完了できるようにします。無効にすると、現在のクエリ/セッションではダウンロードがフォアグラウンドのままになります。                                 |
| `max_query_cache_size`                                                  | Size | `false`                 | クエリごとの cache の最大サイズです。cache 設定で `enable_filesystem_query_cache_limit` が必要です。                                                 |
| `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` | ブール値 | `true`                  | `max_query_cache_size` に達した場合の動作を制御します: <br />- `true`: 新しいデータのダウンロードを停止します <br />- `false`: 新しいデータ用の空き領域を確保するため、古いデータを削除します |

:::warning
cache 設定と cache クエリ設定は最新の ClickHouse バージョンに対応しています。
それ以前のバージョンでは、一部の機能がサポートされていない場合があります。
:::

<div id="cache-system-tables-file-cache">
  #### キャッシュ関連のシステムテーブル
</div>

| テーブル名                         | 説明                         | 要件                                         |
| ----------------------------- | -------------------------- | ------------------------------------------ |
| `system.filesystem_cache`     | ファイルシステムキャッシュの現在の状態を表示します。 | なし                                         |
| `system.filesystem_cache_log` | クエリごとの詳細なキャッシュ使用統計を表示します。  | `enable_filesystem_cache_log = true` が必要です |

<div id="cache-commands-file-cache">
  #### キャッシュ関連コマンド
</div>

<div id="system-clear-filesystem-cache-on-cluster">
  ##### `SYSTEM CLEAR|DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER`
</div>

このコマンドは、`<cache_name>` が指定されていない場合にのみ使用できます

<div id="show-filesystem-caches">
  ##### `SHOW FILESYSTEM CACHES`
</div>

server で設定されているファイルシステムキャッシュの一覧を表示します。
(`22.8` 以前のバージョンでは、このコマンド名は `SHOW CACHES` です)

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="describe-filesystem-cache">
  ##### `DESCRIBE FILESYSTEM CACHE '<cache_name>'`
</div>

特定のキャッシュの設定と一般的な統計情報の一部を表示します。
キャッシュ名は `SHOW FILESYSTEM CACHES` コマンドで確認できます。 (`22.8` 以下の
バージョンでは、このコマンドは `DESCRIBE CACHE` という名前です)

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

```text title="Response"
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

| キャッシュの現在のメトリクス            | キャッシュの非同期メトリクス         | キャッシュのプロファイルイベント                                                                          |
| ------------------------- | ---------------------- | ----------------------------------------------------------------------------------------- |
| `FilesystemCacheSize`     | `FilesystemCacheBytes` | `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes`               |
| `FilesystemCacheElements` | `FilesystemCacheFiles` | `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds` |
|                           |                        | `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`               |
|                           |                        | `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`             |

<div id="web-storage">
  ### 静的Webストレージの使用 (読み取り専用)
</div>

これは読み取り専用のディスクです。データは読み取られるだけで、変更されることはありません。新しいテーブル
は `ATTACH TABLE` クエリでこのディスクに読み込まれます (以下の例を参照) 。ローカルディスク
は実際には使用されず、各 `SELECT` クエリごとに必要なデータを
取得するための `http` リクエストが発生します。テーブルデータを変更しようとすると
例外が発生します。つまり、次の種類のクエリは許可されません: [`CREATE TABLE`](/ja/sql-reference/statements/create/table.md),
[`ALTER TABLE`](/ja/sql-reference/statements/alter/index.md), [`RENAME TABLE`](/ja/sql-reference/statements/rename#rename-table),
[`DETACH TABLE`](/ja/sql-reference/statements/detach.md) および [`TRUNCATE TABLE`](/ja/sql-reference/statements/truncate.md)。
Webストレージは読み取り専用の用途に利用できます。たとえば、
サンプルデータの公開やデータ移行に使えます。`clickhouse-static-files-uploader` というツールがあり、
指定したテーブルのデータディレクトリを準備します (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`) 。
必要なテーブルごとに、ファイルを含むディレクトリが生成されます。これらのファイルは、たとえば静的ファイルを配信する
Webサーバーにアップロードできます。この準備が完了すると、
`DiskWeb` を使って任意の ClickHouseサーバーにこのテーブルを読み込めます。

このサンプル設定では:

* ディスクのタイプは `web`
* データは `http://nginx:80/test1/` でホストされています
* ローカルストレージ上の cache が使用されます

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>http://nginx:80/test1/</endpoint>
            </web>
            <cached_web>
                <type>cache</type>
                <disk>web</disk>
                <path>cached_web_cache/</path>
                <max_size>100000000</max_size>
            </cached_web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
            <cached_web>
                <volumes>
                    <main>
                        <disk>cached_web</disk>
                    </main>
                </volumes>
            </cached_web>
        </policies>
    </storage_configuration>
</clickhouse>
```

:::tip
Web データセットを常用する予定がない場合は、クエリ内で一時的にストレージを設定することもできます。[動的設定](#dynamic-configuration)を参照し、設定ファイルの編集は省略してください。

[デモデータセット](https://github.com/ClickHouse/web-tables-demo) は GitHub でホストされています。独自のテーブルを Web ストレージ用に準備する方法については、ツール [clickhouse-static-files-uploader](/ja/operations/utilities/static-files-disk-uploader) を参照してください。
:::

この `ATTACH TABLE` クエリでは、指定された `UUID` はデータのディレクトリ名に対応しており、endpoint は GitHub の生コンテンツの URL です。

```sql
-- highlight-next-line
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  -- highlight-end
```

すぐに使えるテストケースです。次の設定をconfigに追加する必要があります:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

次に、以下のクエリを実行します:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

<div id="required-parameters-s3">
  #### パラメータ
</div>

| Parameter  | Description                                                             |
| ---------- | ----------------------------------------------------------------------- |
| `type`     | `web`。これ以外の値では、ディスクは作成されません。                                            |
| `endpoint` | `path` 形式のエンドポイント URL。エンドポイント URL には、アップロード先となるデータ保存用のルートパスを含める必要があります。 |

<div id="required-parameters-s3">
  #### パラメータ
</div>

| パラメータ                               | 説明                                | デフォルト値    |
| ----------------------------------- | --------------------------------- | --------- |
| `min_bytes_for_seek`                | 順次読み取りの代わりに seek 操作を使用するための最小バイト数 | `1` MB    |
| `remote_fs_read_backoff_threashold` | リモートディスクからデータを読み取ろうとする際の最大待機時間    | `10000` 秒 |
| `remote_fs_read_backoff_max_tries`  | backoff を伴う読み取りの最大再試行回数           | `5`       |

クエリが例外 `DB:Exception Unreachable URL` により失敗する場合は、[http&#95;connection&#95;timeout](/ja/operations/settings/settings.md/#http_connection_timeout)、[http&#95;receive&#95;timeout](/ja/operations/settings/settings.md/#http_receive_timeout)、[keep&#95;alive&#95;timeout](/ja/operations/server-configuration-parameters/settings#keep_alive_timeout) の調整を試してください。

アップロード用のファイルを取得するには、次を実行します。
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` はクエリ `SELECT data_paths FROM system.tables WHERE name = 'table_name'` で確認できます) 。

`endpoint` でファイルを読み込む場合、ファイルは `<endpoint>/store/` パスに配置する必要がありますが、config には `endpoint` のみを指定する必要があります。

server の起動時に table のロード中、ディスクの読み込み時に URL に到達できない場合は、すべての error が捕捉されます。このとき error が発生していても、table は `DETACH TABLE table_name` -&gt; `ATTACH TABLE table_name` によって再ロードして再び表示できます。server 起動時にメタデータの読み込みが正常に完了していれば、table はすぐに利用可能です。

1 回の HTTP 読み取り中の最大再試行回数を制限するには、[http&#95;max&#95;single&#95;read&#95;retries](/ja/operations/storing-data#web-storage) 設定を使用します。

<div id="zero-copy">
  ### ゼロコピー レプリケーション (本番環境向けではありません)
</div>

ゼロコピー レプリケーションは、`S3` および `HDFS` (未サポート) ディスクで使用できますが、推奨されません。ゼロコピー レプリケーションとは、データが複数のマシン上にリモート保存されており同期が必要な場合に、データ自体ではなくメタデータ (データパーツへのパス) のみがレプリケートされることを意味します。

:::note ゼロコピー レプリケーションは本番環境向けではありません
ClickHouse バージョン 22.8 以降では、ゼロコピー レプリケーションはデフォルトで無効になっています。この機能は本番環境での使用を推奨していません。
:::