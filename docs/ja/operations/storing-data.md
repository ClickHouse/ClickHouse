---
slug: /ja/operations/storing-data
sidebar_position: 68
sidebar_label: "データ保存用の外部ディスク"
title: "データ保存用の外部ディスク"
---

ClickHouseで処理されたデータは、通常、ClickHouseサーバーと同じマシンにあるローカルファイルシステムに保存されます。これは大容量のディスクを必要とし、これらはかなり高価になることがあります。それを避けるために、データをリモートに保存することもできます。様々なストレージがサポートされています：
1. [Amazon S3](https://aws.amazon.com/s3/) オブジェクトストレージ。
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs)。
3. 非サポート: Hadoop 分散ファイルシステム ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

:::note ClickHouseは外部テーブルエンジンもサポートしており、このページで説明する外部ストレージオプションとは異なります。これらは、一般的なファイルフォーマット（例: Parquet）で保存されたデータを読み取ることができますが、このページではClickHouseの`MergeTree`ファミリまたは`Log`ファミリテーブルのストレージ設定を説明しています。
1. `Amazon S3`ディスクに保存されているデータを操作するには、[S3](/docs/ja/engines/table-engines/integrations/s3.md)テーブルエンジンを使用してください。
2. Azure Blob Storageに保存されているデータを操作するには、[AzureBlobStorage](/docs/ja/engines/table-engines/integrations/azureBlobStorage.md)テーブルエンジンを使用してください。
3. 非サポート: Hadoop 分散ファイルシステムのデータを操作するには、[HDFS](/docs/ja/engines/table-engines/integrations/hdfs.md)テーブルエンジンを使用してください。
:::

## 外部ストレージの設定 {#configuring-external-storage}

[MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)と[Log](/docs/ja/engines/table-engines/log-family/log.md)ファミリテーブルエンジンは、`s3`、`azure_blob_storage`、`hdfs` (非サポート)のタイプを持つディスクを使用してデータを`S3`、`AzureBlobStorage`、`HDFS`(非サポート)に保存できます。

ディスク設定には以下が必要です:
1. `type`セクションは`s3`、`azure_blob_storage`、`hdfs`(非サポート)、`local_blob_storage`、`web`のいずれかと等しくする。
2. 特定の外部ストレージタイプの設定。

24.1バージョンのclickhouseからは、新しい設定オプションを使用できるようになりました。
これは以下を指定する必要があります:
1. `type`が`object_storage`と等しいこと
2. `object_storage_type`が`s3`、`azure_blob_storage`（24.3以降は単に`azure`）、`hdfs`（非サポート）、`local_blob_storage`（24.3以降は単に`local`）、`web`のいずれかと等しいこと。オプションで、`metadata_type`を指定できます（デフォルトで`local`と等しい）が、`plain`、`web`、そして24.4以降は`plain_rewritable`に設定することもできます。`plain`メタデータタイプの使用法については、[plain storage section](/docs/ja/operations/storing-data.md/#storing-data-on-webserver)を参照してください。 `web`メタデータタイプは`web`オブジェクトストレージタイプでのみ使用可能で、`local`メタデータタイプはメタデータファイルをローカルに保存します（各メタデータファイルには、オブジェクトストレージ内のファイルへのマッピングとそれらについての追加のメタ情報が含まれます）。

例としての設定オプション
``` xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

は、（24.1以降のバージョンの）設定と等しい：
``` xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

設定
``` xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

は以下と等しい
``` xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

完全なストレージ設定の例は次のようになるでしょう：
``` xml
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

24.1バージョンのclickhouseからは以下のようにも設定できます：
``` xml
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

特定の種類のストレージをすべての`MergeTree`テーブルのデフォルトオプションにするには、 次のセクションを設定ファイルに追加します：

``` xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

特定のストレージポリシーを特定のテーブルにのみ設定したい場合は、テーブルを作成する際に設定で定義できます：

``` sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

`storage_policy`の代わりに`disk`を使用することもできます。この場合、`storage_policy`セクションは設定ファイルに不要で、`disk`セクションだけで十分です。

``` sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

## 動的設定 {#dynamic-configuration}

設定ファイル内にあらかじめ定義されたディスクなしでストレージ設定を指定することも可能ですが、`CREATE`/`ATTACH`クエリ設定に設定できます。

次のクエリア例は、上述の動的ディスク設定を基に構築されており、URLに保存されているテーブルからデータをキャッシュするためにローカルディスクを使用する方法を示しています。

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
  # highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  # highlight-end
```

以下の例では外部ストレージにキャッシュを追加します。

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
  # highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  # highlight-end
```

以下の設定に注目してください。`type=web`のディスクが`type=cache`のディスク内にネストされています。

:::note
例では`type=web`を使用していますが、任意のディスクタイプを動的に設定可能です。ローカルディスクの場合、`custom_local_disks_base_directory`設定パラメータ内で`path`引数が必要です。このデフォルトはありませんので、ローカルディスクを使用する場合はその設定も忘れずに行ってください。
:::

設定に基づく設定とSQL定義された設定の組み合わせも可能です：

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
  # highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  # highlight-end
```

ここで`web`はサーバー設定ファイルから来るものです：

``` xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

### S3ストレージの使用 {#s3-storage}

必要なパラメーター：

- `endpoint` — `path`または`virtual hosted`[スタイル](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html)のS3エンドポイントURL。エンドポイントURLにはデータを保存するバケットとルートパスを含める必要があります。
- `access_key_id` — S3アクセスキーID。
- `secret_access_key` — S3シークレットアクセスキー。

オプションのパラメーター：

- `region` — S3リージョン名。
- `support_batch_delete` — バッチ削除がサポートされているかのチェックを制御します。Google Cloud Storage (GCS)を使用する場合、バッチ削除はサポートされていないため、このチェックを予防することでログにエラーメッセージが表示されないよう`false`に設定します。
- `use_environment_credentials` — 環境変数AWS_ACCESS_KEY_ID、AWS_SECRET_ACCESS_KEY、およびAWS_SESSION_TOKEN（存在する場合）からAWS資格情報を読み取ります。デフォルト値は`false`です。
- `use_insecure_imds_request` — `true`に設定すると、S3クライアントはAmazon EC2メタデータから資格情報を取得する際に非セキュアなIMDS リクエストを使用します。デフォルト値は`false`です。
- `expiration_window_seconds` — 有効期限ベースの資格情報の有効期限を確認するための猶予期間。オプションで、デフォルト値は`120`です。
- `proxy` — S3エンドポイントのためのプロキシ設定。`proxy`ブロック内の各`uri`要素はプロキシURLを含める必要があります。
- `connect_timeout_ms` — ソケット接続タイムアウト（ミリ秒単位）。デフォルト値は`10秒`です。
- `request_timeout_ms` — リクエストタイムアウト（ミリ秒単位）。デフォルト値は`5秒`です。
- `retry_attempts` — リクエストが失敗した場合の再試行回数。デフォルト値は`10`です。
- `single_read_retries` — 読み取り中の接続ドロップ時に再試行する回数。デフォルト値は`4`です。
- `min_bytes_for_seek` — 順次読み取りの代わりにシーク操作を使用する最小バイト数。デフォルト値は`1 Mb`です。
- `metadata_path` — S3のメタデータファイルを保存するローカルFSパス。デフォルト値は`/var/lib/clickhouse/disks/<disk_name>/`です。
- `skip_access_check` — trueの場合、ディスクの起動時にアクセスチェックは実行されません。デフォルト値は`false`です。
- `header` — 指定されたHTTPヘッダーを指定されたエンドポイントへのリクエストに追加します。オプションで、複数回指定可能です。
- `server_side_encryption_customer_key_base64` — 指定されている場合、SSE-C暗号化によるS3オブジェクトへのアクセスに必要なヘッダーが設定されます。
- `server_side_encryption_kms_key_id` - 指定された場合、[SSE-KMS暗号化](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)のために必要なヘッダーが設定されます。空の文字列が指定された場合、AWS管理のS3キーが使用されます。オプションです。
- `server_side_encryption_kms_encryption_context` - `server_side_encryption_kms_key_id`と共に指定された場合、SSE-KMSのための暗号化コンテキストヘッダーが設定されます。オプションです。
- `server_side_encryption_kms_bucket_key_enabled` - `server_side_encryption_kms_key_id`と共に指定された場合、SSE-KMSのためのS3バケットキーを有効にするためのヘッダーが設定されます。オプションで、`true`または`false`を指定でき、デフォルトでは何も指定されていません（バケットレベルの設定に一致します）。
- `s3_max_put_rps` — スロットリング前の最大PUTリクエスト/秒レート。デフォルト値は`0`（無制限）です。
- `s3_max_put_burst` — リクエスト/秒の制限に達する前に同時に発行できる最大リクエスト数。デフォルトでは (`0`値) `s3_max_put_rps`に等しい。
- `s3_max_get_rps` — スロットリング前の最大GETリクエスト/秒レート。デフォルト値は`0`（無制限）です。
- `s3_max_get_burst` — リクエスト/秒の制限に達する前に同時に発行できる最大リクエスト数。デフォルトでは (`0`値) `s3_max_get_rps`に等しい。
- `read_resource` — このディスクへの読み取りリクエストの[スケジューリング](/docs/ja/operations/workload-scheduling.md)に使用されるリソース名。デフォルト値は空文字列（IOスケジューリングはこのディスクに対して有効ではありません）。
- `write_resource` — このディスクへの書き込みリクエストの[スケジューリング](/docs/ja/operations/workload-scheduling.md)に使用されるリソース名。デフォルト値は空文字列（IOスケジューリングはこのディスクに対して有効ではありません）。
- `key_template` — オブジェクトのキーが生成される形式を定義します。デフォルトでは、ClickHouseは`endpoint`オプションの`root path`を取得し、ランダムに生成されたサフィックスを追加します。そのサフィックスは3つのランダム記号を持つディレクトリと29のランダム記号を持つファイル名です。このオプションでは、オブジェクトキーがどのように生成されるかを完全に制御できます。いくつかの使用シナリオでは、プレフィックスまたはオブジェクトキーの中央にランダム記号を保持する必要があります。たとえば、`[a-z]{3}-prefix-random/constant-part/random-middle-[a-z]{3}/random-suffix-[a-z]{29}`。値は[`re2`](https://github.com/google/re2/wiki/Syntax)で解析されます。サフィックスがサポートされているかどうかを確認してください。`key_compatibility_prefix`オプションの定義が必要です。この機能を使用するには、[storage_metadata_write_full_object_key](/docs/ja/operations/settings/settings#storage_metadata_write_full_object_key)の機能フラグを有効にする必要があります。`endpoint`オプションに`root path`を宣言することは禁止されています。`key_compatibility_prefix`のオプションの定義が必要です。
- `key_compatibility_prefix` — `key_template`オプションが使用されている場合、このオプションが必要です。メタデータバージョンが`VERSION_FULL_OBJECT_KEY`より低いメタデータファイルで保存されたオブジェクトキーを読み取ることができるようにするために、以前の`endpoint`オプションの`root path`をここに設定する必要があります。

:::note
Google Cloud Storage (GCS) も`type`を`s3`として使用することでサポートされています。詳細は[GCSバッキングされたMergeTree](/docs/ja/integrations/gcs)を参照してください。
:::

### プレーンストレージの使用 {#plain-storage}

`22.10`から導入された新しいディスクタイプ`s3_plain`は、書き込みのみのストレージを提供します。構成パラメータは`s3`ディスクタイプと同じです。
`s3`ディスクタイプとは異なり、データはそのまま記憶されます。つまり、ランダムに生成されたブロブ名の代わりに通常のファイル名が使用され（clickhouseがローカルディスクにファイルを保存する方法と同じ）、`s3`内のデータから派生したメタデータがローカルに保存されません。

このディスクタイプは、既存のデータに対するマージを実行したり、新しいデータの挿入を許可しないため、テーブルの静的なバージョンを保持することを可能にします。このディスクタイプのユースケースとしては、`BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')` を使用してバックアップを作成し、その後 `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')` または `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'` を使用する方法があります。

設定:
``` xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

`24.1`以降、任意のオブジェクトストレージディスク（`s3`、`azure`、`hdfs`（非サポート）、`local`）を使用して`plain`メタデータタイプを構成することが可能です。

設定:
``` xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

### S3 プレーンリライト可能ストレージの使用 {#s3-plain-rewritable-storage}

新しいディスクタイプ`s3_plain_rewritable`は`24.4`で導入されました。
`s3_plain`ディスクタイプと同様に、追加のメタデータファイルを必要とせず、メタデータはS3に保存されます。
`s3_plain`ディスクタイプとは異なり、`s3_plain_rewritable`はマージの実行を許可し、INSERT操作をサポートします。
ただし、[ミューテーション](/docs/ja/sql-reference/statements/alter#mutations)とレプリケーションはサポートされていません。

このディスクタイプのユースケースとしては、非レプリケート`MergeTree`テーブルがあります。`s3`ディスクタイプも非レプリケート`MergeTree`テーブルに適していますが、このディスクタイプを選択することで、テーブルのローカルメタデータが不要で、制限された操作セットに満足できる場合に役立ちます。これはたとえば、システムテーブルに役立つかもしれません。

設定:
``` xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

これは以下と等しい
``` xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

`24.5`以降、任意のオブジェクトストレージディスク（`s3`、`azure`、`local`）を使って`plain_rewritable`メタデータタイプを構成することが可能です。

### Azure Blob Storage の使用 {#azure-blob-storage}

`MergeTree`ファミリテーブルエンジンは、`azure_blob_storage`タイプのディスクを使用して[Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)にデータを保存することができます。

2022年2月現在、この機能はまだ新しい追加機能なので、Azure Blob Storageの一部の機能が未実装である可能性があります。

設定マークアップ:
``` xml
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

接続パラメータ:
* `storage_account_url` - **必須**、Azure Blob StorageアカウントURL。`http://account.blob.core.windows.net`や`http://azurite1:10000/devstoreaccount1`のような形式。
* `container_name` - 対象のコンテナ名、デフォルトは`default-container`。
* `container_already_exists` - `false`に設定すると、新しいコンテナ`container_name`がストレージアカウントに作成されます。`true`に設定すると、コンテナに直接接続されます。設定されていない場合、ディスクはアカウントに接続してコンテナ`container_name`が存在するかチェックし、まだ存在しない場合は作成します。

認証パラメータ（ディスクは使用可能な全てのメソッド**と**管理対象ID資格情報を試みます）:
* `connection_string` - 接続文字列を用いた認証用。
* `account_name`と`account_key` - 共有キーを用いた認証用。

制限パラメータ（主に内部使用向け）:
* `s3_max_single_part_upload_size` - Blob Storageに単一のブロックアップロードのサイズを制限します。
* `min_bytes_for_seek` - シーク可能な領域のサイズを制限します。
* `max_single_read_retries` - Blob Storageからのデータチャンクの読み取り試行回数を制限します。
* `max_single_download_retries` - Blob Storageからの読み取り可能なバッファのダウンロード試行回数を制限します。
* `thread_pool_size` - `IDiskRemote`がインスタンス化する際のスレッド数を制限します。
* `s3_max_inflight_parts_for_one_file` - 一つのオブジェクトに対して同時に実行可能なputリクエストの数を制限します。

その他のパラメータ:
* `metadata_path` - Blob Storageのメタデータファイルを保存するローカルFSパス。デフォルト値は`/var/lib/clickhouse/disks/<disk_name>/`です。
* `skip_access_check` - trueの場合、ディスクの起動時にアクセスチェックは実行されません。デフォルト値は`false`です。
* `read_resource` — このディスクへの読み取りリクエストの[スケジューリング](/docs/ja/operations/workload-scheduling.md)に使用されるリソース名。デフォルト値は空文字列（IOスケジューリングはこのディスクに対して有効ではありません）。
* `write_resource` — このディスクへの書き込みリクエストの[スケジューリング](/docs/ja/operations/workload-scheduling.md)に使用されるリソース名。デフォルト値は空文字列（IOスケジューリングはこのディスクに対して有効ではありません）。
* `metadata_keep_free_space_bytes` - メタデータディスクに予約される空き領域の量。

動作する設定例は、統合テストディレクトリにあります（例: [test_merge_tree_azure_blob_storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml)または[test_azure_blob_storage_zero_copy_replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)を参照）。

:::note Zero-copy レプリケーションは本番環境には対応していません
Zero-copy レプリケーションはClickHouseバージョン22.8以降でデフォルトで無効になっています。この機能は本番環境での使用を推奨しません。
:::

## HDFS ストレージの使用（非対応）

このサンプル設定では：
- ディスクタイプは`hdfs`（非サポート）
- データは`hdfs://hdfs1:9000/clickhouse/`にホストされています。

ちなみに、HDFSはサポートされていないため、使用時に問題が発生する可能性があります。問題が発生した場合は、修正のためのプルリクエストを自由に行ってください。

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

HDFSは、コーナーケースで動作しない場合があります。

### データ暗号化の使用 {#encrypted-virtual-file-system}

[S3](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)、またはオンプレミスディスクに保存されたデータを暗号化することができます。暗号化モードをオンにするには、設定ファイルで`encrypted`タイプのディスクを定義し、データが保存されるディスクを選択する必要があります。`encrypted`ディスクは書き込み中にすべてのファイルをオンザフライで暗号化し、ファイルを読む際には自動的に復号します。したがって、通常のディスクと同様に`encrypted`ディスクを操作できます。

ディスク設定の例：

``` xml
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

例えば、ClickHouseがあるテーブルからファイル`store/all_1_1_0/data.bin`を`disk1`に書き込む場合、実際にはこのファイルは物理ディスクにパス`/path1/store/all_1_1_0/data.bin`として書き込まれます。

同じファイルを`disk2`に書き込む際には、実際には物理ディスクにパス`/path1/path2/store/all_1_1_0/data.bin`として暗号化モードで書き込まれます。

必須パラメータ：

- `type` — `encrypted`。暗号化されたディスクを作成するにはこれが必要です。
- `disk` — データを保存するためのディスクのタイプ。
- `key` — 暗号化と復号化のためのキー。タイプ: [Uint64](/docs/ja/sql-reference/data-types/int-uint.md)。キーを16進形式でエンコードするためには`key_hex`パラメータを使用できます。
keyを複数指定する場合、`id`属性を使用して識別できます（下記の例を参照）。

オプションパラメータ：

- `path` — ディスク上でデータが保存される場所へのパス。指定されていない場合は、データはルートディレクトリに保存されます。
- `current_key_id` — 暗号化に使用されるキーです。指定されたすべてのキーは復号化に使用でき、以前に暗号化されたデータへのアクセスを維持しつつ、いつでも別のキーに切り替えられます。
- `algorithm` — 暗号化に使用する[アルゴリズム](/docs/ja/sql-reference/statements/create/table.md/#create-query-encryption-codecs)。可能な値：`AES_128_CTR`、`AES_192_CTR`、または`AES_256_CTR`。デフォルト値：`AES_128_CTR`。アルゴリズムによってキーの長さは異なります：`AES_128_CTR` — 16バイト、`AES_192_CTR` — 24バイト、`AES_256_CTR` — 32バイト。

ディスク設定の例：

``` xml
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
                <key_hex id="1">ffeeddccbbaaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

### ローカルキャッシュの使用 {#using-local-cache}

バージョン22.3以降、ストレージ設定内でディスクに対してローカルキャッシュを設定することが可能です。
バージョン22.3から22.7の間は、`s3`ディスクタイプに対してのみキャッシュがサポートされています。バージョン>=22.8では、任意のディスクタイプ（S3、Azure、ローカル、暗号化など）に対してキャッシュがサポートされています。
バージョン>=23.5では、リモートディスクタイプ（S3、Azure、HDFS 非対応）のみキャッシュがサポートされています。
キャッシュは`LRU`キャッシュポリシーを使用しています。


次にバージョン22.8以降の構成例です：

``` xml
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

バージョン22.8以前の構成例：

``` xml
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

ファイルキャッシュ**ディスク設定の設定**：

これらの設定は、ディスク設定セクションで定義する必要があります。

- `path` - キャッシュを保存するディレクトリへのパス。デフォルト：None、この設定は必須です。

- `max_size` - バイト単位または読み取り可能な形式でのキャッシュの最大サイズ（例：`ki, Mi, Gi`など、例`10Gi`（この形式はバージョン`22.10`以降で使用可能））。制限に達した場合、キャッシュファイルはキャッシュエビクションポリシーに従って削除されます。デフォルト：None、この設定は必須です。

- `cache_on_write_operations` - `write-through`キャッシュ（すべての書き込み操作（`INSERT`クエリ、バックグラウンドマージ）でデータがキャッシュされます）をオンにすることができます。デフォルト：`false`です。`write-through`キャッシュは、設定`enable_filesystem_cache_on_write_operations`を使用してクエリごとに無効にすることができます（データは、キャッシュ設定と対応するクエリ設定が両方とも有効な場合にのみキャッシュされます）。

- `enable_filesystem_query_cache_limit` - 各クエリ内でダウンロードされたキャッシュのサイズを制限することを許可します（ユーザー設定`max_query_cache_size`に依存します）。デフォルト：`false`です。

- `enable_cache_hits_threshold` - データがキャッシュされる前に必要な読み取り回数を定義します。デフォルト：`false`です。このしきい値は、`cache_hits_threshold`によって定義できます。デフォルト：`0`、つまり最初の試行でデータがキャッシュされます。

- `enable_bypass_cache_with_threshold` - 要求された読み取り範囲がしきい値を超えた場合にキャッシュを完全にスキップすることを許可します。デフォルト：`false`です。このしきい値は、`bypass_cache_threashold`によって定義できます。デフォルト：`268435456`（`256Mi`）。

- `max_file_segment_size` - 単一キャッシュファイルの最大サイズ（バイト単位または読み取り可能な形式（`ki, Mi, Gi`など、例`10Gi`））。デフォルト：`8388608`（`8Mi`）。

- `max_elements` - キャッシュファイルの数の制限。デフォルト：`10000000`。

- `load_metadata_threads` - 起動時にキャッシュメタデータをロードするために使用されるスレッドの数。デフォルト：`16`.

ファイルキャッシュ**クエリ/プロファイル設定**：

これらの設定の一部は、デフォルトまたはディスク設定の設定で有効にされているキャッシュ機能をクエリ/プロファイルごとに無効にします。 たとえば、キャッシュをディスク設定で有効にし、クエリ/プロファイル設定を`enable_filesystem_cache`として`false`に設定してクエリごとに無効にすることができます。また、 `write-through`キャッシュが有効であることを意味します。特定のクエリごとにこの一般的な設定を無効にする必要がある場合は、`enable_filesystem_cache_on_write_operations`を`false`にすることができます。

- `enable_filesystem_cache` - クエリごとにディスクタイプが`cache`で構成されているストレージポリシーの場合でもキャッシュを無効にすることを許可します。デフォルト：`true`。

- `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` - 既に存在する場合にのみクエリでキャッシュを利用し、それ以外の場合はクエリデータがローカルキャッシュストレージに書き込まれないようにすることを許可します。デフォルト：`false`。

- `enable_filesystem_cache_on_write_operations` - `write-through`キャッシュをオンにします。この設定は、キャッシュ設定で`cache_on_write_operations`がオンになっている場合にのみ機能します。デフォルト：`false`。クラウドのデフォルト値：`true`。

- `enable_filesystem_cache_log` - `system.filesystem_cache_log`テーブルへのログ記録をオンにします。クエリごとに、またはプロファイル内で有効にすることができます。デフォルト：`false`。

- `max_query_cache_size` - ローカルキャッシュストレージに書き込める最大キャッシュサイズの制限。キャッシュ設定で`enable_filesystem_query_cache_limit`が有効な場合にのみ機能します。デフォルト：`false`。

- `skip_download_if_exceeds_query_cache` - `max_query_cache_size`設定の動作を変更します。デフォルト：`true`。この設定がオンで、クエリ中にキャッシュダウンロード制限に達した場合、キャッシュはこれ以上ダウンロードされません。この設定がオフで、クエリ中にキャッシュダウンロード制限が達した場合、キャッシュはそれでも現在のクエリ内でダウンロードされたデータを以前にエビクトすることで書き込まれます。つまり、2番目の動作はクエリキャッシュ制限を維持しながら、`last recently used`の振る舞いを維持します。

**警告**
キャッシュ設定の設定とキャッシュクエリ設定は最新のClickHouseバージョンに対応していますが、以前のバージョンでは何かがサポートされていない可能性があります。

キャッシュ**システムテーブル**：

- `system.filesystem_cache` - キャッシュの現在の状況を表示するシステムテーブル。

- `system.filesystem_cache_log` - クエリごとのキャッシュ使用の詳細を表示するシステムテーブル。`enable_filesystem_cache_log`設定が`true`であることが必要です。

キャッシュ**コマンド**：

- `SYSTEM DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER`は`<cache_name>`が指定されていない時のみサポートされます

- `SHOW FILESYSTEM CACHES` -- サーバーで構成されているファイルシステムキャッシュのリストを表示します。(バージョン<= `22.8`では`SHOW CACHES`というコマンド名が使用されます)

```sql
SHOW FILESYSTEM CACHES
```

結果：

``` text
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

- `DESCRIBE FILESYSTEM CACHE '<cache_name>'` - 特定のキャッシュの構成といくつかの一般的な統計を表示します。キャッシュ名は`SHOW FILESYSTEM CACHES`コマンドから取得できます。(バージョン<= `22.8`では`DESCRIBE CACHE`というコマンド名が使用されます)

```sql
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

``` text
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

キャッシュの現在のメトリクス：

- `FilesystemCacheSize`

- `FilesystemCacheElements`

キャッシュの非同期メトリクス：

- `FilesystemCacheBytes`

- `FilesystemCacheFiles`

キャッシュプロファイルイベント：

- `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes,`

- `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds`

- `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`

- `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`

### 静的Webストレージの使用（読み取り専用） {#web-storage}

これは読み取り専用ディスクです。そのデータは読み取られるだけで、変更されることはありません。新しいテーブルは `ATTACH TABLE` クエリを介してこのディスクにロードされます（以下の例を参照）。ローカルディスクは実際には使用されず、各`SELECT`クエリは必要なデータを取得するための`http`リクエストを発生させます。テーブルデータの変更はすべて例外となり、以下のタイプのクエリは許可されません：[CREATE TABLE](/docs/ja/sql-reference/statements/create/table.md), [ALTER TABLE](/docs/ja/sql-reference/statements/alter/index.md), [RENAME TABLE](/docs/ja/sql-reference/statements/rename.md/#misc_operations-rename_table), [DETACH TABLE](/docs/ja/sql-reference/statements/detach.md), [TRUNCATE TABLE](/docs/ja/sql-reference/statements/truncate.md)。
ウェブストレージは読み取り専用目的で使用できます。例として、サンプルデータのホスティングやデータの移行があります。
データディレクトリを特定のテーブル（`SELECT data_paths FROM system.tables WHERE name = 'table_name'`）に対して準備するツール`clickhouse-static-files-uploader`があります。必要な各テーブルについて、ファイルのディレクトリが得られます。これらのファイルは、たとえば静的ファイルを持つウェブサーバーにアップロードできます。この準備の後、任意のClickHouseサーバにこのテーブルを`DiskWeb`経由でロードすることができます。

このサンプル構成では：
- ディスクタイプは`web`
- データは`http://nginx:80/test1/`にホストされています
- ローカルストレージにキャッシュが使用されます

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
通常使用されないWebデータセットの場合、クエリ内で一時的にストレージを設定できるので、設定ファイルを編集する手間が省けます。詳細は[dynamic configuration](#dynamic-configuration)を参照。
:::

:::tip
デモデータセットがGitHubにホストされています。自分のテーブルをWebストレージに準備する方法については、ツール[clickhouse-static-files-uploader](/docs/ja/operations/storing-data.md/#storing-data-on-webserver)を参照してください。
:::

この`ATTACH TABLE`クエリでは、指定された`UUID`がデータのディレクトリ名に一致し、エンドポイントは生のGitHubコンテンツのURLです。

```sql
# highlight-next-line
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
  # highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  # highlight-end
```

すぐにテストケースを準備します。この設定をコンフィグに追加する必要があります：

``` xml
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

次にこのクエリを実行します：

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

必須パラメーター：

- `type` — `web`。これ以外ではディスクは作成されません。
- `endpoint` — `path`形式でのエンドポイントURL。エンドポイントURLには、データがアップロードされたルートパスを含める必要があります。

オプションパラメーター：

- `min_bytes_for_seek` — シーク操作を使用する最小バイト数。デフォルト値：`1` Mb。
- `remote_fs_read_backoff_threashold` — リモートディスクのデータを読み取る際の最大待機時間。デフォルト値：`10000`秒。
- `remote_fs_read_backoff_max_tries` — バックオフの最大リトライ回数。デフォルト値：`5`。

クエリが`DB:Exception Unreachable URL`の例外で失敗した場合は、設定を調整してみてください:[http_connection_timeout](/docs/ja/operations/settings/settings.md/#http_connection_timeout), [http_receive_timeout](/docs/ja/operations/settings/settings.md/#http_receive_timeout), [keep_alive_timeout](/docs/ja/operations/server-configuration-parameters/settings.md/#keep-alive-timeout)。

アップロードするファイルを取得するには、以下を実行します：
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>`（`--metadata-path` はクエリ`SELECT data_paths FROM system.tables WHERE name = 'table_name'`で確認できます）。

ロードする際にはファイルは`<endpoint>/store/`パスにアップロードされる必要がありますが、コンフィグには`endpoint`だけを含める必要があります。

ディスクロード時にURLに到達できない場合、つまりサーバが起動してテーブルを開始する時に、すべてのエラーがキャッチされます。この場合、テーブルは再ロード（可視化）されることがあります。`DETACH TABLE table_name` -> `ATTACH TABLE table_name`。サーバ起動時にメタデータが正常にロードされた場合、テーブルはすぐに利用可能です。

設定が`HTTP接続の最大再試行`で制限されている場合、単一のHTTP読み取り中の最大再試行回数を制限するためにこの設定を使用してください。[http_max_single_read_retries](/docs/ja/operations/settings/settings.md/#http-max-single-read-retries)。

### ゼロコピー レプリケーション（生産環境には不適） {#zero-copy}

ゼロコピー レプリケーションは、`S3`および`HDFS`（非対応）ディスクで可能ですが、推奨されていません。ゼロコピー レプリケーションとは、データが複数のマシン上のリモートに保存されていて同期する必要がある場合、データそのものではなく、メタデータ（データ部のパス）だけがレプリケートされることを意味します。

:::note ゼロコピー レプリケーションは、本番環境には不向きです
ゼロコピー レプリケーションは、ClickHouseバージョン22.8以降でデフォルトで無効になっています。この機能は、本番環境での使用を推奨しません。
:::

