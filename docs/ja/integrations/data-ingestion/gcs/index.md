---
sidebar_label: Google Cloud Storage (GCS)
sidebar_position: 4
slug: /ja/integrations/gcs
description: "Google Cloud Storage (GCS) を使用する MergeTree"
---

import BucketDetails from '@site/docs/ja/_snippets/_GCS_authentication_and_bucket.md';

# ClickHouseとGoogle Cloud Storageの統合

:::note
ClickHouse Cloudを[Google Cloud](https://cloud.google.com)で使用している場合、このページは適用されません。サービスはすでに[Google Cloud Storage](https://cloud.google.com/storage)を使用しています。GCSからデータを`SELECT`または`INSERT`する方法をお探しの場合は、[`gcs`テーブル関数](/ja/sql-reference/table-functions/gcs)をご参照ください。
:::

ClickHouseは、ストレージと計算を分離することを求めるユーザーにとって、GCSが魅力的なストレージソリューションであることを認識しています。これを達成するために、MergeTreeエンジンのストレージとしてGCSを使用するサポートが提供されています。これにより、ユーザーはGCSのスケーラビリティとコストメリット、およびMergeTreeエンジンの挿入とクエリのパフォーマンスを活用することができます。

## GCSバックのMergeTree

### ディスクの作成

GCSバケットをディスクとして利用するためには、まずClickHouseの設定ファイル内で`conf.d`に宣言する必要があります。以下にGCSディスクの宣言例を示します。この設定には、GCS "ディスク"、キャッシュ、およびDDLクエリでテーブルをGCSディスク上に作成する際に指定されるポリシーを設定するための複数のセクションが含まれています。それぞれの説明を以下に示します。

#### storage_configuration > disks > gcs

この設定の該当部分は、以下のように示されています。
- バッチ削除は行われません。GCSは現在バッチ削除をサポートしていないため、エラーメッセージを抑制するために自動検出が無効となっています。
- ディスクのタイプは`type`が`s3`であり、S3 APIが使用されています。
- GCSによって提供されるエンドポイント
- サービスアカウントのHMACキーおよびシークレット
- ローカルディスク上のメタデータパス

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <gcs>
	    <!--highlight-start-->
                <support_batch_delete>false</support_batch_delete>
                <type>s3</type>
                <endpoint>https://storage.googleapis.com/BUCKET NAME/FOLDER NAME/</endpoint>
                <access_key_id>SERVICE ACCOUNT HMAC KEY</access_key_id>
                <secret_access_key>SERVICE ACCOUNT HMAC SECRET</secret_access_key>
                <metadata_path>/var/lib/clickhouse/disks/gcs/</metadata_path>
	    <!--highlight-end-->
            </gcs>
        </disks>
        <policies>
            <gcs_main>
                <volumes>
                    <main>
                        <disk>gcs</disk>
                    </main>
                </volumes>
            </gcs_main>
        </policies>
    </storage_configuration>
</clickhouse>
```

#### storage_configuration > disks > cache

次に示す設定例では、ディスク`gcs`に対して10Giのメモリキャッシュを有効にしています。

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <gcs>
                <support_batch_delete>false</support_batch_delete>
                <type>s3</type>
                <endpoint>https://storage.googleapis.com/BUCKET NAME/FOLDER NAME/</endpoint>
                <access_key_id>SERVICE ACCOUNT HMAC KEY</access_key_id>
                <secret_access_key>SERVICE ACCOUNT HMAC SECRET</secret_access_key>
                <metadata_path>/var/lib/clickhouse/disks/gcs/</metadata_path>
            </gcs>
	    <!--highlight-start-->
	    <gcs_cache>
                <type>cache</type>
                <disk>gcs</disk>
                <path>/var/lib/clickhouse/disks/gcs_cache/</path>
                <max_size>10Gi</max_size>
            </gcs_cache>
	    <!--highlight-end-->
        </disks>
        <policies>
            <gcs_main>
                <volumes>
                    <main>
                        <disk>gcs_cache</disk>
                    </main>
                </volumes>
            </gcs_main>
        </policies>
    </storage_configuration>
</clickhouse>
```

#### storage_configuration > policies > gcs_main

ストレージ構成ポリシーは、データがどこに保存されるかを選択できるようにします。以下に示すポリシーは、`gcs_main`というポリシーを指定することで、データをディスク`gcs`に保存できるようにしています。たとえば、`CREATE TABLE ... SETTINGS storage_policy='gcs_main'`のように。

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <gcs>
                <support_batch_delete>false</support_batch_delete>
                <type>s3</type>
                <endpoint>https://storage.googleapis.com/BUCKET NAME/FOLDER NAME/</endpoint>
                <access_key_id>SERVICE ACCOUNT HMAC KEY</access_key_id>
                <secret_access_key>SERVICE ACCOUNT HMAC SECRET</secret_access_key>
                <metadata_path>/var/lib/clickhouse/disks/gcs/</metadata_path>
            </gcs>
        </disks>
        <policies>
	    <!--highlight-start-->
            <gcs_main>
                <volumes>
                    <main>
                        <disk>gcs</disk>
                    </main>
                </volumes>
            </gcs_main>
	    <!--highlight-end-->
        </policies>
    </storage_configuration>
</clickhouse>
```

このディスク宣言に関連する設定の完全なリストは[こちら](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)で確認できます。

### テーブルの作成

書き込み権限のあるバケットを使用するようにディスクを設定した場合、下記の例のようにテーブルを作成できるはずです。簡潔にするために、NYCタクシーのカラムの一部を使用し、データをGCSバックのテーブルに直接ストリームします：

```sql
CREATE TABLE trips_gcs
(
   `trip_id` UInt32,
   `pickup_date` Date,
   `pickup_datetime` DateTime,
   `dropoff_datetime` DateTime,
   `pickup_longitude` Float64,
   `pickup_latitude` Float64,
   `dropoff_longitude` Float64,
   `dropoff_latitude` Float64,
   `passenger_count` UInt8,
   `trip_distance` Float64,
   `tip_amount` Float32,
   `total_amount` Float32,
   `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY pickup_datetime
# highlight-next-line
SETTINGS storage_policy='gcs_main'
```

```sql
INSERT INTO trips_gcs SELECT trip_id, pickup_date, pickup_datetime, dropoff_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, passenger_count, trip_distance, tip_amount, total_amount, payment_type FROM s3('https://ch-nyc-taxi.s3.eu-west-3.amazonaws.com/tsv/trips_{0..9}.tsv.gz', 'TabSeparatedWithNames') LIMIT 1000000;
```

ハードウェアによっては、この後の1m行の挿入に数分かかる場合があります。進行状況はsystem.processesテーブルで確認できます。行数を1000万の制限まで調整して、いくつかのサンプルクエリを試してみてください。

```sql
SELECT passenger_count, avg(tip_amount) as avg_tip, avg(total_amount) as avg_amount FROM trips_gcs GROUP BY passenger_count;
```

### レプリケーションの処理

GCSディスクを使用したレプリケーションは、`ReplicatedMergeTree`テーブルエンジンを使用することで実現できます。詳しくは、[GCSを使用して2つのGCPリージョンにわたって単一シャードをレプリケートする](#gcs-multi-region)ガイドをご覧ください。

### 詳しく学ぶ

[Cloud Storage XML API](https://cloud.google.com/storage/docs/xml-api/overview)は、Amazon Simple Storage Service（Amazon S3）などのサービスで動作するツールやライブラリと相互運用可能です。

スレッド調整に関する詳細情報は、[パフォーマンスの最適化](../s3/index.md#s3-optimizing-performance)をご覧ください。

## Google Cloud Storage (GCS) の使用 {#gcs-multi-region}

:::tip
オブジェクトストレージは既にClickHouse Cloudでデフォルトで使用されているため、ClickHouse Cloudで実行している場合はこの手順を実行する必要はありません。
:::

### デプロイの計画

このチュートリアルは、Google Cloudで動作し、Google Cloud Storage (GCS) をClickHouseストレージディスクの "タイプ"として使用する、レプリケートされたClickHouseのデプロイメントを説明するために書かれています。

このチュートリアルでは、Google Cloud EngineのVMにClickHouseサーバーノードをデプロイし、それぞれに関連するGCSバケットをストレージとして持ちます。レプリケーションは、VMとしてデプロイされた一連のClickHouse Keeperノードによって調整されます。

高可用性のためのサンプル要件：
- 2つのGCPリージョンに2つのClickHouseサーバーノード
- 同じリージョンに配置された2つのGCSバケット
- ClickHouseサーバーノードと同じリージョンに2つを含む3つのClickHouse Keeperノード。三つ目は最初の2つのKeeperノードのうちの1つと同じリージョンにできますが、異なるアベイラビリティゾーンに配置してください。

ClickHouse Keeperは機能するために2つのノードを必要とするため、高可用性のために3つのノードが必要です。

### VMの準備

3つのリージョンに5つのVMをデプロイします：

| リージョン | ClickHouseサーバー | バケット             | ClickHouse Keeper |
|-----------|--------------------|---------------------|------------------|
| 1         | chnode1            | bucket_regionname   | keepernode1      |
| 2         | chnode2            | bucket_regionname   | keepernode2      |
| 3 `*`     |                    |                     | keepernode3      |

`*` これは、上記の1または2のリージョンと同じリージョンの異なるアベイラビリティゾーンにできます。

#### ClickHouseのデプロイ

2つのホストにClickHouseをデプロイします。このサンプル設定では、これらは`chnode1`、`chnode2`と名付けられています。

`chnode1`を1つのGCPリージョンに、`chnode2`をもう1つのリージョンに置いてください。このガイドでは、`us-east1`と`us-east4`をコンピュートエンジンVM、GCSバケットのために使用します。

:::note
`clickhouse server`を設定するまで開始しないでください。インストールするだけにしてください。
:::

ClickHouseサーバーノードのデプロイ手順を実行する際は、[インストール手順](/docs/ja/getting-started/install.md/#available-installation-options)を参照してください。

#### ClickHouse Keeperのデプロイ

3つのホストにClickHouse Keeperをデプロイします。このサンプル設定では、これらは`keepernode1`、`keepernode2`、`keepernode3`と名付けられています。`keepernode1`は`chnode1`と同じリージョンに、`keepernode2`は`chnode2`と同じリージョンに、`keepernode3`は任意のリージョンに配置できますが、そのリージョンのClickHouseノードとは異なるアベイラビリティゾーンに配置してください。

ClickHouse Keeperノードのデプロイ手順を実行する際は、[インストール手順](/docs/ja/getting-started/install.md/#install-standalone-clickhouse-keeper)を参照してください。

### 2つのバケットを作成する

高可用性のために、2つのClickHouseサーバーは異なるリージョンに配置されます。各サーバーは同じリージョンにGCSバケットを持ちます。

**Cloud Storage > Buckets**で**CREATE BUCKET**を選択します。このチュートリアルでは、`us-east1`と`us-east4`に1つずつ2つのバケットを作成します。これらのバケットは単一リージョン、スタンダードストレージクラスで、公開されていません。プロンプトが表示されたら、公開アクセスの防止を有効にします。フォルダーは作成しないでください。ClickHouseがストレージに書き込んだときに作成されます。

バケットとHMACキーを作成するためのステップバイステップの手順が必要な場合は、**Create GCS buckets and an HMAC key**を展開して、指示に従ってください。

<BucketDetails />

### ClickHouse Keeperの設定

すべてのClickHouse Keeperノードは、次に示すファイルと同じ設定ファイルを持ちますが、`server_id`行（最初のハイライト行）だけが異なります。ファイルを開いて、ClickHouse Keeperサーバーのホスト名を編集し、それぞれのサーバーで`server_id`を、`raft_configuration`の適切な`server`エントリに合わせて設定します。例では`server_id`が`3`に設定されているため、`raft_configuration`の一致する行をハイライトしています。

- ファイルをホスト名で編集し、ClickHouseサーバーノードとKeeperノードから解決できることを確認してください
- ファイルを所定の場所（各Keeperサーバーの`/etc/clickhouse-keeper/keeper_config.xml`）にコピー
- それぞれのマシンで、`server_id`をエントリ番号に基づいて編集

```xml title=/etc/clickhouse-keeper/keeper_config.xml
<clickhouse>
    <logger>
        <level>trace</level>
        <log>/var/log/clickhouse-keeper/clickhouse-keeper.log</log>
        <errorlog>/var/log/clickhouse-keeper/clickhouse-keeper.err.log</errorlog>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <listen_host>0.0.0.0</listen_host>
    <keeper_server>
        <tcp_port>9181</tcp_port>
<!--highlight-next-line-->
        <server_id>3</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>keepernode1.us-east1-b.c.clickhousegcs-374921.internal</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>keepernode2.us-east4-c.c.clickhousegcs-374921.internal</hostname>
                <port>9234</port>
            </server>
<!--highlight-start-->
            <server>
                <id>3</id>
                <hostname>keepernode3.us-east5-a.c.clickhousegcs-374921.internal</hostname>
                <port>9234</port>
            </server>
<!--highlight-end-->
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

### ClickHouseサーバーの設定

:::note ベストプラクティス
このガイドのいくつかのステップでは、構成ファイルを`/etc/clickhouse-server/config.d/`に配置するように求めます。これはLinuxシステムのデフォルトの場所で、構成上書きファイルのためのものです。このディレクトリにファイルを置くと、デフォルトの構成とコンテンツがマージされます。これらのファイルを`config.d`ディレクトリに配置すると、アップグレード中に構成を失うことを避けることができます。
:::

#### ネットワーク

デフォルトでは、ClickHouseはループバックインターフェースをリッスンしますが、複製されたセットアップではマシン間のネットワークが必要です。すべてのインターフェースでリッスンします：

```xml title=/etc/clickhouse-server/config.d/network.xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
</clickhouse>
```

#### リモートClickHouse Keeperサーバー

レプリケーションはClickHouse Keeperによって調整されます。この構成ファイルはClickHouse Keeperノードのホスト名とポート番号を識別します。

- ホスト名を編集してKeeperホストと一致させます

```xml title=/etc/clickhouse-server/config.d/use-keeper.xml
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>keepernode1.us-east1-b.c.clickhousegcs-374921.internal</host>
            <port>9181</port>
        </node>
        <node index="2">
            <host>keepernode2.us-east4-c.c.clickhousegcs-374921.internal</host>
            <port>9181</port>
        </node>
        <node index="3">
            <host>keepernode3.us-east5-a.c.clickhousegcs-374921.internal</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

#### リモートClickHouseサーバー

このファイルは、クラスター内の各ClickHouseサーバーのホスト名とポートを設定します。 デフォルトの構成ファイルにはサンプルのクラスター定義が含まれているため、完全に設定されたクラスターのみを示すために、タグ`replace="true"`を`remote_servers`エントリに追加して、クリックしてコンフィグレーションをデフォルトとマージするときに`remote_servers`セクションを置き換えるようにします。

- ファイルを編集してホスト名を追加し、ClickHouseサーバーノードから解決できることを確認してください

```xml title=/etc/clickhouse-server/config.d/remote-servers.xml
<clickhouse>
    <remote_servers replace="true">
        <cluster_1S_2R>
            <shard>
                <replica>
                    <host>chnode1.us-east1-b.c.clickhousegcs-374921.internal</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>chnode2.us-east4-c.c.clickhousegcs-374921.internal</host>
                    <port>9000</port>
                </replica>
            </shard>
        </cluster_1S_2R>
    </remote_servers>
</clickhouse>
```

#### レプリカの識別

このファイルは、ClickHouse Keeperパスに関連する設定を行います。 特に、データがどのレプリカに属しているかを特定するために使用されるマクロです。 1つのサーバーにはレプリカ`replica_1`として指定し、もう1つのサーバーには`replica_2`と指定します。 名称は変更できますが、例では、1つのレプリカがサウスカロライナに保存され、もう1つが北バージニアに保存され、`carolina`と`virginia`のように異なる値にしてください。

```xml title=/etc/clickhouse-server/config.d/macros.xml
<clickhouse>
    <distributed_ddl>
            <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <macros>
        <cluster>cluster_1S_2R</cluster>
        <shard>1</shard>
<!--highlight-next-line-->
        <replica>replica_1</replica>
    </macros>
</clickhouse>
```

#### GCSでのストレージ

ClickHouseのストレージ構成には、`disks`と`policies`が含まれます。 下記の設定では、ディスク`gcs`の`type`は`s3`に指定されています。 タイプがs3というのは、ClickHouseがGCSバケットへAWS S3バケットとしてアクセスしているためです。 この構成の2つのコピーが必要で、それぞれのClickHouseサーバーノード用です。

以下の構成でこれらの置換を行う必要があります。

これらの置換は、2つのClickHouseサーバーノード間で異なります：
- `REPLICA 1 BUCKET`は、サーバーと同じリージョンにあるバケットの名前に設定
- `REPLICA 1 FOLDER`は、1つのサーバーに`replica_1`、もう1つに`replica_2`に変更

これらの置換は、2つのノード間で同一：
- `access_key_id`は、前に生成されたHMACキーに設定
- `secret_access_key`は、前に生成されたHMAC秘密情報に設定

```xml title=/etc/clickhouse-server/config.d/storage.xml
<clickhouse>
    <storage_configuration>
        <disks>
            <gcs>
                <support_batch_delete>false</support_batch_delete>
                <type>s3</type>
                <endpoint>https://storage.googleapis.com/REPLICA 1 BUCKET/REPLICA 1 FOLDER/</endpoint>
                <access_key_id>SERVICE ACCOUNT HMAC KEY</access_key_id>
                <secret_access_key>SERVICE ACCOUNT HMAC SECRET</secret_access_key>
                <metadata_path>/var/lib/clickhouse/disks/gcs/</metadata_path>
            </gcs>
	    <cache>
                <type>cache</type>
                <disk>gcs</disk>
                <path>/var/lib/clickhouse/disks/gcs_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <gcs_main>
                <volumes>
                    <main>
                        <disk>gcs</disk>
                    </main>
                </volumes>
            </gcs_main>
        </policies>
    </storage_configuration>
</clickhouse>
```

### ClickHouse Keeperの開始

お使いのオペレーティングシステムのコマンドを使用します。たとえば：

```bash
sudo systemctl enable clickhouse-keeper
sudo systemctl start clickhouse-keeper
sudo systemctl status clickhouse-keeper
```

#### ClickHouse Keeperのステータスを確認

`netcat`を使用してClickHouse Keeperにコマンドを送信します。たとえば、`mntr`はClickHouse Keeperクラスターの状態を返します。 コマンドを各Keeperノードで実行すると、1つはリーダーで、他の2つはフォロワーであることがわかります。

```bash
echo mntr | nc localhost 9181
```
```response
zk_version	v22.7.2.15-stable-f843089624e8dd3ff7927b8a125cf3a7a769c069
zk_avg_latency	0
zk_max_latency	11
zk_min_latency	0
zk_packets_received	1783
zk_packets_sent	1783
# highlight-start
zk_num_alive_connections	2
zk_outstanding_requests	0
zk_server_state	leader
# highlight-end
zk_znode_count	135
zk_watch_count	8
zk_ephemerals_count	3
zk_approximate_data_size	42533
zk_key_arena_size	28672
zk_latest_snapshot_size	0
zk_open_file_descriptor_count	182
zk_max_file_descriptor_count	18446744073709551615
# highlight-start
zk_followers	2
zk_synced_followers	2
# highlight-end
```
### ClickHouseサーバーの開始

`chnode1`と`chnode2`で次のコマンドを実行します：
```bash
sudo service clickhouse-server start
```
```bash
sudo service clickhouse-server status
```

### 検証

#### ディスクの構成を確認

`system.disks`には、それぞれのディスクに対するレコードが含まれている必要があります：
- デフォルト
- gcs
- キャッシュ

```sql
SELECT *
FROM system.disks
FORMAT Vertical
```
```response
Row 1:
──────
name:             cache
path:             /var/lib/clickhouse/disks/gcs/
free_space:       18446744073709551615
total_space:      18446744073709551615
unreserved_space: 18446744073709551615
keep_free_space:  0
type:             s3
is_encrypted:     0
is_read_only:     0
is_write_once:    0
is_remote:        1
is_broken:        0
cache_path:       /var/lib/clickhouse/disks/gcs_cache/

Row 2:
──────
name:             default
path:             /var/lib/clickhouse/
free_space:       6555529216
total_space:      10331889664
unreserved_space: 6555529216
keep_free_space:  0
type:             local
is_encrypted:     0
is_read_only:     0
is_write_once:    0
is_remote:        0
is_broken:        0
cache_path:

Row 3:
──────
name:             gcs
path:             /var/lib/clickhouse/disks/gcs/
free_space:       18446744073709551615
total_space:      18446744073709551615
unreserved_space: 18446744073709551615
keep_free_space:  0
type:             s3
is_encrypted:     0
is_read_only:     0
is_write_once:    0
is_remote:        1
is_broken:        0
cache_path:

3 rows in set. Elapsed: 0.002 sec.
```

#### クラスタに作成されたテーブルが両方のノードに作成されたことを確認
```sql
# highlight-next-line
create table trips on cluster 'cluster_1S_2R' (
 `trip_id` UInt32,
 `pickup_date` Date,
 `pickup_datetime` DateTime,
 `dropoff_datetime` DateTime,
 `pickup_longitude` Float64,
 `pickup_latitude` Float64,
 `dropoff_longitude` Float64,
 `dropoff_latitude` Float64,
 `passenger_count` UInt8,
 `trip_distance` Float64,
 `tip_amount` Float32,
 `total_amount` Float32,
 `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4))
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY pickup_datetime
# highlight-next-line
SETTINGS storage_policy='gcs_main'
```
```response
┌─host───────────────────────────────────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode2.us-east4-c.c.gcsqa-375100.internal │ 9000 │      0 │       │                   1 │                1 │
└────────────────────────────────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
┌─host───────────────────────────────────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode1.us-east1-b.c.gcsqa-375100.internal │ 9000 │      0 │       │                   0 │                0 │
└────────────────────────────────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘

2 rows in set. Elapsed: 0.641 sec.
```

#### データが挿入できることを確認

```sql
INSERT INTO trips SELECT
    trip_id,
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    trip_distance,
    tip_amount,
    total_amount,
    payment_type
FROM s3('https://ch-nyc-taxi.s3.eu-west-3.amazonaws.com/tsv/trips_{0..9}.tsv.gz', 'TabSeparatedWithNames')
LIMIT 1000000
```

#### テーブル用にストレージポリシー`gcs_main`が使用されていることを確認
```sql
SELECT
    engine,
    data_paths,
    metadata_path,
    storage_policy,
    formatReadableSize(total_bytes)
FROM system.tables
WHERE name = 'trips'
FORMAT Vertical
```
```response
Row 1:
──────
engine:                          ReplicatedMergeTree
data_paths:                      ['/var/lib/clickhouse/disks/gcs/store/631/6315b109-d639-4214-a1e7-afbd98f39727/']
metadata_path:                   /var/lib/clickhouse/store/e0f/e0f3e248-7996-44d4-853e-0384e153b740/trips.sql
storage_policy:                  gcs_main
formatReadableSize(total_bytes): 36.42 MiB

1 row in set. Elapsed: 0.002 sec.
```

#### Google Cloud Consoleで確認

バケットに移動すると、設定ファイルの`storage.xml`で使用された名前を持つフォルダーがそれぞれのバケットに作成されていることがわかります。フォルダーを展開すると、多くのファイルが表示され、データのパーティションを表しています。
#### レプリカ1のバケット
![replica one bucket](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-examine-bucket-1.png)
#### レプリカ2のバケット
![replica two bucket](@site/docs/ja/integrations/data-ingestion/s3/images/GCS-examine-bucket-2.png)
