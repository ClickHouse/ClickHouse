---
slug: /ja/architecture/horizontal-scaling
sidebar_label: 水平拡張
sidebar_position: 10
title: 水平拡張
---
import ReplicationShardingTerminology from '@site/docs/ja/_snippets/_replication-sharding-terminology.md';
import ConfigFileNote from '@site/docs/ja/_snippets/_config-files.md';

## 概要
この例のアーキテクチャは、スケーラビリティを提供するように設計されています。こちらには3つのノードが含まれています：2つのClickHouseとコーディネーション（ClickHouse Keeper）サーバーが組み合わさったもの、およびこのクォーラムを完成させるためのClickHouse Keeperのみを持つ3番目のサーバーです。この例を使用して、データベース、テーブル、および2つのノードの両方でデータをクエリできる分散テーブルを作成します。

## レベル: 基本

<ReplicationShardingTerminology />

## 環境
### アーキテクチャ図
![2シャードと1レプリカのアーキテクチャ図](@site/docs/ja/deployment-guides/images/scaling-out-1.png)

|ノード|説明|
|----|-----------|
|chnode1|データ + ClickHouse Keeper|
|chnode2|データ + ClickHouse Keeper|
|chnode3|ClickHouse Keeperクォーラムに使用|

:::note
本番環境では、ClickHouse Keeperを専用ホストで実行することを強くお勧めします。この基本構成では、ClickHouse Serverプロセス内でKeeper機能を実行しています。ClickHouse Keeperをスタンドアロンでデプロイする手順は、[インストールドキュメント](/docs/ja/getting-started/install.md/#install-standalone-clickhouse-keeper)で確認できます。
:::

## インストール

3つのサーバーにClickhouseをインストールし、[アーカイブタイプごとの手順](/docs/ja/getting-started/install.md/#available-installation-options) (.deb, .rpm, .tar.gzなど) に従ってください。この例では、3台のマシンすべてにClickHouse ServerとClientのインストール手順に従います。

## 設定ファイルの編集

<ConfigFileNote />

## chnode1の設定

chnode1には、5つの設定ファイルがあります。これらのファイルを1つのファイルにまとめることもできますが、ドキュメント内で個別に見る方が明確かもしれません。設定ファイルを読み進めると、chnode1とchnode2のほとんどの設定が同じであることがわかります。一部の違いは強調されます。

### ネットワークとログ設定

これらの値は、お好みでカスタマイズできます。この例の設定は、1000Mで3回ロールオーバーするデバッグログを提供します。ClickHouseはIPv4ネットワークでポート8123と9000でリッスンし、インターサーバー通信にポート9009を使用します。

```xml title="network-and-logging.xml on chnode1"
<clickhouse>
        <logger>
                <level>debug</level>
                <log>/var/log/clickhouse-server/clickhouse-server.log</log>
                <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
                <size>1000M</size>
                <count>3</count>
        </logger>
        <display_name>clickhouse</display_name>
        <listen_host>0.0.0.0</listen_host>
        <http_port>8123</http_port>
        <tcp_port>9000</tcp_port>
        <interserver_http_port>9009</interserver_http_port>
</clickhouse>
```

### ClickHouse Keeper設定

ClickHouse Keeperは、データレプリケーションと分散DDLクエリの実行のためのコーディネーションシステムを提供します。ClickHouse KeeperはApache ZooKeeperと互換性があります。この設定では、ポート9181でClickHouse Keeperを有効にします。強調表示されている行は、Keeperのインスタンスに`server_id`を1としていることを示しています。この`enable-keeper.xml`ファイルの違いは3つのサーバーすべてでこれだけです。`chnode2`では`server_id`は`2`に設定され、`chnode3`では`server_id`は`3`に設定されます。raft構成セクションは3つのサーバーすべてで同じであり、下記に強調されて、`server_id`とraft構成内の`server`インスタンスの関係を示します。

:::note
何らかの理由でKeeperノードが交換または再構築された場合、既存の`server_id`を再利用しないでください。例えば、`server_id`が`2`であるKeeperノードが再構築された場合は、`server_id`を`4`以上にしてください。
:::

```xml title="enable-keeper.xml on chnode1"
<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
 # highlight-next-line
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <session_timeout_ms>30000</session_timeout_ms>
        <raft_logs_level>trace</raft_logs_level>
    </coordination_settings>

    <raft_configuration>
    # highlight-start
        <server>
            <id>1</id>
            <hostname>chnode1</hostname>
            <port>9234</port>
        </server>
    # highlight-end
        <server>
            <id>2</id>
            <hostname>chnode2</hostname>
            <port>9234</port>
        </server>
        <server>
            <id>3</id>
            <hostname>chnode3</hostname>
            <port>9234</port>
        </server>
    </raft_configuration>
  </keeper_server>
</clickhouse>
```

### マクロ設定

マクロ`shard`と`replica`により、分散DDLの複雑さが軽減されます。設定された値はDDLクエリ内で自動的に置き換えられるため、DDLを簡素化できます。この設定のマクロは、各ノードのシャードとレプリカ番号を指定します。この2シャード1レプリカの例では、レプリカマクロはchnode1とchnode2の両方で`replica_1`に設定されています。シャードマクロはchnode1では`1`、chnode2では`2`です。

```xml title="macros.xml on chnode1"
<clickhouse>
  <macros>
 # highlight-next-line
    <shard>1</shard>
    <replica>replica_1</replica>
  </macros>
</clickhouse>
```

### レプリケーションとシャーディング設定

以下の設定の大まかな概要です：
- XMLの`remote_servers`セクションは、環境内の各クラスターを指定します。属性`replace=true`は、デフォルトのClickHouse設定のサンプル`remote_servers`をこのファイルで指定された`remote_servers`設定で置き換えます。この属性がない場合、このファイル内のリモートサーバーはデフォルトのサンプルリストに追加されます。
- この例では、1つのクラスターが`cluster_2S_1R`という名前で存在します。
- クラスター`cluster_2S_1R`のためのシークレットが`mysecretphrase`という値で作成されます。環境内のすべてのリモートサーバー間でシークレットが共有され、正しいサーバーが一緒に結合されることを保証します。
- クラスター`cluster_2S_1R`には2つのシャードがあり、それぞれのシャードに1つのレプリカがあります。文書の冒頭近くのアーキテクチャ図を見て、以下のXMLの2つの`shard`定義と比較してください。各シャード定義には1つのレプリカがあります。レプリカは特定のシャード用です。レプリカのホストとポートが指定されています。最初のシャードの設定のレプリカは`chnode1`に保存され、2番目のシャードの設定のレプリカは`chnode2`に保存されます。
- シャードの内部レプリケーションはtrueに設定されています。各シャードは設定ファイルで`internal_replication`パラメーターを定義することができ、このパラメーターがtrueに設定されている場合、書き込み操作は最初の正常なレプリカを選択し、データを書き込みます。

```xml title="remote-servers.xml on chnode1"
<clickhouse>
  <remote_servers replace="true">
    <cluster_2S_1R>
    <secret>mysecretphrase</secret>
        <shard>
            <internal_replication>true</internal_replication>
            <replica>
                <host>chnode1</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <internal_replication>true</internal_replication>
            <replica>
                <host>chnode2</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_2S_1R>
  </remote_servers>
</clickhouse>
```

### Keeperの使用設定

上でClickHouse Keeperが設定されました。この設定ファイル`use-keeper.xml`は、レプリケーションと分散DDLのコーディネーションのためにClickHouse ServerがClickHouse Keeperを使用する設定を行っています。このファイルは、ClickHouse Serverがポート9181でchnode1から3のKeeperを使用するように指定しており、ファイルは`chnode1`と`chnode2`で同じです。

```xml title="use-keeper.xml on chnode1"
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>chnode1</host>
            <port>9181</port>
        </node>
        <node index="2">
            <host>chnode2</host>
            <port>9181</port>
        </node>
        <node index="3">
            <host>chnode3</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

## chnode2の設定

chnode1とchnode2の設定は非常に似ているため、ここでは違いのみを指摘します。

### ネットワークとログ設定

```xml title="network-and-logging.xml on chnode2" 
<clickhouse>
        <logger>
                <level>debug</level>
                <log>/var/log/clickhouse-server/clickhouse-server.log</log>
                <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
                <size>1000M</size>
                <count>3</count>
        </logger>
        <display_name>clickhouse</display_name>
        <listen_host>0.0.0.0</listen_host>
        <http_port>8123</http_port>
        <tcp_port>9000</tcp_port>
        <interserver_http_port>9009</interserver_http_port>
</clickhouse>
```

### ClickHouse Keeper設定

このファイルにはchnode1とchnode2の違いの一つが含まれています。Keeperの設定で`server_id`が`2`に設定されています。

```xml title="enable-keeper.xml on chnode2"
<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
 # highlight-next-line
    <server_id>2</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <session_timeout_ms>30000</session_timeout_ms>
        <raft_logs_level>trace</raft_logs_level>
    </coordination_settings>

    <raft_configuration>
        <server>
            <id>1</id>
            <hostname>chnode1</hostname>
            <port>9234</port>
        </server>
        # highlight-start
        <server>
            <id>2</id>
            <hostname>chnode2</hostname>
            <port>9234</port>
        </server>
        # highlight-end
        <server>
            <id>3</id>
            <hostname>chnode3</hostname>
            <port>9234</port>
        </server>
    </raft_configuration>
  </keeper_server>
</clickhouse>
```

### マクロ設定

マクロ設定にはchnode1とchnode2の違いの一つがあります。`shard`はこのノードでは`2`に設定されています。

```xml title="macros.xml on chnode2"
<clickhouse>
<macros>
 # highlight-next-line
    <shard>2</shard>
    <replica>replica_1</replica>
</macros>
</clickhouse>
```

### レプリケーションとシャーディング設定

```xml title="remote-servers.xml on chnode2"
<clickhouse>
  <remote_servers replace="true">
    <cluster_2S_1R>
    <secret>mysecretphrase</secret>
        <shard>
            <internal_replication>true</internal_replication>
            <replica>
                <host>chnode1</host>
                <port>9000</port>
            </replica>
        </shard>
            <shard>
            <internal_replication>true</internal_replication>
            <replica>
                <host>chnode2</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_2S_1R>
  </remote_servers>
</clickhouse>
```

### Keeperの使用設定

```xml title="use-keeper.xml on chnode2"
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>chnode1</host>
            <port>9181</port>
        </node>
        <node index="2">
            <host>chnode2</host>
            <port>9181</port>
        </node>
        <node index="3">
            <host>chnode3</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

## chnode3の設定

chnode3はデータを保存せず、ClickHouse Keeperがクォーラムの3番目のノードを提供するために使用されているため、chnode3には、ネットワークとログを設定するファイルとClickHouse Keeperを設定するファイルの2つの設定ファイルのみがあります。

### ネットワークとログ設定

```xml title="network-and-logging.xml on chnode3" 
<clickhouse>
        <logger>
                <level>debug</level>
                <log>/var/log/clickhouse-server/clickhouse-server.log</log>
                <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
                <size>1000M</size>
                <count>3</count>
        </logger>
        <display_name>clickhouse</display_name>
        <listen_host>0.0.0.0</listen_host>
        <http_port>8123</http_port>
        <tcp_port>9000</tcp_port>
        <interserver_http_port>9009</interserver_http_port>
</clickhouse>
```

### ClickHouse Keeper設定

```xml title="enable-keeper.xml on chnode3"
<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
 # highlight-next-line
    <server_id>3</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <session_timeout_ms>30000</session_timeout_ms>
        <raft_logs_level>trace</raft_logs_level>
    </coordination_settings>

    <raft_configuration>
        <server>
            <id>1</id>
            <hostname>chnode1</hostname>
            <port>9234</port>
        </server>
        <server>
            <id>2</id>
            <hostname>chnode2</hostname>
            <port>9234</port>
        </server>
        # highlight-start
        <server>
            <id>3</id>
            <hostname>chnode3</hostname>
            <port>9234</port>
        </server>
        # highlight-end
    </raft_configuration>
  </keeper_server>
</clickhouse>
```

## テスト

1. `chnode1`に接続し、上で設定されたクラスター`cluster_2S_1R`が存在することを確認します。
```sql
SHOW CLUSTERS
```
```response
┌─cluster───────┐
│ cluster_2S_1R │
└───────────────┘
```

2. クラスター上にデータベースを作成します。
```sql
CREATE DATABASE db1 ON CLUSTER cluster_2S_1R
```
```response
┌─host────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode2 │ 9000 │      0 │       │                   1 │                0 │
│ chnode1 │ 9000 │      0 │       │                   0 │                0 │
└─────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
```

3. クラスター上にMergeTreeテーブルエンジンを使用したテーブルを作成します。
:::note
テーブルエンジンのパラメーターを指定する必要はありません。これらはマクロに基づいて自動的に定義されます。
:::

```sql
CREATE TABLE db1.table1 ON CLUSTER cluster_2S_1R
(
    `id` UInt64,
    `column1` String
)
ENGINE = MergeTree
ORDER BY id
```
```response
┌─host────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode1 │ 9000 │      0 │       │                   1 │                0 │
│ chnode2 │ 9000 │      0 │       │                   0 │                0 │
└─────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
```

4. `chnode1`に接続して1行を挿入します。
```sql
INSERT INTO db1.table1 (id, column1) VALUES (1, 'abc');
```

5. `chnode2`に接続して1行を挿入します。

```sql
INSERT INTO db1.table1 (id, column1) VALUES (2, 'def');
```

6. `chnode1`または`chnode2`のどちらかに接続し、そのノードのテーブルに挿入された行のみが表示されることを確認します。
例えば、`chnode2`で
```sql
SELECT * FROM db1.table1;
```
```response
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘
```

7. 両方のノードで両方のシャードをクエリする分散テーブルを作成します。
（この例では、`rand()`関数をシャーディングキーとして設定し、各挿入をランダムに分散させます）
```sql
CREATE TABLE db1.table1_dist ON CLUSTER cluster_2S_1R
(
    `id` UInt64,
    `column1` String
)
ENGINE = Distributed('cluster_2S_1R', 'db1', 'table1', rand())
```
```response
┌─host────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode2 │ 9000 │      0 │       │                   1 │                0 │
│ chnode1 │ 9000 │      0 │       │                   0 │                0 │
└─────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
```

8. `chnode1`または`chnode2`に接続し、両方の行を確認するために分散テーブルをクエリします。
```
SELECT * FROM db1.table1_dist;
```
```reponse
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘
┌─id─┬─column1─┐
│  1 │ abc     │
└────┴─────────┘
```

## さらに詳しい情報:

- [分散テーブルエンジン](/docs/ja/engines/table-engines/special/distributed.md)
- [ClickHouse Keeper](/docs/ja/guides/sre/keeper/index.md)
