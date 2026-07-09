---
description: 'ClickHouse のクラスター検出に関するドキュメント'
sidebar_label: 'Cluster Discovery'
slug: /operations/cluster-discovery
title: 'Cluster Discovery'
doc_type: 'guide'
---

<div id="overview">
  ## 概要
</div>

ClickHouse の Cluster Discovery 機能では、設定ファイルに各ノードを明示的に定義しなくても、ノードを自動的に検出して登録できるため、クラスター構成を簡素化できます。これは特に、各ノードを手動で定義するのが煩雑な場合に有効です。

:::note

Cluster Discovery は Experimental 機能であり、今後のバージョンで変更または削除される可能性があります。
有効にするには、設定ファイルに `allow_experimental_cluster_discovery` 設定を追加してください。

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## リモートサーバー設定
</div>

<div id="traditional-manual-configuration">
  ### 従来の手動設定
</div>

従来、ClickHouse では、クラスター内の各分片とレプリカを設定ファイルに手動で指定する必要がありました。

```xml
<remote_servers>
    <cluster_name>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node4</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_name>
</remote_servers>

```

<div id="using-cluster-discovery">
  ### Cluster Discovery の使用
</div>

Cluster Discovery では、各ノードを明示的に定義する代わりに、ZooKeeper 内のパスを指定するだけで済みます。ZooKeeper でこのパス配下に登録されたすべてのノードは、自動的に検出され、クラスターに追加されます。

```xml
<remote_servers>
    <cluster_name>
        <discovery>
            <path>/clickhouse/discovery/cluster_name</path>

            <!-- # Optional configuration parameters: -->

            <!-- ## Authentication credentials to access all other nodes in cluster: -->
            <!-- <user>user1</user> -->
            <!-- <password>pass123</password> -->
            <!-- ### Alternatively to password, interserver secret may be used: -->
            <!-- <secret>secret123</secret> -->

            <!-- ## Shard for current node (see below): -->
            <!-- <shard>1</shard> -->

            <!-- ## Observer mode (see below): -->
            <!-- <observer/> -->
        </discovery>
    </cluster_name>
</remote_servers>
```

特定のノードに分片番号を指定する場合は、`<discovery>`セクション内に`<shard>`タグを含めることができます。

`node1`と`node2`の場合:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

`node3` と `node4` の場合:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### オブザーバーモード
</div>

オブザーバーモードに設定されたノードは、自身をレプリカとして登録しません。
能動的には参加せず、クラスター内のほかのアクティブなレプリカを監視・検出するだけです。
オブザーバーモードを有効にするには、`<discovery>` セクション内に `<observer/>` タグを追加します。

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### クラスターの検出
</div>

クラスター内のホストだけでなく、クラスター自体を追加・削除する必要がある場合もあります。複数のクラスターのルートパスを持つ `<multicluster_root_path>` ノードを使用できます。

```xml
<remote_servers>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

この場合、別のホストがパス `/clickhouse/discovery/some_new_cluster` に自身を登録すると、`some_new_cluster` という名前のクラスターが追加されます。

両方の機能は同時に使用できます。ホストはクラスター `my_cluster` に自身を登録しつつ、ほかの任意のクラスターを検出することもできます。

```xml
<remote_servers>
    <my_cluster>
        <discovery>
            <path>/clickhouse/discovery/my_cluster</path>
        </discovery>
    </my_cluster>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

制限事項:

* 同じ `remote_servers` サブツリー内で `<path>` と `<multicluster_root_path>` の両方を使用することはできません。
* `<multicluster_root_path>` は `<observer/>` としか併用できません。
* Keeper の path の最後の要素がクラスター名として使用されますが、登録時には XML タグから名前が取得されます。

<div id="use-cases-and-limitations">
  ## ユースケースと制限事項
</div>

指定されたZooKeeper パスにノードが追加または削除されると、設定を変更したりサーバーを再起動したりしなくても、自動的に検出されるか、クラスターから削除されます。

ただし、変更が影響するのはクラスター構成のみであり、データや既存のデータベースおよびテーブルには影響しません。

3つのノードからなるクラスターで、次の例を考えてみましょう。

```xml
<remote_servers>
    <default>
        <discovery>
            <path>/clickhouse/discovery/default_cluster</path>
        </discovery>
    </default>
</remote_servers>
```

```sql
SELECT * EXCEPT (default_database, errors_count, slowdowns_count, estimated_recovery_time, database_shard_name, database_replica_name)
FROM system.clusters WHERE cluster = 'default';

┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

```sql
CREATE TABLE event_table ON CLUSTER default (event_time DateTime, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/event_table', '{replica}')
ORDER BY event_time PARTITION BY toYYYYMM(event_time);

INSERT INTO event_table ...
```

次に、クラスターに新しいノードを追加します。具体的には、設定ファイルの `remote_servers` セクションに同じエントリを持つノードを新たに起動します。

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

4 番目のノードはクラスターに参加していますが、テーブル `event_table` はまだ最初の 3 つのノードにしか存在していません：

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

すべてのノードにテーブルをレプリケートする必要がある場合は、クラスター検出の代わりに [Replicated](../engines/database-engines/replicated.md) データベースエンジンを使用できます。