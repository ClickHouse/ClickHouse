---
slug: /ja/operations/cluster-discovery
sidebar_label: クラスター検出
---
# クラスター検出

## 概要

ClickHouseのクラスター検出機能は、ノードを自動で検出・登録できるようにすることで、クラスターの設定を簡素化します。設定ファイルでノードを明示的に定義する必要がなくなり、特に各ノードを手動で定義することが負担になる場合に便利です。

:::note

クラスター検出はエクスペリメンタルな機能であり、将来のバージョンで変更または削除される可能性があります。
この機能を有効にするには、次の設定を設定ファイルに含めてください：

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```
:::

## リモートサーバーの設定

### 従来の手動設定

従来、ClickHouseではクラスター内の各シャードとレプリカを設定で手動で指定する必要がありました：

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

### クラスター検出の使用

クラスター検出を使用すると、ZooKeeperでパスを指定するだけで、すべてのノードが自動的に検出され、クラスターに追加されます。

```xml
<remote_servers>
    <cluster_name>
        <discovery>
            <path>/clickhouse/discovery/cluster_name</path>

            <!-- # オプションの設定パラメータ: -->

            <!-- ## クラスター内の他のすべてのノードへのアクセスのための認証情報: -->
            <!-- <user>user1</user> -->
            <!-- <password>pass123</password> -->
            <!-- ### パスワードの代わりにインターサーバーの秘密キーを使用することも可能: -->
            <!-- <secret>secret123</secret> -->

            <!-- ## 現在のノードのシャード番号 (以下を参照): -->
            <!-- <shard>1</shard> -->

            <!-- ## オブザーバーモード (以下を参照): -->
            <!-- <observer/> -->
        </discovery>
    </cluster_name>
</remote_servers>
```

特定のノードにシャード番号を指定したい場合は、`<discovery>`セクション内に`<shard>`タグを含めます：

`node1` と `node2` 用:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

`node3` と `node4` 用:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

### オブザーバーモード

オブザーバーモードで設定されたノードは、レプリカとして自分自身を登録しません。
クラスター内の他のアクティブなレプリカを観察・検出するだけで、積極的に参加しません。
オブザーバーモードを有効にするには、`<discovery>`セクション内に`<observer/>`タグを含めます：

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

## 使用例と制限事項

指定したZooKeeperのパスにノードが追加または削除されると、それに応じてクラスターに自動的に追加または削除され、設定変更やサーバー再起動は必要ありません。

ただし、変更はクラスター設定にのみ影響し、データや既存のデータベースおよびテーブルには影響を与えません。

3ノードのクラスターの例を考えてみましょう：

```xml
<remote_servers>
    <default>
        <discovery>
            <path>/clickhouse/discovery/default_cluster</path>
        </discovery>
    </default>
</remote_servers>
```

```
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

次に新しいノードをクラスターに追加し、設定ファイルの`remote_servers`セクションに同じエントリを持つ新しいノードを起動します：

```
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

4番目のノードがクラスターに参加していますが、テーブル`event_table`はまだ最初の3つのノードにのみ存在しています：

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

テーブルがすべてのノードでレプリケートされる必要がある場合は、クラスター検出の代わりに[レプリケートされた](../engines/database-engines/replicated.md)データベースエンジンを使用することをお勧めします。
