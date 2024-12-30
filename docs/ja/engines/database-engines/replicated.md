---
slug: /ja/engines/database-engines/replicated
sidebar_position: 30
sidebar_label: Replicated
---

# Replicated

このエンジンは[Atomic](../../engines/database-engines/atomic.md)エンジンに基づいています。ZooKeeperに書き込まれ、特定のデータベースのすべてのレプリカで実行されるDDLログを通じてメタデータのレプリケーションをサポートしています。

1つのClickHouseサーバーは複数のレプリケートされたデータベースを同時に実行および更新できます。ただし、同じレプリケートされたデータベースの複数のレプリカを持つことはできません。

## データベースの作成 {#creating-a-database}
``` sql
CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**エンジンパラメータ**

- `zoo_path` — ZooKeeperのパス。同じZooKeeperパスは同じデータベースに対応します。
- `shard_name` — シャード名。データベースレプリカは`shard_name`によってシャードにグループ化されます。
- `replica_name` — レプリカ名。同じシャードのすべてのレプリカに対してレプリカ名は異なる必要があります。

[ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication)テーブルの場合、引数が提供されていない場合、デフォルトの引数が使用されます：`/clickhouse/tables/{uuid}/{shard}`および`{replica}`。これらはサーバー設定[default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path)および[default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name)で変更できます。マクロ`{uuid}`はテーブルのUUIDに展開され、`{shard}`および`{replica}`はデータベースエンジンパラメータではなくサーバー設定からの値に展開されます。しかし将来的には、Replicatedデータベースの`shard_name`および`replica_name`を使用できるようになります。

## 特徴と推奨事項 {#specifics-and-recommendations}

`Replicated`データベースを使用したDDLクエリは、[ON CLUSTER](../../sql-reference/distributed-ddl.md)クエリと類似の方法で動作しますが、いくつかの違いがあります。

まず、DDLリクエストは起動者（ユーザーからのリクエストを最初に受け取ったホスト）で実行を試みます。リクエストが完了しない場合、ユーザーはすぐにエラーを受け取り、他のホストはそれを実行しようとはしません。リクエストが起動者で正常に完了した場合、他のすべてのホストはそれが完了するまで自動的に再試行します。起動者は他のホストでクエリが完了するのを待とうとし（[distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)を超えることはありません）、各ホストでのクエリ実行ステータスを持つテーブルを返します。

エラーの場合の動作は[distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode)設定によって規制され、`Replicated`データベースでは`null_status_on_timeout`に設定するのが望ましいです。つまり、あるホストが[distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)中にリクエストを実行するのに時間がない場合、例外を投げるのではなく、テーブルに対して`NULL`ステータスを表示します。

[system.clusters](../../operations/system-tables/clusters.md)システムテーブルには、データベースのすべてのレプリカで構成される、レプリケートされたデータベースと同じ名前のクラスターが含まれています。このクラスターはレプリカの作成/削除時に自動的に更新され、[分散テーブル](../../engines/table-engines/special/distributed.md#distributed)用に使用できます。

新しいデータベースレプリカを作成する際、このレプリカは自分でテーブルを作成します。レプリカが長時間使用できない状態にあり、レプリケーションログから遅れをとった場合、それは自分のローカルメタデータを現在のZooKeeperのメタデータと比較し、余分なテーブルをデータ付きで非レプリケートされた別のデータベースに移動し（余計なものを誤って削除しないように）、欠けているテーブルを作成し、名前が変更された場合にはテーブル名を更新します。データは`ReplicatedMergeTree`レベルでレプリケートされます。つまり、テーブルがレプリケートされていない場合、データはレプリケートされません（データベースはメタデータのみの責任を負います）。

[`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md)クエリは許可されていますが、レプリケートされません。データベースエンジンは現在のレプリカにのみパーティション/パートを追加/取得/削除します。ただし、テーブル自体がレプリケートされたテーブルエンジンを使用している場合、`ATTACH`を使用した後にデータがレプリケートされます。

テーブルのレプリケーションを維持せずにクラスターを設定するだけが必要な場合は、[クラスターの自動検出](../../operations/cluster-discovery.md)機能を参照してください。

## 使用例 {#usage-example}

3つのホストでクラスターを作成する：

``` sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

DDLクエリを実行する：

``` sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

``` text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

システムテーブルを表示する：

``` sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

分散テーブルを作成し、データを挿入する：

``` sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

``` text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

もう1つのホストにレプリカを追加する：

``` sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

クラスターの構成は次のようになります：

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

分散テーブルは新しいホストからもデータを取得します：

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```
