---
description: 'このエンジンは Atomic エンジンをベースにしています。特定のデータベースに対して、
  ZooKeeper に書き込まれた DDL ログをすべてのレプリカで実行することで、
  メタデータのレプリケーションをサポートします。'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

このエンジンは [Atomic](../../engines/database-engines/atomic.md) エンジンをベースにしています。特定のデータベースに対して、ZooKeeper に書き込まれた DDL ログをすべてのレプリカで実行することで、メタデータのレプリケーションをサポートします。

1 つの ClickHouse サーバーでは、複数の Replicated データベースを同時に実行し、更新できます。ただし、同じ Replicated データベースのレプリカを複数持つことはできません。

<div id="creating-a-database">
  ## データベースの作成
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**エンジンパラメータ**

* `zoo_path` — ZooKeeper パス。同じ ZooKeeper パスは同じデータベースに対応します。
* `shard_name` — 分片名。データベースのレプリカは `shard_name` ごとに分片にグループ化されます。
* `replica_name` — レプリカ名。同じ分片内のすべてのレプリカで、レプリカ名はそれぞれ異なる必要があります。

パラメータは省略できます。その場合、不足しているパラメータにはデフォルト値が設定されます。

`zoo_path` にマクロ `{uuid}` が含まれている場合は、このデータベースのすべてのレプリカで同じ UUID が使われるよう、明示的に UUID を指定するか、CREATE ステートメントに [ON CLUSTER](../../sql-reference/distributed-ddl.md) を追加する必要があります。

[ReplicatedMergeTree](/ja/engines/table-engines/mergetree-family/replication) テーブルでは、引数が指定されていない場合、デフォルトの引数 `/clickhouse/tables/{uuid}/{shard}` と `{replica}` が使用されます。これらはサーバー設定の [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) および [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name) で変更できます。マクロ `{uuid}` はテーブルの uuid に展開され、`{shard}` と `{replica}` はデータベースエンジンの引数ではなくサーバー設定の値に展開されます。ただし将来的には、Replicated データベースの `shard_name` と `replica_name` を使えるようになる予定です。

デフォルトの ZooKeeper クラスターの代わりに、レプリケートされたデータベースのメタデータ保存先として補助的な ZooKeeper クラスターを使用することもサポートされています。次のように、補助的な ZooKeeper クラスターを使ってレプリケートされたデータベースを作成できます。

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## 注意点と推奨事項
</div>

`Replicated` データベースでの DDLクエリは [ON CLUSTER](../../sql-reference/distributed-ddl.md) クエリと似た動作をしますが、いくつか細かな違いがあります。

まず、DDL リクエストはイニシエーター (ユーザーからのリクエストを最初に受け取ったホスト) で実行が試みられます。リクエストが実行されなかった場合、ユーザーはただちにエラーを受け取り、他のホストは実行を試みません。リクエストがイニシエーターで正常に完了した場合、他のすべてのホストは完了するまで自動的に再試行します。イニシエーターは他のホストでクエリが完了するのを待機しようとし (待機時間は [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout) まで) 、各ホストでのクエリ実行ステータスを示すテーブルを返します。

エラー発生時の動作は [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode) 設定によって制御されます。`Replicated` データベースでは、これを `null_status_on_timeout` に設定することを推奨します。つまり、一部のホストが [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout) 以内にリクエストを実行できなかった場合、例外をスローするのではなく、それらのホストについてはテーブル内に `NULL` ステータスを表示します。

[system.clusters](../../operations/system-tables/clusters.md) システムテーブルには、レプリケートされたデータベースと同名のクラスターが含まれており、このクラスターはそのデータベースのすべてのレプリカで構成されています。このクラスターは、レプリカの作成や削除に応じて自動的に更新され、[Distributed](/ja/engines/table-engines/special/distributed) テーブルで使用できます。

データベースの新しいレプリカを作成すると、そのレプリカは自動的にテーブルを作成します。レプリカが長時間利用できず、レプリケーションログに対して遅延が生じた場合、そのレプリカはローカルのメタデータを ZooKeeper 内の現在のメタデータと照合し、余分なデータ付きテーブルを別の非レプリケートデータベースへ移動し (不要なものを誤って削除しないようにするため) 、不足しているテーブルを作成し、テーブル名が変更されていればその名前を更新します。データは `ReplicatedMergeTree` レベルでレプリケーションされます。つまり、テーブルがレプリケートされていない場合、データはレプリケーションされません (データベースが担うのはメタデータのみです) 。

[`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) クエリは許可されていますが、レプリケーションはされません。データベースエンジンは、現在のレプリカに対してのみパーティションまたはパートの追加、取得、削除を行います。ただし、テーブル自体が Replicated テーブルエンジンを使用している場合は、`ATTACH` の実行後にデータがレプリケーションされます。

テーブルのレプリケーションを維持せずにクラスターだけを構成したい場合は、[Cluster Discovery](../../operations/cluster-discovery.md) 機能を参照してください。

<div id="usage-example">
  ## 使用例
</div>

3つのホストで構成されるクラスターを作成する:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

暗黙的パラメータを使用してクラスター上でデータベースを作成する:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

DDLクエリの実行:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

システムテーブルの表示:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

分散テーブルの作成とデータの挿入:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

別のホストにレプリカを追加する場合:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

`zoo_path` でマクロ `{uuid}` を使用している場合に、別のホストにレプリカを追加する:

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

クラスター構成は次のようになります。

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

分散テーブルは、新しいホストからのデータも取得します:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## 設定
</div>

次の設定がサポートされています。

| Setting                                                                      | Default                        | Description                                                                                                                                                                                                                 |
| ---------------------------------------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | 古くなったテーブルの全テーブルに対する比率がこれを超える場合、レプリカは自動的に復旧されません                                                                                                                                                                             |
| `max_replication_lag_to_enqueue`                                             | 50                             | レプリカのレプリケーションラグがこれを超えている場合、そのレプリカでクエリを実行しようとすると例外がスローされます                                                                                                                                                                   |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | タイムアウトを超え、かつイニシエーターのホストがまだそれを実行していない場合、レプリカはクエリのキャンセルを試みます                                                                                                                                                                  |
| `collection_name`                                                            |                                | クラスター認証に必要なすべての情報が定義されている、server の config で定義されたコレクション名                                                                                                                                                                     |
| `check_consistency`                                                          | true                           | ローカルのメタデータと Keeper 内のメタデータの整合性を確認し、不整合がある場合はレプリカの復旧を行います                                                                                                                                                                    |
| `max_retries_before_automatic_recovery`                                      | 10                             | キュー内のエントリの実行試行回数の上限です。これを超えると、レプリカを失われたものとしてマークし、スナップショットから復旧します (0 は無制限を意味します)                                                                                                                                             |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | 有効にすると、Replicated データベースで DDLs を処理する際に、可能であればリフレッシュ可能なマテリアライズドビューの一時テーブルに関する DDLs の作成と交換をスキップします                                                                                                                            |
| `logs_to_keep`                                                               | 1000                           | Replicated データベースについて ZooKeeper に保持するログのデフォルト数です。                                                                                                                                                                           |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | ZooKeeper 内のデータベースへの path です。引数が省略された場合、データベースの作成時に使用されます。                                                                                                                                                                  |
| `default_replica_shard_name`                                                 | `{shard}`                      | データベース内のレプリカの分片名です。引数が省略された場合、データベースの作成時に使用されます。                                                                                                                                                                            |
| `default_replica_name`                                                       | `{replica}`                    | データベース内のレプリカ名です。引数が省略された場合、データベースの作成時に使用されます。                                                                                                                                                                               |
| `internal_replication`                                                       | false                          | この Replicated データベースのクラスターで作成された分散テーブルが、いずれか 1 つのレプリカにデータを送信するか (internal replication はクラスターの replicas 自身がレプリケーションを行うことを意味します) 、またはすべてのレプリカに送信するかを指定します (internal replication が無効な場合、分散テーブルは挿入されたデータをすべての replicas に送信します)  |

デフォルト値は configuration file で上書きできます

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```