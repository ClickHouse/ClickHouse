---
description: 'Distributed エンジンを使用するテーブルは独自のデータを保存せず、
  複数のサーバーにまたがる分散クエリ処理を可能にします。読み取りは自動的に
  並列化されます。読み取り時には、存在する場合、リモートサーバー上のテーブル索引が
  使用されます。'
sidebar_label: 'Distributed'
sidebar_position: 10
slug: /engines/table-engines/special/distributed
title: 'Distributed テーブルエンジン'
doc_type: 'reference'
---

:::warning Cloud での Distributed engine
ClickHouse Cloud で分散テーブルエンジンを作成するには、[`remote` と `remoteSecure`](../../../sql-reference/table-functions/remote) テーブル関数を使用できます。
ClickHouse Cloud では `Distributed(...)` 構文は使用できません。
:::

Distributed エンジンを使用するテーブルは独自のデータを保存せず、複数のサーバーにまたがる分散クエリ処理を可能にします。
読み取りは自動的に並列化されます。読み取り時には、存在する場合、リモートサーバー上のテーブル索引が使用されます。

<div id="distributed-creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

<div id="distributed-from-a-table">
  ### テーブルから
</div>

`Distributed` テーブルが現在のサーバー上のテーブルを参照している場合は、そのテーブルのスキーマを流用できます。

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

<div id="distributed-parameters">
  ### Distributed パラメーター
</div>

| パラメーター                    | 説明                                                                                                                                                                                                                                                                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`                 | server の設定ファイル内のクラスター名                                                                                                                                                                                                                                                                                                                |
| `database`                | リモートデータベースの名前                                                                                                                                                                                                                                                                                                                         |
| `table`                   | リモートテーブルの名前                                                                                                                                                                                                                                                                                                                           |
| `sharding_key` (Optional) | シャーディングキー。<br /> `sharding_key` の指定は、次の場合に必要です。 <ul><li>分散テーブルへの `INSERTs` の場合 (table engine がデータをどのように分割するかを判断するために `sharding_key` を必要とするため) 。ただし、`insert_distributed_one_random_shard` 設定が有効になっている場合、`INSERTs` にシャーディングキーは不要です。</li><li>`optimize_skip_unused_shards` を使用する場合。どの分片をクエリ対象にするかを判断するために `sharding_key` が必要です</li></ul> |
| `policy_name` (Optional)  | policy 名です。バックグラウンド送信用の temporary files を保存するために使用されます                                                                                                                                                                                                                                                                                |

**関連項目**

* [distributed&#95;foreground&#95;insert](../../../operations/settings/settings.md#distributed_foreground_insert) 設定
* 例については [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) を参照してください

<div id="distributed-settings">
  ### Distributed 設定
</div>

| Setting                                    | Description                                                                                                                                                                                                                                  | Default value                  |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `fsync_after_insert`                       | Distributed へのバックグラウンド `INSERT` の後に、ファイルデータに対して `fsync` を実行します。これにより、OS が **イニシエーター node** のディスク上で、挿入されたデータ全体をファイルに flush したことが保証されます。                                                                                                       | `false`                        |
| `fsync_directories`                        | ディレクトリに対して `fsync` を実行します。これにより、Distributed テーブルでのバックグラウンド `INSERT` に関連する操作 (`INSERT` 後、データを分片に送信した後など) の後に、OS がディレクトリのメタデータを更新したことが保証されます。                                                                                                  | `false`                        |
| `skip_unavailable_shards`                  | true の場合、ClickHouse は利用できない分片を暗黙的にスキップします。この設定の動作は `skip_unavailable_shards_mode` パラメーターによって制御されます。                                                                                                                                          | `false`                        |
| `skip_unavailable_shards_mode`             | `skip_unavailable_shards` が有効なときに、リモート分片からのどの例外を無視するかを制御します。`unavailable` は connection error のみを無視します。`unavailable_or_table_missing` は、見つからないテーブルまたはデータベースも無視します。`unavailable_or_exception_before_processing` は、分片がデータを返す前に受信したあらゆる例外も無視します。 | `unavailable_or_table_missing` |
| `bytes_to_throw_insert`                    | バックグラウンド `INSERT` で保留される compressed bytes 数がこの値を超える場合、例外がスローされます。`0` の場合はスローしません。                                                                                                                                                            | `0`                            |
| `bytes_to_delay_insert`                    | バックグラウンド `INSERT` で保留される compressed bytes 数がこの値を超える場合、クエリは遅延されます。`0` の場合は遅延しません。                                                                                                                                                             | `0`                            |
| `max_delay_to_insert`                      | バックグラウンド送信の保留 bytes が多い場合に、Distributed テーブルへのデータ挿入を遅延させる最大時間 (秒) 。                                                                                                                                                                           | `60`                           |
| `background_insert_batch`                  | [`distributed_background_insert_batch`](../../../operations/settings/settings.md#distributed_background_insert_batch) と同じです                                                                                                                  | `0`                            |
| `background_insert_split_batch_on_failure` | [`distributed_background_insert_split_batch_on_failure`](../../../operations/settings/settings.md#distributed_background_insert_split_batch_on_failure) と同じです                                                                                | `0`                            |
| `background_insert_sleep_time_ms`          | [`distributed_background_insert_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) と同じです                                                                                                  | `0`                            |
| `background_insert_max_sleep_time_ms`      | [`distributed_background_insert_max_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) と同じです                                                                                          | `0`                            |
| `flush_on_detach`                          | `DETACH` / `DROP` / server のシャットダウン時に、データをリモート node に flush します。                                                                                                                                                                             | `true`                         |

:::note
**耐久性設定** (`fsync_...`):

* バックグラウンド `INSERT` (つまり `distributed_foreground_insert=false`) にのみ影響します。この場合、データはまずイニシエーター node のディスクに保存され、その後バックグラウンドで分片に送信されます。
* `INSERT` のパフォーマンスが大幅に低下する可能性があります
* 分散テーブルのフォルダー内に保存されたデータを、**insert を受け付けた node** に書き込む処理に影響します。基盤となる MergeTree テーブルへのデータ書き込み保証が必要な場合は、`system.merge_tree_settings` の耐久性設定 (`...fsync...`) を参照してください

**Insert 制限設定** (`..._insert`) については、以下も参照してください。

* [`distributed_foreground_insert`](../../../operations/settings/settings.md#distributed_foreground_insert) 設定
* [`prefer_localhost_replica`](/ja/operations/settings/settings#prefer_localhost_replica) 設定
* `bytes_to_throw_insert` は `bytes_to_delay_insert` より先に処理されるため、`bytes_to_delay_insert` より小さい値は設定しないでください
  :::

**Example**

```sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

`logs` クラスター内のすべてのサーバーにある `default.hits` テーブルからデータが読み取られます。データは読み取られるだけでなく、可能な範囲でリモートサーバー上で部分的に処理もされます。たとえば、`GROUP BY` を含むクエリでは、データはリモートサーバー上で集計され、集約関数の中間状態がリクエスト元のサーバーに送信されます。その後、データはさらに集計されます。

データベース名の代わりに、文字列を返す定数式を使用できます。たとえば、`currentDatabase()` です。

<div id="distributed-clusters">
  ## クラスター
</div>

クラスターは、[サーバー設定ファイル](../../../operations/configuration-files.md)で設定します。

```xml
<remote_servers>
    <logs>
        <!-- Inter-server per-cluster secret for Distributed queries
             default: no secret (no authentication will be performed)

             If set, then Distributed queries will be validated on shards, so at least:
             - such cluster should exist on the shard,
             - such cluster should have the same secret.

             And also (and which is more important), the initial_user will
             be used as current user for the query.
        -->
        <!-- <secret></secret> -->
        
        <!-- Optional. Whether distributed DDL queries (ON CLUSTER clause) are allowed for this cluster. Default: true (allowed). -->        
        <!-- <allow_distributed_ddl_queries>true</allow_distributed_ddl_queries> -->
        
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. The shard name.  Must be non-empty and unique among shards in the cluster. If not specified, will be empty. -->
            <name>shard_01</name>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <!-- Optional. Priority of the replica for load balancing (see also load_balancing setting). Default: 1 (less value has more priority). -->
                <priority>1</priority>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <name>shard_02</name>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

ここでは、`logs` という名前のクラスターを定義しています。このクラスターは 2 つの分片で構成され、各分片には 2 つのレプリカがあります。分片は、データの異なる部分を保持するサーバーを指します (すべてのデータを読むには、すべての分片にアクセスする必要があります) 。レプリカは、同じデータを複製したサーバーです (すべてのデータを読む場合、各レプリカのうちどれか 1 つにアクセスすれば十分です) 。

クラスター名にドットを含めることはできません。

各サーバーについて、`host`、`port`、および必要に応じて `user`、`password`、`secure`、`compression`、`bind_host` の各パラメーターを指定します。

| Parameter     | Description                                                                                                                                                                | Default Value |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `host`        | リモートサーバーのアドレスです。ドメイン名、IPv4 アドレス、IPv6 アドレスのいずれも使用できます。ドメイン名を指定した場合、サーバーは起動時に DNS リクエストを実行し、その結果はサーバーの稼働中保持されます。DNS リクエストが失敗すると、サーバーは起動しません。DNS レコードを変更した場合は、サーバーを再起動してください。 | -             |
| `port`        | ネイティブ TCP 通信用のポートです (config 内の `tcp_port` で、通常は 9000 に設定されます) 。`http_port` と混同しないでください。                                                                                    | -             |
| `user`        | リモートサーバーへの接続に使用するユーザー名です。このユーザーには、指定したサーバーへの接続に必要なアクセス権が必要です。アクセスは `users.xml` ファイルで設定します。詳細は、[アクセス権](../../../guides/sre/user-management/index.md) のセクションを参照してください。       | `default`     |
| `password`    | リモートサーバーへの接続に使用するパスワードです (マスクされません) 。                                                                                                                                      | &#39;&#39;    |
| `secure`      | 安全な SSL/TLS 接続を使用するかどうかを指定します。通常はポートの指定も必要です (デフォルトのセキュアポートは `9440`) 。サーバーは `<tcp_port_secure>9440</tcp_port_secure>` で待ち受け、正しい証明書が設定されている必要があります。                         | `false`       |
| `compression` | データ圧縮を使用します。                                                                                                                                                               | `true`        |
| `bind_host`   | この node からリモートサーバーに接続するときに使用する送信元アドレスです。IPv4 アドレスのみサポートされます。ClickHouse の distributed queries で使用する送信元 IP アドレスを設定する必要がある、高度なデプロイメントのユースケースを想定しています。                         | -             |

レプリカを指定すると、読み取り時には各分片について、利用可能なレプリカのうち1つが選択されます。負荷分散 (どのレプリカにアクセスするかの優先順位) のアルゴリズムは、[load&#95;balancing](../../../operations/settings/settings.md#load_balancing) 設定で構成できます。サーバーとの接続が確立されていない場合は、短いタイムアウトで接続が試行されます。接続に失敗すると、次のレプリカが選択され、以降すべてのレプリカに対して同様に処理されます。すべてのレプリカへの接続試行が失敗した場合は、同じ方法で数回再試行されます。これは回復性の向上には有利ですが、完全な耐障害性を提供するものではありません。リモートサーバーが接続を受け付けても、動作しない、または正常に動作しない可能性があるためです。

分片は1つだけ指定することもできます (この場合、クエリ処理は distributed ではなく remote と呼ぶべきです) 。また、任意の数の分片を指定できます。各分片には、1つ以上の任意の数のレプリカを指定できます。分片ごとに異なる数のレプリカを指定することも可能です。

設定には、必要な数だけクラスターを指定できます。

クラスターを確認するには、`system.clusters` テーブルを使用します。

`Distributed` エンジンを使うと、クラスターをローカルサーバーのように扱えます。ただし、クラスターの設定を動的に指定することはできず、サーバー設定ファイルで構成する必要があります。通常、クラスター内のすべてのサーバーは同じクラスター設定を持ちます (必須ではありません) 。設定ファイル内のクラスターは、サーバーを再起動しなくてもその場で更新されます。

毎回、未知の分片とレプリカの組にクエリを送る必要がある場合は、`Distributed` テーブルを作成する必要はありません。代わりに `remote` テーブル関数を使用してください。[テーブル関数](../../../sql-reference/table-functions/index.md) のセクションを参照してください。

<div id="distributed-writing-data">
  ## データの書き込み
</div>

クラスターにデータを書き込む方法は 2 つあります。

1 つ目は、どのサーバーにどのデータを書き込むかを定義し、各分片に直接書き込む方法です。言い換えると、`Distributed` テーブルが参照しているクラスター内のリモートテーブルに対して、直接 `INSERT` ステートメントを実行します。これは最も柔軟な方法であり、対象領域の要件に応じて複雑なものも含め、任意のシャーディング方式を利用できます。また、データを異なる分片に完全に独立して書き込めるため、最も効率的な方法でもあります。

2 つ目は、`Distributed` テーブルに対して `INSERT` ステートメントを実行する方法です。この場合、テーブル自体が挿入されたデータを各サーバーに分散します。`Distributed` テーブルに書き込むには、`sharding_key` パラメータが設定されている必要があります (分片が 1 つしかない場合を除く) 。

各分片では、設定ファイルで `<weight>` を定義できます。デフォルトでは、重みは `1` です。データは、分片の重みに比例した量で各分片に分散されます。まずすべての分片の重みを合計し、次に各分片の重みをその合計で割って、各分片の比率を決定します。たとえば、2 つの分片があり、1 つ目の重みが 1、2 つ目の重みが 2 の場合、1 つ目には挿入された行の 3 分の 1 (1 / 3) が送られ、2 つ目には 3 分の 2 (2 / 3) が送られます。

各分片では、設定ファイルで `internal_replication` パラメータを定義できます。このパラメータを `true` に設定すると、書き込み時に最初の正常なレプリカが選択され、そこにデータが書き込まれます。`Distributed` テーブルの基になるテーブルがレプリケートテーブルである場合 (たとえば、`Replicated*MergeTree` テーブルエンジンのいずれか) には、これを使用してください。テーブルレプリカの 1 つが書き込みを受け取り、その内容は自動的に他のレプリカへレプリケーションされます。

`internal_replication` が `false` (デフォルト) に設定されている場合、データはすべてのレプリカに書き込まれます。この場合、`Distributed` テーブル自体がデータをレプリケーションします。これはレプリケートテーブルを使うより劣る方法です。レプリカ間の整合性は検証されないため、時間の経過とともに、それぞれに含まれるデータにわずかな差異が生じます。

データの 1 行をどの分片に送るかを決めるには、シャーディング式を評価し、その値を分片の重みの合計で割った剰余を使用します。行は、`prev_weights` から `prev_weights + weight` までの剰余の半開区間に対応する分片へ送られます。ここで、`prev_weights` はそれより番号の小さい分片の重みの合計、`weight` はこの分片の重みです。たとえば、2 つの分片があり、1 つ目の重みが 9、2 つ目の重みが 10 の場合、剰余が範囲 [0, 9) にある行は 1 つ目の分片に送られ、範囲 [9, 19) にある行は 2 つ目の分片に送られます。

シャーディング式には、定数およびテーブルのカラムからなる、整数を返す任意の式を使用できます。たとえば、データをランダムに分散するには `rand()` を、ユーザー ID を割った剰余で分散するには `UserID` を使用できます (この場合、1 人のユーザーのデータは 1 つの分片に配置されるため、ユーザー単位で `IN` や `JOIN` を実行しやすくなります) 。いずれかのカラムの分布が十分に均一でない場合は、`intHash64(UserID)` のようにハッシュ関数でラップできます。

除算の剰余をそのまま使う単純な方法は、シャーディングとしては限定的であり、常に適切とは限りません。これは中規模から大規模のデータ量 (数十台のサーバー) には有効ですが、非常に大規模なデータ量 (数百台以上のサーバー) には適していません。後者の場合は、`Distributed` テーブルのエントリを使うのではなく、対象領域に必要なシャーディング方式を使用してください。

次のような場合には、シャーディング方式を慎重に検討する必要があります。

* 特定のキーでデータを結合する必要があるクエリ (`IN` または `JOIN`) を使用する場合。データがこのキーでシャーディングされていれば、`GLOBAL IN` や `GLOBAL JOIN` の代わりにローカル `IN` や `JOIN` を使用でき、はるかに効率的です。
* 大量の小さなクエリを伴って多数のサーバー (数百台以上) を使用する場合。たとえば、個々のクライアント (Web サイト、広告主、パートナーなど) のデータに対するクエリです。小さなクエリがクラスター全体に影響しないようにするには、1 つのクライアントのデータを 1 つの分片に配置するのが合理的です。あるいは、2 段階のシャーディングを構成することもできます。つまり、クラスター全体を「レイヤー」に分割し、1 つのレイヤーは複数の分片で構成される場合があります。1 つのクライアントのデータは 1 つのレイヤーに配置されますが、必要に応じてそのレイヤーに分片を追加でき、その内部ではデータはランダムに分散されます。各レイヤーごとに `Distributed` テーブルを作成し、グローバルクエリ用に 1 つの共有分散テーブルを作成します。

データはバックグラウンドで書き込まれます。テーブルに挿入されると、データブロックはローカルファイルシステムに書き込まれるだけです。データはできるだけ早くバックグラウンドでリモートサーバーに送信されます。データ送信の周期は、[distributed&#95;background&#95;insert&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) および [distributed&#95;background&#95;insert&#95;max&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) 設定で管理されます。`Distributed` エンジンは、挿入されたデータを含む各ファイルを個別に送信しますが、[distributed&#95;background&#95;insert&#95;batch](../../../operations/settings/settings.md#distributed_background_insert_batch) 設定を使うと、ファイルのバッチ送信を有効にできます。この設定により、ローカルサーバーとネットワークのリソースをより効率的に活用できるため、クラスターのパフォーマンスが向上します。テーブルディレクトリ内のファイル一覧 (送信待ちのデータ) `/var/lib/clickhouse/data/database/table/` を確認して、データが正常に送信されているかを確認してください。バックグラウンドタスクを実行するスレッド数は、[background&#95;distributed&#95;schedule&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size) 設定で指定できます。

`Distributed` テーブルへの `INSERT` 後にサーバーが消失した場合、または異常な再起動が発生した場合 (たとえばハードウェア障害による場合) 、挿入されたデータが失われる可能性があります。テーブルディレクトリで破損した data part が検出されると、それは `broken` サブディレクトリに移動され、以後は使用されません。

<div id="distributed-reading-data">
  ## データの読み取り
</div>

`Distributed` テーブルに対してクエリを実行すると、`SELECT` クエリはすべての分片に送信され、データが分片間でどのように分散されているかに関係なく機能します (完全にランダムに分散されていても問題ありません) 。新しい分片を追加する際も、古いデータをそこへ移動する必要はありません。代わりに、重みを大きくして新しいデータを書き込めます。データの分散はやや不均一になりますが、クエリは正しく効率的に動作します。

`max_parallel_replicas` オプションが有効な場合、クエリ処理は単一の分片内のすべてのレプリカにまたがって並列化されます。詳しくは、[max&#95;parallel&#95;replicas](../../../operations/settings/settings.md#max_parallel_replicas) のセクションを参照してください。

分散 `in` クエリおよび `global in` クエリがどのように処理されるかについて詳しくは、[こちら](/ja/sql-reference/operators/in#distributed-subqueries) のドキュメントを参照してください。

<div id="virtual-columns">
  ## 仮想カラム
</div>

<div id="_shard_num">
  #### _Shard_num
</div>

`_shard_num` — テーブル `system.clusters` の `shard_num` の値が含まれます。型: [UInt32](../../../sql-reference/data-types/int-uint.md)。

:::note
[`remote`](../../../sql-reference/table-functions/remote.md) および [`cluster](../../../sql-reference/table-functions/cluster.md) table function は内部で一時的な Distributed テーブルを作成するため、`&#95;shard&#95;num&#96; はここでも使用できます。
:::

**関連項目**

* [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns) の説明
* [`background_distributed_schedule_pool_size`](/ja/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size) 設定
* [`shardNum()`](../../../sql-reference/functions/other-functions.md#shardNum) および [`shardCount()`](../../../sql-reference/functions/other-functions.md#shardCount) 関数