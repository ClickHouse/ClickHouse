---
slug: /ja/guides/sre/keeper/clickhouse-keeper

sidebar_label: ClickHouse Keeperの設定
sidebar_position: 10
keywords:
  - Keeper
  - ZooKeeper
  - clickhouse-keeper
  - レプリケーション
description: ClickHouse Keeperまたはclickhouse-keeperは、ZooKeeperを置き換えるもので、レプリケーションと調整を提供します。
---

# ClickHouse Keeper (clickhouse-keeper)

import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_automated.md';

<SelfManaged />

ClickHouse Keeperは、データの[レプリケーション](/docs/ja/engines/table-engines/mergetree-family/replication.md)と[分散DDL](/docs/ja/sql-reference/distributed-ddl.md)クエリの実行を調整するシステムを提供します。ClickHouse KeeperはZooKeeperと互換性があります。

### 実装の詳細 {#implementation-details}

ZooKeeperは、最初に知られるオープンソースの調整システムの1つです。Javaで実装されており、非常にシンプルで強力なデータモデルを持っています。ZooKeeperの調整アルゴリズム、ZooKeeper Atomic Broadcast (ZAB)は、各ZooKeeperノードがローカルに読み取りを行うため、読み取りに対する線形化可能性保証を提供しません。ZooKeeperとは異なり、ClickHouse KeeperはC++で書かれており、[RAFTアルゴリズム](https://raft.github.io/)の[実装](https://github.com/eBay/NuRaft)を使用しています。このアルゴリズムは読み書きに対する線形化可能性を許可し、さまざまな言語でオープンソースの実装があります。

デフォルトでは、ClickHouse KeeperはZooKeeperと同じ保証を提供します：線形化可能な書き込みと非線形化可能な読み取りです。クライアントサーバープロトコルは互換性があるため、任意の標準的なZooKeeperクライアントを使用してClickHouse Keeperと対話できます。スナップショットとログはZooKeeperとは互換性のない形式ですが、`clickhouse-keeper-converter`ツールを使用してZooKeeperデータをClickHouse Keeperスナップショットに変換できます。ClickHouse KeeperのインターサーバープロトコルもZooKeeperと互換性がないため、ZooKeeper / ClickHouse Keeperの混合クラスターは不可能です。

ClickHouse Keeperは、[ZooKeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)と同じ方法でアクセス制御リスト（ACL）をサポートします。ClickHouse Keeperは、同じ権限セットをサポートし、`world`、`auth`、および`digest`の同一の組み込みスキームを持っています。ダイジェスト認証スキームは`username:password`ペアを使用し、パスワードはBase64でエンコードされます。

:::note
外部統合はサポートされていません。
:::

### 設定 {#configuration}

ClickHouse Keeperは、ZooKeeperのスタンドアロンの代替品として、またはClickHouseサーバーの内部の一部として使用できます。いずれの場合も、設定はほぼ同じ`.xml`ファイルです。

#### Keeperの設定項目

ClickHouse Keeperの主な設定タグは`<keeper_server>`で、以下のパラメータがあります：

| パラメータ                           | 説明                                                                                                                                                                                                                                                   | デフォルト                                                            |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `tcp_port`                           | クライアントが接続するためのポート。                                                                                                                                                                                                                     | `2181`                                                                |
| `tcp_port_secure`                    | クライアントとkeeperサーバー間のSSL接続のためのセキュアポート。                                                                                                                                                                                         | -                                                                     |
| `server_id`                          | ユニークなサーバーID。ClickHouse Keeperクラスターの各参加者は、1, 2, 3といったユニークな番号を持たなければなりません。                                                                                                                               | -                                                                     |
| `log_storage_path`                   | 調整ログの保存パス。ZooKeeperと同様に、ログはビジーでないノードに保存するのが最適です。                                                                                                                                                                | -                                                                     |
| `snapshot_storage_path`              | 調整スナップショットの保存パス。                                                                                                                                                                                                                       | -                                                                     |
| `enable_reconfiguration`             | [`reconfig`](#reconfiguration)を介した動的クラスター再設定を有効にします。                                                                                                                                                                              | `False`                                                               |
| `max_memory_usage_soft_limit`        | Keeperの最大メモリ使用量のソフト制限（バイト単位）。                                                                                                                                                                                                   | `max_memory_usage_soft_limit_ratio` * `physical_memory_amount`        |
| `max_memory_usage_soft_limit_ratio`  | `max_memory_usage_soft_limit`が設定されていないか0に設定されている場合、この値を使用してデフォルトソフト制限を定義します。                                                                                                                                | `0.9`                                                                 |
| `cgroups_memory_observer_wait_time`  | `max_memory_usage_soft_limit`が設定されていないか0に設定されている場合、物理メモリ量を監視するための間隔。このメモリ量が変化すると、`max_memory_usage_soft_limit_ratio`によってKeeperのメモリソフト制限を再計算します。                               | `15`                                                                  |
| `http_control`                       | [HTTP control](#http-control)インターフェイスの設定。                                                                                                                                                                                                    | -                                                                     |
| `digest_enabled`                     | リアルタイムデータ整合性チェックを有効にします。                                                                                                                                                                                                         | `True`                                                                |
| `create_snapshot_on_exit`            | シャットダウン時にスナップショットを作成します。                                                                                                                                                                                                          | -                                                                     |
| `hostname_checks_enabled`            | クラスター設定のためのホスト名チェックを有効にします（例：リモートエンドポイントと一緒にlocalhostが使用されている場合）。                                                                                                                             | `True`                                                                |

他の一般的なパラメータは、ClickHouseサーバーの設定（`listen_host`、`logger`など）から継承されます。

#### 内部調整設定

内部の調整設定は、`<keeper_server>.<coordination_settings>`セクションにあり、以下のパラメータがあります：

| パラメータ                           | 説明                                                                                                                                                                                                                                           | デフォルト                                                                                                     |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `operation_timeout_ms`               | 単一クライアント操作のタイムアウト（ms）。                                                                                                                                                                                                      | `10000`                                                                                                       |
| `min_session_timeout_ms`             | クライアントセッションの最小タイムアウト（ms）。                                                                                                                                                                                               | `10000`                                                                                                       |
| `session_timeout_ms`                 | クライアントセッションの最大タイムアウト（ms）。                                                                                                                                                                                               | `100000`                                                                                                      |
| `dead_session_check_period_ms`       | ClickHouse Keeperがデッドセッションをチェックし削除する頻度（ms）。                                                                                                                                                                             | `500`                                                                                                         |
| `heart_beat_interval_ms`             | ClickHouse Keeperリーダーがフォロワーにハートビートを送信する頻度（ms）。                                                                                                                                                                       | `500`                                                                                                         |
| `election_timeout_lower_bound_ms`    | フォロワーがこの間隔内にリーダーからハートビートを受信しない場合、フォロワーはリーダー選挙を開始できます。`election_timeout_upper_bound_ms`より小さくまたは等しくなければなりません。理想的には等しくない方が良いです。                                                    | `1000`                                                                                                        |
| `election_timeout_upper_bound_ms`    | フォロワーがこの間隔内にリーダーからハートビートを受信しない場合、リーダー選挙を開始しなければなりません。                                                                                                                                         | `2000`                                                                                                        |
| `rotate_log_storage_interval`        | 単一ファイルに保存するログレコードの数。                                                                                                                                                                                                       | `100000`                                                                                                      |
| `reserved_log_items`                 | コンパクション前に保存する調整ログレコードの数。                                                                                                                                                                                               | `100000`                                                                                                      |
| `snapshot_distance`                  | ClickHouse Keeperが新しいスナップショットを作成する頻度（ログ内のレコード数で）。                                                                                                                                                               | `100000`                                                                                                      |
| `snapshots_to_keep`                  | 保持するスナップショットの数。                                                                                                                                                                                                                 | `3`                                                                                                           |
| `stale_log_gap`                      | リーダーがフォロワーを古くなったと見なしてスナップショットを送信する代わりにログを送信するしきい値。                                                                                                                                            | `10000`                                                                                                       |
| `fresh_log_gap`                      | ノードが新しくなったとき。                                                                                                                                                                                                                     | `200`                                                                                                         |
| `max_requests_batch_size`            | RAFTに送信する前にリクエストのバッチでの最大サイズ（リクエスト数）。                                                                                                                                                                           | `100`                                                                                                         |
| `force_sync`                         | 調整ログへの各書き込みで`fsync`を呼び出します。                                                                                                                                                                                                | `true`                                                                                                        |
| `quorum_reads`                       | 全体的なRAFTコンセンサスと同じスピードで読み取り要求を実行します。                                                                                                                                                                               | `false`                                                                                                       |
| `raft_logs_level`                    | 調整に関するテキストログレベル（trace、debugなど）。                                                                                                                                                                                            | `system default`                                                                                              |
| `auto_forwarding`                    | フォロワーからリーダーに書き込み要求を転送することを許可します。                                                                                                                                                                                | `true`                                                                                                        |
| `shutdown_timeout`                   | 内部接続を終了してシャットダウンするまでの待機時間（ms）。                                                                                                                                                                                      | `5000`                                                                                                        |
| `startup_timeout`                    | サーバーが指定されたタイムアウト内に他のクォーラム参加者に接続しない場合、終了します（ms）。                                                                                                                                                     | `30000`                                                                                                       |
| `four_letter_word_white_list`        | 4lwコマンドのホワイトリスト。                                                                                                                                                                                                                  | `conf, cons, crst, envi, ruok, srst, srvr, stat, wchs, dirs, mntr, isro, rcvr, apiv, csnp, lgif, rqld, ydld`  |
| `async_replication`                  | 非同期レプリケーションを有効にします。全ての書き込みと読み取りの保証が維持され、より良いパフォーマンスが達成されます。この設定は、後方互換性を破壊しないようにするためにデフォルトでは無効になっています。                                                          | `false`                                                                                                       |
| `latest_logs_cache_size_threshold`   | 最新のログエントリのインメモリキャッシュの最大合計サイズ                                                                                                                                                                                       | `1GiB`                                                                                                        |
| `commit_logs_cache_size_threshold`   | 次にコミットのために必要なログエントリのインメモリキャッシュの最大合計サイズ                                                                                                                                                                   | `500MiB`                                                                                                      |
| `disk_move_retries_wait_ms`          | ディスク間でファイルを移動している間に発生した失敗後、再試行までどれくらい待機するか                                                                                                                                                             | `1000`                                                                                                        |
| `disk_move_retries_during_init`      | 初期化中にディスク間でファイルを移動している間に発生した失敗後の再試行回数                                                                                                                                                                     | `100`                                                                                                         |
| `experimental_use_rocksdb`           | rocksdbをバックエンドストレージとして使用                                                                                                    | `0`                                                                                                           |

クォーラム設定は、`<keeper_server>.<raft_configuration>`セクションにあり、サーバーの説明を含んでいます。

すべてのクォーラムに対する唯一のパラメータは`secure`で、クォーラム参加者間の通信の暗号化接続を有効にします。このパラメータは、ノード間の内部通信にSSL接続が必要な場合は `true` に設定できますが、それ以外の場合は指定しなくても構いません。

各`<server>`のメインパラメータは以下です：

- `id` — クォーラム内のサーバー識別子。
- `hostname` — このサーバーが配置されているホスト名。
- `port` — サーバーが接続を受け付けるポート。
- `can_become_leader` — サーバーを`learner`として設定するには`false`を設定します。省略すると値は`true`です。

:::note
ClickHouse Keeperクラスターのトポロジーが変更される場合（例：サーバーを置き換える場合）、`server_id`から`hostname`へのマッピングを一貫して保持し、既存の`server_id`を他のサーバーに再利用したりしないようにしてください（例：ClickHouse Keeperのデプロイに自動化スクリプトを使用する場合）。

Keeperインスタンスのホストが変わる可能性がある場合は、生IPアドレスの代わりにホスト名を定義して使用することをお勧めします。ホスト名を変更することは、サーバーを削除して再追加するのと同等で、場合によっては不可能な場合があります（例：クォーラム用のKeeperインスタンスが十分でない場合）。
:::

:::note
`async_replication`は後方互換性を破壊しないようにするため、デフォルトでは無効になっています。クラスター内の全てのKeeperインスタンスが`async_replication`をサポートするバージョン（v23.9+）を実行している場合、この設定を有効にすることをお勧めします。パフォーマンスを向上させることができ、デメリットはありません。
:::

3ノードのクォーラムの設定例は、[インテグレーションテスト](https://github.com/ClickHouse/ClickHouse/tree/master/tests/integration)で`test_keeper_`接頭辞付きで見つけることができます。サーバー#1の設定例：

```xml
<keeper_server>
    <tcp_port>2181</tcp_port>
    <server_id>1</server_id>
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
            <hostname>zoo1</hostname>
            <port>9234</port>
        </server>
        <server>
            <id>2</id>
            <hostname>zoo2</hostname>
            <port>9234</port>
        </server>
        <server>
            <id>3</id>
            <hostname>zoo3</hostname>
            <port>9234</port>
        </server>
    </raft_configuration>
</keeper_server>
```

### 実行方法 {#how-to-run}

ClickHouse KeeperはClickHouseサーバーパッケージにバンドルされており、`<keeper_server>`の設定をあなたの`/etc/your_path_to_config/clickhouse-server/config.xml`に追加し、通常通りClickHouseサーバーを開始します。スタンドアロンのClickHouse Keeperを実行したい場合は、次のようにして開始できます：

```bash
clickhouse-keeper --config /etc/your_path_to_config/config.xml
```

シンボリックリンク（`clickhouse-keeper`）を持たない場合は、それを作成するか`clickhouse`への引数として`keeper`を指定できます：

```bash
clickhouse keeper --config /etc/your_path_to_config/config.xml
```

### 4文字コマンド {#four-letter-word-commands}

ClickHouse Keeperは、ZooKeeperとほぼ同じ4文字コマンド（4lw）を提供します。各コマンドは`mntr`、`stat`などの4文字で構成されています。興味深いコマンドのいくつかを紹介すると、`stat`はサーバーと接続されたクライアントに関する一般情報を提供し、`srvr`と`cons`はそれぞれサーバーと接続に関する詳細を提供します。

4lwコマンドのホワイトリスト設定`four_letter_word_white_list`は、デフォルトで`conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,ydld`です。

これらのコマンドをClickHouse Keeperに対してtelnetやncで発行できます。

```
echo mntr | nc localhost 9181
```

以下に4lwコマンドの詳細を示します：

- `ruok`: サーバーがエラーステートなしで実行中であることをテストします。サーバーが実行中の場合`imok`で応答します。そうでない場合、まったく応答しません。サーバーが実行中であることを示す `imok` の応答は、サーバーがクォーラムに参加していることを必ずしも示しているわけではなく、単にサーバープロセスがアクティブで指定されたクライアントポートにバインドされていることを示しています。

```
imok
```

- `mntr`: クラスターの健全性を監視するために使用できる変数のリストを出力します。

```
zk_version      v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
zk_avg_latency  0
zk_max_latency  0
zk_min_latency  0
zk_packets_received     68
zk_packets_sent 68
zk_num_alive_connections        1
zk_outstanding_requests 0
zk_server_state leader
zk_znode_count  4
zk_watch_count  1
zk_ephemerals_count     0
zk_approximate_data_size        723
zk_open_file_descriptor_count   310
zk_max_file_descriptor_count    10240
zk_followers    0
zk_synced_followers     0
```

- `srvr`: サーバーの完全な詳細をリストします。

```
ClickHouse Keeper version: v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
Latency min/avg/max: 0/0/0
Received: 2
Sent : 2
Connections: 1
Outstanding: 0
Zxid: 34
Mode: leader
Node count: 4
```

- `stat`: サーバーと接続されたクライアントの簡単な詳細をリストします。

```
ClickHouse Keeper version: v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
Clients:
 192.168.1.1:52852(recved=0,sent=0)
 192.168.1.1:52042(recved=24,sent=48)
Latency min/avg/max: 0/0/0
Received: 4
Sent : 4
Connections: 1
Outstanding: 0
Zxid: 36
Mode: leader
Node count: 4
```

- `srst`: サーバーの統計をリセットします。このコマンドは`srvr`、`mntr`、および`stat`の結果に影響を与えます。

```
Server stats reset.
```

- `conf`: 提供中の設定の詳細を表示します。

```
server_id=1
tcp_port=2181
four_letter_word_white_list=*
log_storage_path=./coordination/logs
snapshot_storage_path=./coordination/snapshots
max_requests_batch_size=100
session_timeout_ms=30000
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=1000000000000000
snapshot_distance=10000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=240000
raft_logs_level=information
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
quorum_reads=false
force_sync=false
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
```

- `cons`: このサーバーに接続されているすべてのクライアントの完全な接続/セッションの詳細をリストします。受信/送信したパケット数、セッションID、操作レイテンシー、最後に実行された操作などの情報を含みます。

```
 192.168.1.1:52163(recved=0,sent=0,sid=0xffffffffffffffff,lop=NA,est=1636454787393,to=30000,lzxid=0xffffffffffffffff,lresp=0,llat=0,minlat=0,avglat=0,maxlat=0)
 192.168.1.1:52042(recved=9,sent=18,sid=0x0000000000000001,lop=List,est=1636454739887,to=30000,lcxid=0x0000000000000005,lzxid=0x0000000000000005,lresp=1636454739892,llat=0,minlat=0,avglat=0,maxlat=0)
```

- `crst`: すべての接続に対する接続/セッション統計をリセットします。

```
Connection stats reset.
```

- `envi`: サーバー環境の詳細を表示します。

```
Environment:
clickhouse.keeper.version=v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
host.name=ZBMAC-C02D4054M.local
os.name=Darwin
os.arch=x86_64
os.version=19.6.0
cpu.count=12
user.name=root
user.home=/Users/JackyWoo/
user.dir=/Users/JackyWoo/project/jd/clickhouse/cmake-build-debug/programs/
user.tmp=/var/folders/b4/smbq5mfj7578f2jzwn602tt40000gn/T/
```

- `dirs`: スナップショットとログファイルの合計サイズをバイト単位で表示します。

```
snapshot_dir_size: 0
log_dir_size: 3875
```

- `isro`: サーバーが読み取り専用モードで実行されているかをテストします。読み取り専用モードで`ro`、そうでない場合は`rw`で応答します。

```
rw
```

- `wchs`: サーバーの監視に関する簡単な情報をリストします。

```
1 connections watching 1 paths
Total watches:1
```

- `wchc`: セッションごとに、サーバーの監視に関する詳細な情報をリストします。セッション（接続）のリストと関連する監視（パス）を出力します。注意として、監視数によっては、この操作は高負荷になる可能性があります（サーバーのパフォーマンスに影響を与える）、注意して使用してください。

```
0x0000000000000001
    /clickhouse/task_queue/ddl
```

- `wchp`: サーバーのパスごとに、監視に関する詳細情報をリストします。パス（znodes）のリストと関連するセッションを出力します。注意として、監視数によっては、この操作は高負荷になる可能性があります（つまり、サーバーのパフォーマンスに影響を与える）、注意して使用してください。

```
/clickhouse/task_queue/ddl
    0x0000000000000001
```

- `dump`: 実行中のセッションと一時ノードをリストします。これはリーダーでのみ機能します。

```
Sessions dump (2):
0x0000000000000001
0x0000000000000002
Sessions with Ephemerals (1):
0x0000000000000001
 /clickhouse/task_queue/ddl
```

- `csnp`: スナップショット作成タスクのスケジュールを設定します。成功した場合はスケジュールされたスナップショットの最後のコミットログインデックスを返し、失敗した場合は`Failed to schedule snapshot creation task.`を返します。`lgif`コマンドはスナップショットが完了したかどうかを判断するのに役立ちます。

```
100
```

- `lgif`: Keeperログの情報。`first_log_idx` : ログストア内の最初のログインデックス; `first_log_term` : 最初のログターム; `last_log_idx` : ログストア内の最後のログインデックス; `last_log_term` : 最後のログターム; `last_committed_log_idx` : 状態マシンで最後にコミットされたログインデックス; `leader_committed_log_idx` : 私の視点からのリーダーのコミットログインデックス; `target_committed_log_idx` : コミットされるべきターゲットログインデックス; `last_snapshot_idx` : 最後のスナップショットの最大コミットログインデックス。

```
first_log_idx   1
first_log_term  1
last_log_idx    101
last_log_term   1
last_committed_log_idx  100
leader_committed_log_idx    101
target_committed_log_idx    101
last_snapshot_idx   50
```

- `rqld`: 新しいリーダーになるようにリクエストします。リーダーシップリクエストが送信された場合は`Sent leadership request to leader.`、送信されなかった場合は`Failed to send leadership request to leader.`を返します。ノードが既にリーダーであれば、結果はリクエストが送信されたのと同じです。

```
Sent leadership request to leader.
```

- `ftfl`: Keeperインスタンスで有効になっているすべてのフィーチャーフラグをリストします。

```
filtered_list   1
multi_read  1
check_not_exists    0
```
- `ydld`: リーダーシップを放棄し、フォロワーになるリクエストを送信します。リクエストを受け取ったサーバーがリーダーの場合、まず書き込み操作を一時停止し、後継者（現在のリーダーは後継者にはなれません）が最新のログのキャッチアップを終了するのを待ってから辞任します。後継者は自動的に選ばれます。リクエストが送信された場合は`リーダーへのリーダーシップ放棄リクエストを送信しました。`を返し、送信されなかった場合は`リーダーへのリーダーシップ放棄リクエストの送信に失敗しました。`を返します。ノードがすでにフォロワーである場合、結果はリクエストが送信された場合と同じです。

```
リーダーへのリーダーシップ放棄リクエストを送信しました。
```

- `pfev`: 収集されたすべてのイベントの値を返します。各イベントについてイベント名、イベント値、およびイベントの説明を返します。

```
FileOpen	62	ファイルが開かれた回数。
Seek	4	'lseek'関数が呼ばれた回数。
ReadBufferFromFileDescriptorRead	126	ファイルディスクリプタからの読み取り（read/pread）の回数。ソケットは含まれません。
ReadBufferFromFileDescriptorReadFailed	0	ファイルディスクリプタからの読み取り（read/pread）が失敗した回数。
ReadBufferFromFileDescriptorReadBytes	178846	ファイルディスクリプタから読み取られたバイト数。ファイルが圧縮されている場合、これは圧縮データのサイズを示します。
WriteBufferFromFileDescriptorWrite	7	ファイルディスクリプタへの書き込み（write/pwrite）の回数。ソケットは含まれません。
WriteBufferFromFileDescriptorWriteFailed	0	ファイルディスクリプタへの書き込み（write/pwrite）が失敗した回数。
WriteBufferFromFileDescriptorWriteBytes	153	ファイルディスクリプタに書き込まれたバイト数。ファイルが圧縮されている場合、これは圧縮データサイズを示します。
FileSync	2	ファイルに対してF_FULLFSYNC/fsync/fdatasync関数が呼ばれた回数。
DirectorySync	0	ディレクトリに対してF_FULLFSYNC/fsync/fdatasync関数が呼ばれた回数。
FileSyncElapsedMicroseconds	12756	ファイルに対するF_FULLFSYNC/fsync/fdatasyncシステムコールを待って費やした合計時間。
DirectorySyncElapsedMicroseconds	0	ディレクトリに対するF_FULLFSYNC/fsync/fdatasyncシステムコールを待って費やした合計時間。
ReadCompressedBytes	0	圧縮ソース（ファイル、ネットワーク）から読み取ったバイト数（解凍前のバイト数）。
CompressedReadBufferBlocks	0	圧縮ソース（ファイル、ネットワーク）から読み取った圧縮ブロック（各自で圧縮されるデータブロック）。
CompressedReadBufferBytes	0	圧縮ソース（ファイル、ネットワーク）から読み取った非圧縮バイト数（解凍後のバイト数）。
AIOWrite	0	LinuxまたはFreeBSDのAIOインターフェースによる書き込みの回数。
AIOWriteBytes	0	LinuxまたはFreeBSDのAIOインターフェースで書き込まれたバイト数。
...
```

### HTTP制御 {#http-control}

ClickHouse Keeperは、レプリカがトラフィックを受信する準備ができているかどうかをチェックするためのHTTPインターフェイスを提供します。これは、[Kubernetes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes)のようなクラウド環境で使用できます。

`/ready`エンドポイントを有効にする構成の例:

```xml
<clickhouse>
    <keeper_server>
        <http_control>
            <port>9182</port>
            <readiness>
                <endpoint>/ready</endpoint>
            </readiness>
        </http_control>
    </keeper_server>
</clickhouse>
```

### フィーチャーフラグ

KeeperはZooKeeperおよびそのクライアントと完全に互換性がありますが、ClickHouseクライアントで使用できるいくつかのユニークな機能やリクエストタイプも導入しています。これらの機能は後方互換性のない変更を引き起こす可能性があるため、そのほとんどはデフォルトで無効になっており、`keeper_server.feature_flags`設定を使用して有効にすることができます。すべての機能は明示的に無効にすることができます。Keeperクラスタに新しい機能を有効にしたい場合、まずクラスタ内のすべてのKeeperインスタンスを機能をサポートするバージョンに更新し、それから機能自体を有効にすることをお勧めします。

`multi_read`を無効にし、`check_not_exists`を有効にするフィーチャーフラグ設定の例:

```xml
<clickhouse>
    <keeper_server>
        <feature_flags>
            <multi_read>0</multi_read>
            <check_not_exists>1</check_not_exists>
        </feature_flags>
    </keeper_server>
</clickhouse>
```

利用可能な機能は次の通りです:

`multi_read` - マルチリードリクエストのサポート。デフォルト: `1`  
`filtered_list` - ノードのタイプ（永続的またはエフェメラル）で結果をフィルターするリストリクエストのサポート。デフォルト: `1`  
`check_not_exists` - ノードが存在しないことを確認する`CheckNotExists`リクエストのサポート。デフォルト: `0`  
`create_if_not_exists` - ノードが存在しない場合に作成しようとする`CreateIfNotExists`リクエストのサポート。存在する場合、変更は適用されず`ZOK`が返されます。デフォルト: `0`

### ZooKeeperからの移行 {#migration-from-zookeeper}

ZooKeeperからClickHouse Keeperへのシームレスな移行は不可能です。ZooKeeperクラスタを停止し、データを変換してClickHouse Keeperを開始する必要があります。`clickhouse-keeper-converter`ツールはZooKeeperログとスナップショットをClickHouse Keeperのスナップショットに変換することを可能にします。これはZooKeeper > 3.4でのみ動作します。移行の手順:

1. すべてのZooKeeperノードを停止します。

2. オプションですが推奨: ZooKeeperのリーダーノードを見つけ、それを開始して再度停止します。これにより、ZooKeeperが一貫したスナップショットを作成することを強制します。

3. リーダーで`clickhouse-keeper-converter`を実行します。例:

```bash
clickhouse-keeper-converter --zookeeper-logs-dir /var/lib/zookeeper/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/version-2 --output-dir /path/to/clickhouse/keeper/snapshots
```

4. Keeperが構成されたClickHouseサーバーノードにスナップショットをコピーするか、ZooKeeperの代わりにClickHouse Keeperを開始します。スナップショットはすべてのノードで持続する必要があります。そうしないと、空のノードがより速くなり、リーダーになることがあります。

:::note
`keeper-converter`ツールはKeeper単独のバイナリからは利用できません。
ClickHouseがインストールされている場合、バイナリを直接使用できます:

```bash
clickhouse keeper-converter ...
```

それ以外の場合は、[バイナリをダウンロード](/docs/ja/getting-started/quick-start#1-download-the-binary)して上記のようにツールを実行することができます。ClickHouseをインストールせずに。
:::


### クォーラム喪失後の復旧

ClickHouse KeeperはRaftを使用しているため、クラスタのサイズに応じて一定のノードのクラッシュを許容できます。
たとえば、3ノードのクラスタの場合、1ノードがクラッシュした場合でも正しく動作し続けます。

クラスタの構成は動的に設定できますが、制限があります。再構成はRaftに依存しているため、クラスタからノードを追加または削除するにはクォーラムが必要です。クラスタ内の多くのノードを同時に失い、それらを再起動できなくなると、Raftは機能を停止し、従来の方法でクラスタを再構成できなくなります。

それにもかかわらず、ClickHouse Keeperにはリカバリーモードがあり、1つのノードだけで強制的にクラスタを再構成することができます。これは、ノードを再起動できない場合、または同じエンドポイントで新しいインスタンスを開始する場合だけを最後の手段として行うべきです。

続行する前に注意すべき重要なこと:
- 失敗したノードが再びクラスタに接続できないことを確認してください。
- ステップで指定されていない限り、新しいノードを起動しないでください。

上記のことを確認した後、以下の手順を実行します:
1. Keeperノードの1つを新しいリーダーとして選択します。そのノードのデータがクラスタ全体に利用されることになるので、最新の状態を持つノードを使用することをお勧めします。
2. 他の何かを行う前に、選択したノードの`log_storage_path`と`snapshot_storage_path`フォルダをバックアップします。
3. 使用したいすべてのノードでクラスタを再構成します。
4. 選択したノードに四文字コマンド`rcvr`を送り、ノードをリカバリモードに移行するか、選択したノードでKeeperインスタンスを停止し、`--force-recovery`引数で再起動します。
5. 新しいノードでKeeperインスタンスを1つずつ開始し、次のノードを開始する前に`mntr`が`zk_server_state`で`follower`を返すことを確認します。
6. リカバリモード中、リーダーノードはクォーラムを新しいノードと達成するまで`mntr`コマンドに対してエラーメッセージを返し、クライアントとフォロワーからのすべての要求を拒否します。
7. クォーラムが達成されると、リーダーノードは通常モードに戻り、すべての要求を受け入れてRaftと共に確認されます。`mntr`で確認すると`zk_server_state`として`leader`が返されます。

## Keeperでディスクを使用する

Keeperはスナップショット、ログファイル、および状態ファイルの保存に[外部ディスク](/docs/ja/operations/storing-data.md)のサブセットをサポートしています。

サポートされているディスクの種類は:
- s3_plain
- s3
- local

以下は、ディスク定義が含まれる設定の例です。

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <log_local>
                <type>local</type>
                <path>/var/lib/clickhouse/coordination/logs/</path>
            </log_local>
            <log_s3_plain>
                <type>s3_plain</type>
                <endpoint>https://some_s3_endpoint/logs/</endpoint>
                <access_key_id>ACCESS_KEY</access_key_id>
                <secret_access_key>SECRET_KEY</secret_access_key>
            </log_s3_plain>
            <snapshot_local>
                <type>local</type>
                <path>/var/lib/clickhouse/coordination/snapshots/</path>
            </snapshot_local>
            <snapshot_s3_plain>
                <type>s3_plain</type>
                <endpoint>https://some_s3_endpoint/snapshots/</endpoint>
                <access_key_id>ACCESS_KEY</access_key_id>
                <secret_access_key>SECRET_KEY</secret_access_key>
            </snapshot_s3_plain>
            <state_s3_plain>
                <type>s3_plain</type>
                <endpoint>https://some_s3_endpoint/state/</endpoint>
                <access_key_id>ACCESS_KEY</access_key_id>
                <secret_access_key>SECRET_KEY</secret_access_key>
            </state_s3_plain>
        </disks>
    </storage_configuration>
</clickhouse>
```

ログ用のディスクを使用するには、`keeper_server.log_storage_disk`設定をディスク名に設定する必要があります。  
スナップショット用のディスクを使用するには、`keeper_server.snapshot_storage_disk`設定をディスク名に設定する必要があります。  
また、最新のログまたはスナップショット用に異なるディスクを使用することができます。これには、`keeper_server.latest_log_storage_disk`と`keeper_server.latest_snapshot_storage_disk`をそれぞれ使用します。  
この場合、新しいログやスナップショットが作成されるたびにKeeperは自動的にファイルを適切なディスクに移動します。
状態ファイル用のディスクを使用するには、`keeper_server.state_storage_disk`設定をディスク名に設定する必要があります。  

ディスク間のファイルの移動は安全で、転送の途中でKeeperが停止した場合でもデータを失うリスクはありません。  
ファイルが完全に新しいディスクに移動されるまで、元のディスクから削除されません。

Keeperで`keeper_server.coordination_settings.force_sync`が`true`に設定されている場合（一部のディスクタイプでは保証を満たすことができません。デフォルトではtrueです）。   
現在、`local`タイプのディスクのみが持続的な同期をサポートしています。   
`force_sync`が使用されている場合、`log_storage_disk`は`local`ディスクでなければなりません。`latest_log_storage_disk`が使用されていない場合。   
`latest_log_storage_disk`が使用されている場合、それは常に`local`ディスクでなければなりません。   
`force_sync`が無効の場合、すべてのタイプのディスクを任意のセットアップで使用できます。

Keeperインスタンスのストレージ設定例は次のようにすることができます:

```xml
<clickhouse>
    <keeper_server>
        <log_storage_disk>log_s3_plain</log_storage_disk>
        <latest_log_storage_disk>log_local</latest_log_storage_disk>

        <snapshot_storage_disk>snapshot_s3_plain</snapshot_storage_disk>
        <latest_snapshot_storage_disk>snapshot_local</latest_snapshot_storage_disk>
    </keeper_server>
</clickhouse>
```

このインスタンスは、最新でないログをすべて`log_s3_plain`ディスクに保存し、最新のログは`log_local`ディスクに保存します。  
スナップショットについても同様で、最新でないスナップショットは`snapshot_s3_plain`に保存され、最新のスナップショットは`snapshot_local`に保存されます。

### ディスク設定の変更

:::important
新しいディスク設定を適用する前に、すべてのKeeperログとスナップショットを手動でバックアップしてください。
:::

階層化ディスク設定が定義されている場合（最新のファイルに別々のディスクを使用する）、Keeperは起動時にファイルを正しいディスクに自動的に移動しようとします。  
以前と同じ保証が適用されます。ファイルが完全に新しいディスクに移動されるまで、元のディスクから削除されません。そのため、複数の再起動は安全に行うことができます。

ファイルを完全に新しいディスクに移動する必要がある場合（または2ディスク設定から1ディスク設定に移行する場合）、`keeper_server.old_snapshot_storage_disk`と`keeper_server.old_log_storage_disk`の複数の定義を使用できます。

次の設定は、以前の2ディスク設定から完全に新しい1ディスク設定に移行する方法を示しています:

```xml
<clickhouse>
    <keeper_server>
        <old_log_storage_disk>log_local</old_log_storage_disk>
        <old_log_storage_disk>log_s3_plain</old_log_storage_disk>
        <log_storage_disk>log_local2</log_storage_disk>

        <old_snapshot_storage_disk>snapshot_s3_plain</old_snapshot_storage_disk>
        <old_snapshot_storage_disk>snapshot_local</old_snapshot_storage_disk>
        <snapshot_storage_disk>snapshot_local2</snapshot_storage_disk>
    </keeper_server>
</clickhouse>
```

起動時に、すべてのログファイルは`log_local`と`log_s3_plain`から`log_local2`に移動されます。  
また、すべてのスナップショットファイルは`snapshot_local`と`snapshot_s3_plain`から`snapshot_local2`に移動されます。

## ログキャッシュの設定

ディスクから読み取るデータ量を最小限に抑えるために、Keeperはログエントリをメモリにキャッシュします。  
リクエストが大きい場合、ログエントリは多くのメモリを使用するため、キャッシュされるログの量に上限があります。  
上限は次の2つの設定で制御されます:
- `latest_logs_cache_size_threshold` - キャッシュに格納された最新のログの総サイズ
- `commit_logs_cache_size_threshold` - 次にコミットする必要がある一連のログの総サイズ

デフォルト値が大きすぎる場合、これら2つの設定を減らしてメモリ使用量を減らすことができます。

:::note
各キャッシュおよびファイルから読み取られたログの量を確認するために`pfev`コマンドを使用できます。  
また、Prometheusエンドポイントからメトリクスを使用して、両方のキャッシュの現在のサイズを追跡することもできます。  
:::


## Prometheus

KeeperはPrometheusからのスクレイピング用にメトリクス・データを公開することができます。  
設定は[ClickHouseと同じ方法で行われます。](/docs/ja/operations/server-configuration-parameters/settings#prometheus)

## ClickHouse Keeperユーザーガイド

このガイドは、ClickHouse Keeperを設定するためのシンプルで最小限の設定を提供し、分散操作をテストする方法の例を示します。この例は、Linux上で3つのノードを使用して実行されます。

### 1. ノードをKeeper設定で構成する

1. 3つのClickHouseインスタンスを3つのホスト（chnode1、chnode2、chnode3）にインストールします。（ClickHouseのインストールに関する詳細は[クイックスタート](/docs/ja/getting-started/install.md)を参照してください。）

2. 各ノードで、ネットワークインターフェイスを介した外部通信を許可するために、次のエントリを追加します。
    ```xml
    <listen_host>0.0.0.0</listen_host>
    ```

3. 次のClickHouse Keeper設定を3つのサーバーに追加し、各サーバーの`<server_id>`設定を更新します。例えば、`chnode1`は`1`、`chnode2`は`2`のように設定します。
    ```xml
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
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
                <hostname>chnode1.domain.com</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>chnode2.domain.com</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>3</id>
                <hostname>chnode3.domain.com</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
    ```

    以下は上記の基本設定です:

    |パラメータ |説明                   |例              |
    |----------|------------------------------|---------------------|
    |tcp_port   |keeperのクライアントが使用するポート|9181 デフォルトではzookeeperの2181相当|
    |server_id| 各ClickHouse Keeperサーバーの識別子でraftの設定で使用されます| 1|
    |coordination_settings| パラメータ（タイムアウト等）を設定するセクション| タイムアウト: 10000, ログレベル: trace|
    |server    |参加するサーバーの定義|各サーバーの定義リスト|
    |raft_configuration| keeperクラスタ内の各サーバーの設定| 各サーバーと設定のリスト|
    |id      |keeperサービス用のサーバーの数値ID|1|
    |hostname   |keeperクラスタ内の各サーバーのホスト名、IP、またはFQDN|chnode1.domain.com|
    |port|インターサーバーのkeeper接続をリッスンするポート|9234|


4. 組み込みのZooKeeperを有効にします。ClickHouse Keeperエンジンを使用します:
    ```xml
        <zookeeper>
            <node>
                <host>chnode1.domain.com</host>
                <port>9181</port>
            </node>
            <node>
                <host>chnode2.domain.com</host>
                <port>9181</port>
            </node>
            <node>
                <host>chnode3.domain.com</host>
                <port>9181</port>
            </node>
        </zookeeper>
    ```

    以下は上記の基本設定です:

    |パラメータ |説明                   |例              |
    |----------|------------------------------|---------------------|
    |node   |ClickHouse Keeper接続用のノードのリスト|各サーバーの設定エントリ|
    |host|各ClickHouse Keeperノードのホスト名、IP、またはFQDN|chnode1.domain.com|
    |port|ClickHouse Keeperのクライアントポート|9181|

5. ClickHouseを再起動し、各Keeperインスタンスが実行されていることを確認します。各サーバーで次のコマンドを実行します。`ruok`コマンドはKeeperが実行中で正常である場合に`imok`を返します。
    ```bash
    # echo ruok | nc localhost 9181; echo
    imok
    ```

6. `system`データベースには、ClickHouse Keeperインスタンスの詳細を含む`zookeeper`テーブルがあります。このテーブルを表示してみましょう:
    ```sql
    SELECT *
    FROM system.zookeeper
    WHERE path IN ('/', '/clickhouse')
    ```

    テーブルは次のようになります:
    ```response
    ┌─name───────┬─value─┬─czxid─┬─mzxid─┬───────────────ctime─┬───────────────mtime─┬─version─┬─cversion─┬─aversion─┬─ephemeralOwner─┬─dataLength─┬─numChildren─┬─pzxid─┬─path────────┐
    │ clickhouse │       │   124 │   124 │ 2022-03-07 00:49:34 │ 2022-03-07 00:49:34 │       0 │        2 │        0 │              0 │          0 │           2 │  5693 │ /           │
    │ task_queue │       │   125 │   125 │ 2022-03-07 00:49:34 │ 2022-03-07 00:49:34 │       0 │        1 │        0 │              0 │          0 │           1 │   126 │ /clickhouse │
    │ tables     │       │  5693 │  5693 │ 2022-03-07 00:49:34 │ 2022-03-07 00:49:34 │       0 │        3 │        0 │              0 │          0 │           3 │  6461 │ /clickhouse │
    └────────────┴───────┴───────┴───────┴─────────────────────┴─────────────────────┴─────────┴──────────┴──────────┴────────────────┴────────────┴─────────────┴───────┴─────────────┘
    ```

### 2. ClickHouseにクラスタを設定する

1. 2つのシャードと1つのレプリカで構成されるシンプルなクラスタを構成します。3番目のノードは、ClickHouse Keeperのクォーラム条件のために使用されます。`chnode1`と`chnode2`で設定を更新します。このクラスタは、各ノードに1つのシャードを配置し、合計2つのシャードを持ち、レプリケーションは行われません。この例では、データの一部が片方のノードにあり、残りがもう一方のノードにあります:
    ```xml
        <remote_servers>
            <cluster_2S_1R>
                <shard>
                    <replica>
                        <host>chnode1.domain.com</host>
                        <port>9000</port>
                        <user>default</user>
                        <password>ClickHouse123!</password>
                    </replica>
                </shard>
                <shard>
                    <replica>
                        <host>chnode2.domain.com</host>
                        <port>9000</port>
                        <user>default</user>
                        <password>ClickHouse123!</password>
                    </replica>
                </shard>
            </cluster_2S_1R>
        </remote_servers>
    ```

    |パラメータ |説明                   |例              |
    |----------|------------------------------|---------------------|
    |shard   |クラスタ定義内のレプリカのリスト|各シャードのレプリカリスト|
    |replica|各レプリカの設定のリスト|各レプリカの設定エントリ|
    |host|レプリカシャードをホストするサーバーのホスト名、IPまたはFQDN|chnode1.domain.com|
    |port|ネイティブTCPプロトコルを使った通信に使用するポート|9000|
    |user|クラスタインスタンスに接続するために使用するユーザー名|default|
    |password|クラスタインスタンスへの接続を許可するためのユーザインのパスワード|ClickHouse123!|


2. ClickHouseを再起動し、クラスタが作成されたことを確認します。
    ```bash
    SHOW clusters;
    ```

    クラスタが表示されます:
    ```response
    ┌─cluster───────┐
    │ cluster_2S_1R │
    └───────────────┘
    ```

### 3. 分散テーブルの作成とテスト

1. 新しいクラスタで新しいデータベースを作成します。`ON CLUSTER`句は、両方のノードでデータベースを自動的に作成します。
    ```sql
    CREATE DATABASE db1 ON CLUSTER 'cluster_2S_1R';
    ```

2. `db1`データベースに新しいテーブルを作成します。再び、`ON CLUSTER`は両方のノードにテーブルを作成します。
    ```sql
    CREATE TABLE db1.table1 on cluster 'cluster_2S_1R'
    (
        `id` UInt64,
        `column1` String
    )
    ENGINE = MergeTree
    ORDER BY column1
    ```

3. `chnode1`ノードにいくつかの行を追加します:
    ```sql
    INSERT INTO db1.table1
        (id, column1)
    VALUES
        (1, 'abc'),
        (2, 'def')
    ```

4. `chnode2`ノードにいくつかの行を追加します:
    ```sql
    INSERT INTO db1.table1
        (id, column1)
    VALUES
        (3, 'ghi'),
        (4, 'jkl')
    ```

5. 各ノードでの`SELECT`ステートメントの実行は、そのノードにあるデータのみを表示します。例えば、`chnode1`で:
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: 7ef1edbc-df25-462b-a9d4-3fe6f9cb0b6d

    ┌─id─┬─column1─┐
    │  1 │ abc     │
    │  2 │ def     │
    └────┴─────────┘

    2 rows in set. Elapsed: 0.006 sec.
    ```

    `chnode2`で:
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: c43763cc-c69c-4bcc-afbe-50e764adfcbf

    ┌─id─┬─column1─┐
    │  3 │ ghi     │
    │  4 │ jkl     │
    └────┴─────────┘
    ```

6. 2つのシャード上のデータを表すために`分散テーブル`を作成できます。`分散テーブル`エンジンを持つテーブルは、自身のデータを保存せず、複数のサーバーで分散クエリ処理を可能にします。読み取りはすべてのシャードに当たり、書き込みはシャード間で分散できます。`chnode1`で以下のクエリを実行します:
    ```sql
    CREATE TABLE db1.dist_table (
        id UInt64,
        column1 String
    )
    ENGINE = Distributed(cluster_2S_1R,db1,table1)
    ```

7. `dist_table`をクエリすると、2つのシャードのデータがすべて4行返されることを確認します:
    ```sql
    SELECT *
    FROM db1.dist_table
    ```

    ```response
    Query id: 495bffa0-f849-4a0c-aeea-d7115a54747a

    ┌─id─┬─column1─┐
    │  1 │ abc     │
    │  2 │ def     │
    └────┴─────────┘
    ┌─id─┬─column1─┐
    │  3 │ ghi     │
    │  4 │ jkl     │
    └────┴─────────┘

    4 rows in set. Elapsed: 0.018 sec.
    ```

### まとめ

このガイドでは、ClickHouse Keeperを使用してクラスタを設定する方法を示しました。ClickHouse Keeperを使用すると、クラスターを構成し、シャード間でレプリケート可能な分散テーブルを定義できます。


## ユニークなパスでClickHouse Keeperを設定する

<SelfManaged />

### 説明

この記事では、組み込みの`{uuid}`マクロ設定を使用して、ClickHouse KeeperまたはZooKeeperにユニークなエントリを作成する方法を説明します。ユニークなパスはテーブルを頻繁に作成および削除する場合に役立ちます。これは、パスが作成されるたびに新しい`uuid`がパスで使用され、パスが再利用されないため、Keeperガベージコレクションがパスエントリを削除するまで数分待つ必要がないからです。

### 環境例
すべてのノードにClickHouse Keeperを持ち、ClickHouseを2ノードに持つように構成される3ノードクラスタ例。この設定により、ClickHouse Keeperには3ノード（タイブレーカーノードを含む）があり、1つのClickHouseシャードが2つのレプリカで構成されます。

|ノード|説明|
|-----|-----|
|chnode1.marsnet.local|データノード - cluster cluster_1S_2R|
|chnode2.marsnet.local|データノード - cluster cluster_1S_2R|
|chnode3.marsnet.local|ClickHouse Keeperタイブレーカーノード|

クラスタの例設定:
```xml
    <remote_servers>
        <cluster_1S_2R>
            <shard>
                <replica>
                    <host>chnode1.marsnet.local</host>
                    <port>9440</port>
                    <user>default</user>
                    <password>ClickHouse123!</password>
                    <secure>1</secure>
                </replica>
                <replica>
                    <host>chnode2.marsnet.local</host>
                    <port>9440</port>
                    <user>default</user>
                    <password>ClickHouse123!</password>
                    <secure>1</secure>
                </replica>
            </shard>
        </cluster_1S_2R>
    </remote_servers>
```

### {uuid}を使用するためのテーブル設定手順
1. 各サーバーでマクロを設定します。サーバー1の例:
```xml
    <macros>
        <shard>1</shard>
        <replica>replica_1</replica>
    </macros>
```
:::note
`shard`と`replica`のマクロを定義することに注意してくださいが、`{uuid}`はここで定義されることはなく、それ自体で組み込まれているため、定義する必要はありません。
:::

2. データベースを作成する

```sql
CREATE DATABASE db_uuid
      ON CLUSTER 'cluster_1S_2R'
      ENGINE Atomic;
```

```response
CREATE DATABASE db_uuid ON CLUSTER cluster_1S_2R
ENGINE = Atomic

Query id: 07fb7e65-beb4-4c30-b3ef-bd303e5c42b5

┌─host──────────────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode2.marsnet.local │ 9440 │      0 │       │                   1 │                0 │
│ chnode1.marsnet.local │ 9440 │      0 │       │                   0 │                0 │
└───────────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
```

3. クラスタ上でマクロと`{uuid}`を使用してテーブルを作成する
```sql
CREATE TABLE db_uuid.uuid_table1 ON CLUSTER 'cluster_1S_2R'
   (
     id UInt64,
     column1 String
   )
   ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_uuid/{uuid}', '{replica}' )
   ORDER BY (id);
```

```response
CREATE TABLE db_uuid.uuid_table1 ON CLUSTER cluster_1S_2R
(
    `id` UInt64,
    `column1` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_uuid/{uuid}', '{replica}')
ORDER BY id

Query id: 8f542664-4548-4a02-bd2a-6f2c973d0dc4

┌─host──────────────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode1.marsnet.local │ 9440 │      0 │       │                   1 │                0 │
│ chnode2.marsnet.local │ 9440 │      0 │       │                   0 │                0 │
└───────────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
```

4.  分散テーブルを作成する
```sql
create table db_uuid.dist_uuid_table1 on cluster 'cluster_1S_2R'
   (
     id UInt64,
     column1 String
   )
   ENGINE = Distributed('cluster_1S_2R', 'db_uuid', 'uuid_table1' );
```

```response
CREATE TABLE db_uuid.dist_uuid_table1 ON CLUSTER cluster_1S_2R
(
    `id` UInt64,
    `column1` String
)
ENGINE = Distributed('cluster_1S_2R', 'db_uuid', 'uuid_table1')

Query id: 3bc7f339-ab74-4c7d-a752-1ffe54219c0e

┌─host──────────────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode2.marsnet.local │ 9440 │      0 │       │                   1 │                0 │
│ chnode1.marsnet.local │ 9440 │      0 │       │                   0 │                0 │
└───────────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘
```

### テスト
1. 最初のノードにデータを挿入する（例：`chnode1`）
```sql
INSERT INTO db_uuid.uuid_table1
   ( id, column1)
   VALUES
   ( 1, 'abc');
```

```response
INSERT INTO db_uuid.uuid_table1 (id, column1) FORMAT Values

Query id: 0f178db7-50a6-48e2-9a1b-52ed14e6e0f9

Ok.

1 row in set. Elapsed: 0.033 sec.
```

2. 2番目のノードにデータを挿入する（例：`chnode2`）
```sql
INSERT INTO db_uuid.uuid_table1
   ( id, column1)
   VALUES
   ( 2, 'def');
```

```response
INSERT INTO db_uuid.uuid_table1 (id, column1) FORMAT Values

Query id: edc6f999-3e7d-40a0-8a29-3137e97e3607

Ok.

1 row in set. Elapsed: 0.529 sec.
```

3. 分散テーブルを使用してレコードを表示する
```sql
SELECT * FROM db_uuid.dist_uuid_table1;
```

```response
SELECT *
FROM db_uuid.dist_uuid_table1

Query id: 6cbab449-9e7f-40fe-b8c2-62d46ba9f5c8

┌─id─┬─column1─┐
│  1 │ abc     │
└────┴─────────┘
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘

2 rows in set. Elapsed: 0.007 sec.
```

### 代替案
デフォルトのレプリケーションパスはマクロによって事前に定義され、`{uuid}`も使用されます。

1. 各ノードでテーブルのデフォルトを設定する
```xml
<default_replica_path>/clickhouse/tables/{shard}/db_uuid/{uuid}</default_replica_path>
```
```default_replica_name
<default_replica_name>{replica}</default_replica_name>
```
:::tip
ノードが特定のデータベースに使用される場合、各ノードでマクロ `{database}` を定義することもできます。
:::

2. パラメータを明示的に指定せずにテーブルを作成する:
```sql
CREATE TABLE db_uuid.uuid_table1 ON CLUSTER 'cluster_1S_2R'
   (
     id UInt64,
     column1 String
   )
   ENGINE = ReplicatedMergeTree
   ORDER BY (id);
```

```response
CREATE TABLE db_uuid.uuid_table1 ON CLUSTER cluster_1S_2R
(
    `id` UInt64,
    `column1` String
)
ENGINE = ReplicatedMergeTree
ORDER BY id

Query id: ab68cda9-ae41-4d6d-8d3b-20d8255774ee

┌─host──────────────────┬─port─┬─status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ chnode2.marsnet.local │ 9440 │      0 │       │                   1 │                0 │
│ chnode1.marsnet.local │ 9440 │      0 │       │                   0 │                0 │
└───────────────────────┴──────┴────────┴───────┴─────────────────────┴──────────────────┘

2 rows in set. Elapsed: 1.175 sec.
```

3. デフォルトの設定が使用されていることを確認する:
```sql
SHOW CREATE TABLE db_uuid.uuid_table1;
```

```response
SHOW CREATE TABLE db_uuid.uuid_table1

Query id: 5925ecce-a54f-47d8-9c3a-ad3257840c9e

┌─statement────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE db_uuid.uuid_table1
(
    `id` UInt64,
    `column1` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/db_uuid/{uuid}', '{replica}')
ORDER BY id
SETTINGS index_granularity = 8192 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.003 sec.
```

### トラブルシューティング

テーブル情報とUUIDを取得するためのコマンドの例:
```sql
SELECT * FROM system.tables
WHERE database = 'db_uuid' AND name = 'uuid_table1';
```

上記のテーブルのUUIDに関する情報をZooKeeperから取得するためのコマンドの例:
```sql
SELECT * FROM system.zookeeper
WHERE path = '/clickhouse/tables/1/db_uuid/9e8a3cc2-0dec-4438-81a7-c3e63ce2a1cf/replicas';
```

:::note
データベースは `Atomic` でなければなりません。以前のバージョンからアップグレードする場合、
`default` データベースはおそらく `Ordinary` タイプです。
:::

チェック方法の例:
```
SELECT name, engine FROM system.databases WHERE name = 'db_uuid';
```

```response
SELECT
    name,
    engine
FROM system.databases
WHERE name = 'db_uuid'

Query id: b047d459-a1d2-4016-bcf9-3e97e30e49c2

┌─name────┬─engine─┐
│ db_uuid │ Atomic │
└─────────┴────────┘

1 row in set. Elapsed: 0.004 sec.
```

## ClickHouse Keeperの動的再構成 {#reconfiguration}

<SelfManaged />

### 説明

ClickHouse Keeperは、動的クラスタ再構成のためにZooKeeperの[`reconfig`](https://zookeeper.apache.org/doc/r3.5.3-beta/zookeeperReconfig.html#sc_reconfig_modifying)
コマンドを部分的にサポートしています。`keeper_server.enable_reconfiguration` がオンになっている場合にのみ動作します。

:::note
この設定がオフの場合、レプリカの `raft_configuration` セクションを手動で変更してクラスタを再構成できます。
リーダーのみが変更を適用するため、すべてのレプリカのファイルを編集することを確認してください。
あるいは、ZooKeeper互換のクライアントを使用して `reconfig` クエリを送信することもできます。
:::

仮想ノード `/keeper/config` には、次の形式で最後にコミットされたクラスタの設定が含まれています:

```
server.id = server_host:server_port[;server_type][;server_priority]
server.id2 = ...
...
```

- 各サーバーエントリは改行で区切られています。
- `server_type` は `participant` または `learner` ([learner](https://github.com/eBay/NuRaft/blob/master/docs/readonly_member.md) はリーダー選出に参加しません) のいずれかです。
- `server_priority` は、[リーダー選出時に優先されるノードを評価する](https://github.com/eBay/NuRaft/blob/master/docs/leader_election_priority.md)非負の整数です。
  0 の優先度は、サーバーがリーダーにならないことを意味します。

例:

```
:) get /keeper/config
server.1=zoo1:9234;participant;1
server.2=zoo2:9234;participant;1
server.3=zoo3:9234;participant;1
```

`reconfig` コマンドを用いて、新しいサーバーを追加し、既存のサーバーを削除したり、
既存のサーバーの優先度を変更したりできます。以下に例を示します（`clickhouse-keeper-client`を使用）:

```bash
# 新しい2つのサーバーを追加
reconfig add "server.5=localhost:123,server.6=localhost:234;learner"
# 他の2つのサーバーを削除
reconfig remove "3,4"
# 既存のサーバーの優先度を8に変更
reconfig add "server.5=localhost:5123;participant;8"
```

こちらは`kazoo`の例です:

```python
# 新しい2つのサーバーを追加し、他の2つのサーバーを削除
reconfig(joining="server.5=localhost:123,server.6=localhost:234;learner", leaving="3,4")

# 既存のサーバーの優先度を8に変更
reconfig(joining="server.5=localhost:5123;participant;8", leaving=None)
```

参加するサーバーは上記のサーバーフォーマットで指定する必要があります。サーバーエントリはカンマで区切る必要があります。
新しいサーバーを追加する際、`server_priority`（デフォルト値は1）や`server_type`（デフォルト値は`participant`）は省略できます。

既存のサーバー優先度を変更したい場合、`joining`に対象の優先度で追加してください。
サーバーのホスト、ポート、タイプは既存のサーバー設定と等しくなければなりません。

サーバーは`joining`および`leaving`に記載された順に追加および削除されます。
`joining`からのすべてのアップデートは`leaving`からのアップデートよりも先に処理されます。

Keeper再構成の実装にはいくつかの注意点があります:

- 増分再構成のみがサポートされています。`new_members`が空でないリクエストは拒否されます。

  ClickHouse Keeperの実装は、会員を動的に変更するためにNuRaft APIに依存しています。NuRaftは、一度に1台のサーバーを追加または削除する方法を提供しています。つまり、構成への変更（`joining`の各部分、`leaving`の各部分）は個別に決定されなければなりません。従って、ユーザーにとって誤解を招くおそれがあるため、大量の再構成は利用できません。

  サーバータイプの変更（participant/learner）はNuRaftでサポートされていないためできません。また、サーバーを削除し追加する方法しかないため、これも誤解を招くおそれがあります。

- 返された `znodestat` 値は使用できません。
- `from_version` フィールドは使用されません。`from_version` を設定したリクエストはすべて拒否されます。
  これは `/keeper/config` が仮想ノードであり、永続ストレージに保存されるのではなく、
  指定されたノード設定に基づいてリクエストごとにリアルタイムで生成されるためです。この決定は、NuRaftがすでにこの構成を保存しているため、データの重複を防ぐために行われました。
- ZooKeeperとは異なり、`sync` コマンドを提出することでクラスタの再構成を待つことはできません。
  新しいコンフィグは_最終的に_適用されますが、時間保証はありません。
- `reconfig` コマンドはさまざまな理由で失敗することがあります。更新が適用されたかどうか、クラスタの状態を確認できます。

## シングルノードのKeeperをクラスターに変換する

時折、エクスペリメンタルなKeeperノードをクラスタに拡張する必要があります。3ノードクラスタへステップバイステップで変換する方法の概要は次のとおりです：

- **重要**: 新しいノードは、現在のクォーラムより少ないバッチで追加されなければなりません、さもないと内部でリーダーを選出します。ここでは一つずつ。
- 既存のKeeperノードは`keeper_server.enable_reconfiguration`設定パラメータをオンにする必要があります。
- 完全な新しいKeeperクラスタ設定で2番目のノードを起動する。
- 起動後、[reconfig](#reconfiguration)を使用してノード1に追加する。
- 次に、3番目のノードを起動し、[reconfig](#reconfiguration)を使用して追加する。
- 新しいKeeperノードを追加して`clickhouse-server`設定を更新し、設定を適用するために再起動する。
- ノード1のRaft設定を更新し、必要に応じて再起動する。

このプロセスに自信を持つために、[サンドボックスリポジトリ](https://github.com/ClickHouse/keeper-extend-cluster)を参照してください。

## 未サポートの機能

ClickHouse KeeperはZooKeeperと完全に互換性を持つことを目指していますが、現在実装されていない機能があります（開発中です）：

- [`create`](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#create(java.lang.String,byte%5B%5D,java.util.List,org.apache.zookeeper.CreateMode,org.apache.zookeeper.data.Stat)) は `Stat` オブジェクトの返却をサポートしていません
- [`create`](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#create(java.lang.String,byte%5B%5D,java.util.List,org.apache.zookeeper.CreateMode,org.apache.zookeeper.data.Stat)) は [TTL](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/CreateMode.html#PERSISTENT_WITH_TTL) をサポートしていません
- [`addWatch`](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#addWatch(java.lang.String,org.apache.zookeeper.Watcher,org.apache.zookeeper.AddWatchMode)) は [`PERSISTENT`](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/AddWatchMode.html#PERSISTENT) ウォッチでは機能しません
- [`removeWatch`](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#removeWatches(java.lang.String,org.apache.zookeeper.Watcher,org.apache.zookeeper.Watcher.WatcherType,boolean)) および [`removeAllWatches`](https://zookeeper.apache.org/doc/r3.9.1/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#removeAllWatches(java.lang.String,org.apache.zookeeper.Watcher.WatcherType,boolean)) はサポートされていません
- `setWatches` はサポートされていません
- [`CONTAINER`](https://zookeeper.apache.org/doc/r3.5.1-alpha/api/org/apache/zookeeper/CreateMode.html) タイプのznode作成はサポートされていません
- [`SASL認証`](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL) はサポートされていません
```
