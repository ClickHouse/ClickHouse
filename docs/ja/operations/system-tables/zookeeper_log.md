---
slug: /ja/operations/system-tables/zookeeper_log
---
# zookeeper_log

このテーブルは、ZooKeeperサーバーへのリクエストのパラメーターとそこからの応答に関する情報を含んでいます。

リクエストの場合、リクエストパラメーターがあるカラムのみが填められ、残りのカラムはデフォルト値（`0` または `NULL`）で埋められます。応答が到着すると、応答のデータが他のカラムに追加されます。

リクエストパラメーターを持つカラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行するサーバーのホスト名。
- `type` ([Enum](../../sql-reference/data-types/enum.md)) — ZooKeeperクライアントのイベントタイプ。以下の値を持つことができます:
  - `Request` — リクエストが送信されました。
  - `Response` — 応答が受信されました。
  - `Finalize` — 接続が失われ、応答が受信されませんでした。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — イベントが発生した日付。
- `event_time` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — イベントが発生した日時。
- `address` ([IPv6](../../sql-reference/data-types/ipv6.md)) — リクエストを行ったZooKeeperサーバーのIPアドレス。
- `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — リクエストを行ったZooKeeperサーバーのポート。
- `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — ZooKeeperサーバーが接続ごとに設定するセッションID。
- `xid` ([Int32](../../sql-reference/data-types/int-uint.md)) — セッション内のリクエストのID。通常は連続したリクエスト番号です。リクエスト行と対応する `response`/`finalize` 行で同じ。
- `has_watch` ([UInt8](../../sql-reference/data-types/int-uint.md)) — [watch](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches)が設定されているかどうか。
- `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — リクエストまたは応答のタイプ。
- `path` ([String](../../sql-reference/data-types/string.md)) — リクエストで指定されたZooKeeperノードのパス、またはパスの指定が不要なリクエストの場合は空文字列。
- `data` ([String](../../sql-reference/data-types/string.md)) — ZooKeeperノードへのデータ（`SET`および`CREATE`リクエストの場合は書き込もうとしたデータ、`GET`リクエストへの応答の場合は読み取られたデータ）または空文字列。
- `is_ephemeral` ([UInt8](../../sql-reference/data-types/int-uint.md)) — ZooKeeperノードが[ephemeral](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Ephemeral+Nodes)として作成されているか。
- `is_sequential` ([UInt8](../../sql-reference/data-types/int-uint.md)) — ZooKeeperノードが[sequential](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Sequence+Nodes+--+Unique+Naming)として作成されているか。
- `version` ([Nullable(Int32)](../../sql-reference/data-types/nullable.md)) — リクエスト実行時に期待されるZooKeeperノードのバージョン。この情報は`CHECK`, `SET`, `REMOVE`リクエストでサポートされます（リクエストがバージョンをチェックしない場合は`-1`、バージョンチェックをサポートしていない他のリクエストの場合は `NULL`）。
- `requests_size` ([UInt32](../../sql-reference/data-types/int-uint.md)) — マルチリクエストに含まれるリクエストの数（複数の連続した通常のリクエストから構成され、これらを原子的に実行する特別なリクエスト）。マルチリクエストに含まれるすべてのリクエストは同じ `xid` を持ちます。
- `request_idx` ([UInt32](../../sql-reference/data-types/int-uint.md)) — マルチリクエストに含まれるリクエストの番号（マルチリクエストの場合 `0`、その後は順番に`1`から）。

リクエスト応答パラメータを持つカラム:

- `zxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — ZooKeeperトランザクションID。正常に実行されたリクエストに応答してZooKeeperサーバーから発行されたシリアル番号（リクエストが実行されなかった/エラーが返された/クライアントがリクエストの実行を知っていない場合は`0`）。
- `error` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — エラーコード。多くの値を持つことができますが、以下はその一部です:
  - `ZOK` — リクエストは正常に実行されました。
  - `ZCONNECTIONLOSS` — 接続が失われました。
  - `ZOPERATIONTIMEOUT` — リクエスト実行のタイムアウトが切れました。
  - `ZSESSIONEXPIRED` — セッションが失効しました。
  - `NULL` — リクエストは完了しました。
- `watch_type` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — `watch`イベントのタイプ（`op_num`が`Watch`の応答の場合）、それ以外の応答の場合：`NULL`。
- `watch_state` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — `watch`イベントのステータス（`op_num`が`Watch`の応答の場合）、それ以外の応答の場合：`NULL`。
- `path_created` ([String](../../sql-reference/data-types/string.md)) — 作成されたZooKeeperノードのパス（`CREATE`リクエストへの応答の場合）、ノードが`sequential`として作成された場合は`path`と異なることがあります。
- `stat_czxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードの作成を引き起こした変更の`zxid`。
- `stat_mzxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードの最後の変更を行った`zxid`。
- `stat_pzxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードの子の最後の変更のトランザクションID。
- `stat_version` ([Int32](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードのデータの変更回数。
- `stat_cversion` ([Int32](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードの子の変更回数。
- `stat_dataLength` ([Int32](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードのデータフィールドの長さ。
- `stat_numChildren` ([Int32](../../sql-reference/data-types/int-uint.md)) — このZooKeeperノードの子の数。
- `children` ([Array(String)](../../sql-reference/data-types/array.md)) — 子ZooKeeperノードのリスト（`LIST`リクエストへの応答の場合）。

**例**

クエリ:

``` sql
SELECT * FROM system.zookeeper_log WHERE (session_id = '106662742089334927') AND (xid = '10858') FORMAT Vertical;
```

結果:

``` text
Row 1:
──────
hostname:         clickhouse.eu-central1.internal
type:             Request
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.291792
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             0
error:            ᴺᵁᴸᴸ
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:
stat_czxid:       0
stat_mzxid:       0
stat_pzxid:       0
stat_version:     0
stat_cversion:    0
stat_dataLength:  0
stat_numChildren: 0
children:         []

Row 2:
──────
type:             Response
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.292086
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             16926267
error:            ZOK
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:
stat_czxid:       16925469
stat_mzxid:       16925469
stat_pzxid:       16926179
stat_version:     0
stat_cversion:    7
stat_dataLength:  0
stat_numChildren: 7
children:         ['query-0000000006','query-0000000005','query-0000000004','query-0000000003','query-0000000002','query-0000000001','query-0000000000']
```

**関連項目**

- [ZooKeeper](../../operations/tips.md#zookeeper)
- [ZooKeeperガイド](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
