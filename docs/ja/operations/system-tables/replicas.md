---
slug: /ja/operations/system-tables/replicas
---
# replicas

このテーブルには、ローカルサーバーにあるレプリケーションされたテーブルの情報とステータスが含まれています。このテーブルは監視に利用できます。テーブルにはすべてのReplicated\*テーブルについて行が含まれています。

例:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'test_table'
FORMAT Vertical
```

``` text
クエリID: dc6dcbcb-dc28-4df9-ae27-4354f5b3b13e

行1:
───────
database:                    db
table:                       test_table
engine:                      ReplicatedMergeTree
is_leader:                   1
can_become_leader:           1
is_readonly:                 0
is_session_expired:          0
future_parts:                0
parts_to_check:              0
zookeeper_path:              /test/test_table
replica_name:                r1
replica_path:                /test/test_table/replicas/r1
columns_version:             -1
queue_size:                  27
inserts_in_queue:            27
merges_in_queue:             0
part_mutations_in_queue:     0
queue_oldest_time:           2021-10-12 14:48:48
inserts_oldest_time:         2021-10-12 14:48:48
merges_oldest_time:          1970-01-01 03:00:00
part_mutations_oldest_time:  1970-01-01 03:00:00
oldest_part_to_get:          1_17_17_0
oldest_part_to_merge_to:
oldest_part_to_mutate_to:
log_max_index:               206
log_pointer:                 207
last_queue_update:           2021-10-12 14:50:08
absolute_delay:              99
total_replicas:              5
active_replicas:             5
lost_part_count:             0
last_queue_update_exception:
zookeeper_exception:
replica_is_active:           {'r1':1,'r2':1}
```

カラム:

- `database` (`String`) - データベース名
- `table` (`String`) - テーブル名
- `engine` (`String`) - テーブルエンジン名
- `is_leader` (`UInt8`) - レプリカがリーダーかどうか。
    複数のレプリカが同時にリーダーになることができます。レプリカがリーダーになるのを防ぐために`merge_tree`設定`replicated_can_become_leader`を使用できます。リーダーはバックグラウンドでのマージをスケジュールする責任を持ちます。
    Zookeeperにセッションを持ち利用可能である限り、リーダーであるかどうかに関わらず、任意のレプリカに書き込みができます。
- `can_become_leader` (`UInt8`) - レプリカがリーダーになれるかどうか。
- `is_readonly` (`UInt8`) - レプリカが読み取り専用モードかどうか。
    このモードは、ClickHouse Keeperのセクションが設定に存在しない場合、セッションをClickHouse Keeperで再初期化する際に未知のエラーが発生した場合、またはClickHouse Keeperでセッションを再初期化する際に有効になります。
- `is_session_expired` (`UInt8`) - ClickHouse Keeperとのセッションが期限切れになったかどうか。基本的には`is_readonly`と同じです。
- `future_parts` (`UInt32`) - 挿入や未完了のマージの結果として現れるデータパーツの数。
- `parts_to_check` (`UInt32`) - 検証中のデータパーツの数。破損している可能性がある場合に検証キューに登録されます。
- `zookeeper_path` (`String`) - ClickHouse Keeperにおけるテーブルデータへのパス。
- `replica_name` (`String`) - ClickHouse Keeperにおけるレプリカ名。同じテーブルの異なるレプリカは異なる名前を持ちます。
- `replica_path` (`String`) - ClickHouse Keeperにおけるレプリカデータへのパス。これは‘zookeeper_path/replicas/replica_path’を連結したものと同じです。
- `columns_version` (`Int32`) - テーブル構造のバージョン番号。ALTERが実行された回数を示します。レプリカが異なるバージョンを持っている場合、一部のレプリカがまだすべてのALTERを行っていないことを意味します。
- `queue_size` (`UInt32`) - 実行待ちの操作のキューのサイズ。操作にはデータブロックの挿入、マージ、および特定の他のアクションが含まれます。通常は`future_parts`と一致します。
- `inserts_in_queue` (`UInt32`) - 挿入される必要のあるデータブロックの挿入数。挿入は通常かなり迅速にレプリケートされます。この数が大きい場合、何か問題があります。
- `merges_in_queue` (`UInt32`) - 実行待ちのマージの数。マージは時折長引くため、この値がしばらくゼロを超えることがあります。
- `part_mutations_in_queue` (`UInt32`) - 実行待ちの変異の数。
- `queue_oldest_time` (`DateTime`) - `queue_size`がゼロを超える場合、最も古い操作がキューに追加された時刻を示します。
- `inserts_oldest_time` (`DateTime`) - `queue_oldest_time`を参照
- `merges_oldest_time` (`DateTime`) - `queue_oldest_time`を参照
- `part_mutations_oldest_time` (`DateTime`) - `queue_oldest_time`を参照

次の4つのカラムは、Zookeeperとのアクティブなセッションがある場合にのみ非ゼロの値を持ちます。

- `log_max_index` (`UInt64`) - 一般活動のログにおける最大のエントリ番号。
- `log_pointer` (`UInt64`) - レプリカが実行キューにコピーした一般活動のログの最大エントリ番号に1を加えたもの。`log_pointer`が`log_max_index`よりはるかに小さい場合、何かがおかしいです。
- `last_queue_update` (`DateTime`) - キューが最後に更新された時刻。
- `absolute_delay` (`UInt64`) - 現在のレプリカが持つ遅延秒数。
- `total_replicas` (`UInt8`) - このテーブルの既知のレプリカの総数。
- `active_replicas` (`UInt8`) - ClickHouse Keeperにセッションのある、このテーブルのレプリカ数（つまり、稼働中のレプリカの数）。
- `lost_part_count` (`UInt64`) - テーブルが作成されて以来、全レプリカで合計としてテーブル内で失われたデータパーツの数。値はClickHouse Keeperに保存され増加のみされます。
- `last_queue_update_exception` (`String`) - キューに壊れたエントリが含まれている場合。特に、ClickHouseがバージョン間で後方互換性を破る際に重要であり、新しいバージョンで書かれたログエントリが古いバージョンで解析できない場合に特に注意します。
- `zookeeper_exception` (`String`) - ClickHouse Keeperから情報を取得する際にエラーが発生した場合に得られた最後の例外メッセージ。
- `replica_is_active` ([Map(String, UInt8)](../../sql-reference/data-types/map.md)) — レプリカ名とレプリカがアクティブかどうかのマップ。

すべてのカラムをリクエストすると、各行ごとにClickHouse Keeperから複数回の読み取りが行われるため、テーブルの動作が多少遅くなる場合があります。最後の4つのカラム（log_max_index, log_pointer, total_replicas, active_replicas）をリクエストしなければ、テーブルは速く動作します。

例えば、すべてが正常に動作しているかを次のようにチェックできます:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

このクエリが何も返さなければ、すべてが正常です。
