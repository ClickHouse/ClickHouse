---
slug: /ja/operations/system-tables/clusters
---
# clusters

コンフィグファイルに記載されている利用可能なクラスターとそのサーバーについての情報を含んでいます。

カラム:

- `cluster` ([String](../../sql-reference/data-types/string.md)) — クラスター名。
- `shard_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — クラスター内のシャード番号。1から始まります。
- `shard_weight` ([UInt32](../../sql-reference/data-types/int-uint.md)) — データを書き込む際のシャードの相対的な重み。
- `replica_num` ([UInt32](../../sql-reference/data-types/int-uint.md)) — シャード内のレプリカ番号。1から始まります。
- `host_name` ([String](../../sql-reference/data-types/string.md)) — コンフィグで指定されたホスト名。
- `host_address` ([String](../../sql-reference/data-types/string.md)) — DNSから取得したホストIPアドレス。
- `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — サーバーへの接続に使用するポート。
- `is_local` ([UInt8](../../sql-reference/data-types/int-uint.md)) — ホストがローカルであるかどうかを示すフラグ。
- `user` ([String](../../sql-reference/data-types/string.md)) — サーバーへの接続に使用するユーザー名。
- `default_database` ([String](../../sql-reference/data-types/string.md)) — デフォルトのデータベース名。
- `errors_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — このホストでレプリカに到達するのに失敗した回数。
- `slowdowns_count` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ヘッジリクエストで接続を確立する際にレプリカを変更することになった遅延の回数。
- `estimated_recovery_time` ([UInt32](../../sql-reference/data-types/int-uint.md)) — レプリカエラーカウントがゼロになり、正常に戻るまでの残り秒数。
- `database_shard_name` ([String](../../sql-reference/data-types/string.md)) — `Replicated` データベースシャードの名前（`Replicated` データベースに属するクラスターの場合）。
- `database_replica_name` ([String](../../sql-reference/data-types/string.md)) — `Replicated` データベースレプリカの名前（`Replicated` データベースに属するクラスターの場合）。
- `is_active` ([Nullable(UInt8)](../../sql-reference/data-types/int-uint.md)) — `Replicated` データベースレプリカの状態（`Replicated` データベースに属するクラスターの場合）。1は「レプリカがオンライン」、0は「レプリカがオフライン」、`NULL` は「不明」を示します。
- `name` ([String](../../sql-reference/data-types/string.md)) - クラスターへのエイリアス。

**例**

クエリ:

```sql
SELECT * FROM system.clusters LIMIT 2 FORMAT Vertical;
```

結果:

```text
行 1:
────
cluster:                 test_cluster_two_shards
shard_num:               1
shard_weight:            1
replica_num:             1
host_name:               127.0.0.1
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
database_shard_name:
database_replica_name:
is_active:               NULL

行 2:
────
cluster:                 test_cluster_two_shards
shard_num:               2
shard_weight:            1
replica_num:             1
host_name:               127.0.0.2
host_address:            127.0.0.2
port:                    9000
is_local:                0
user:                    default
default_database:
errors_count:            0
slowdowns_count:         0
estimated_recovery_time: 0
database_shard_name:
database_replica_name:
is_active:               NULL
```

**参照**

- [テーブルエンジン Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap 設定](../../operations/settings/settings.md#distributed_replica_error_cap)
- [distributed_replica_error_half_life 設定](../../operations/settings/settings.md#distributed_replica_error_half_life)
