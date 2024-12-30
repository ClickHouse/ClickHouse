---
slug: /ja/operations/system-tables/zookeeper
---
# zookeeper

このテーブルは、ClickHouse Keeper または ZooKeeper が設定されていない限り存在しません。`system.zookeeper` テーブルは、configで定義された Keeper クラスターからのデータを公開します。以下に示すように、クエリには `WHERE` 条項で `path =` 条件または `path IN` 条件を設定する必要があります。これは、データを取得したい子ノードのパスに対応します。

クエリ `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` は、`/clickhouse` ノードのすべての子に関するデータを出力します。すべてのルートノードに対するデータを出力するには、`path = '/'` と書きます。`path` で指定したパスが存在しない場合、例外がスローされます。

クエリ `SELECT * FROM system.zookeeper WHERE path IN ('/', '/clickhouse')` は、`/` と `/clickhouse` ノードのすべての子に対するデータを出力します。指定した `path` コレクション内に存在しないパスがある場合、例外がスローされます。これは、複数の Keeper パス クエリを実行するために使用できます。

カラム:

- `name` (String) — ノードの名前。
- `path` (String) — ノードへのパス。
- `value` (String) — ノードの値。
- `dataLength` (Int32) — 値のサイズ。
- `numChildren` (Int32) — 子孫の数。
- `czxid` (Int64) — ノードを作成したトランザクションのID。
- `mzxid` (Int64) — 最後にノードを変更したトランザクションのID。
- `pzxid` (Int64) — 最後に子孫を削除または追加したトランザクションのID。
- `ctime` (DateTime) — ノード作成の時間。
- `mtime` (DateTime) — ノードの最後の変更時間。
- `version` (Int32) — ノードのバージョン: ノードが変更された回数。
- `cversion` (Int32) — 追加または削除された子孫の数。
- `aversion` (Int32) — ACLの変更回数。
- `ephemeralOwner` (Int64) — 一時ノードの場合、このノードを所有するセッションのID。

例:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
行 1:
──────
name:           example01-08-1
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

行 2:
──────
name:           example01-08-2
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```
