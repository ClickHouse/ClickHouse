---
slug: /ja/sql-reference/table-functions/cluster
sidebar_position: 30
sidebar_label: cluster
title: "cluster, clusterAllReplicas"
---

クラスタのすべてのシャード（`remote_servers`セクションで設定）が、[分散テーブル](../../engines/table-engines/special/distributed.md)を作成せずにアクセスできます。各シャードのレプリカのうち1つだけがクエリされます。

`clusterAllReplicas`関数 — `cluster`と同様ですが、すべてのレプリカがクエリされます。クラスタ内の各レプリカが個別のシャード/接続として使用されます。

:::note
利用可能なすべてのクラスタは[system.clusters](../../operations/system-tables/clusters.md)テーブルにリストされています。
:::

**構文**

``` sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```
**引数**

- `cluster_name` – リモートおよびローカルサーバーへのアドレスと接続パラメータのセットを構築するために使用されるクラスタの名前。指定しない場合は`default`が設定されます。
- `db.table` または `db`, `table` - データベースとテーブルの名前。
- `sharding_key` - シャーディングキー。任意。クラスタが複数のシャードを持つ場合に指定が必要です。

**返される値**

クラスタからのデータセット。

**マクロの使用**

`cluster_name`にはマクロを含めることができます。波括弧の中にある値は、サーバー設定ファイルの[macros](../../operations/server-configuration-parameters/settings.md#macros)セクションからの置換値です。

例:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

**使用法と推奨事項**

`cluster`および`clusterAllReplicas`テーブル関数を利用することは、`Distributed`テーブルを作成するよりも効率が劣ります。というのは、この場合、要求ごとにサーバー接続が再確立されるためです。大量のクエリを処理する際は、常に事前に`Distributed`テーブルを作成し、`cluster`および`clusterAllReplicas`テーブル関数を使用しないでください。

`cluster`および`clusterAllReplicas`テーブル関数は、次のような場合に有用です：

- 特定のクラスタへのアクセスを通じたデータの比較、デバッグ、およびテスト。
- 研究目的でのさまざまなClickHouseクラスタやレプリカへのクエリ。
- 手動で実行されるまれな分散要求。

接続設定（`host`、`port`、`user`、`password`、`compression`、`secure`など）は`<remote_servers>`構成セクションから取得されます。[Distributedエンジン](../../engines/table-engines/special/distributed.md)の詳細を参照してください。

**関連項目**

- [skip_unavailable_shards](../../operations/settings/settings.md#skip_unavailable_shards)
- [load_balancing](../../operations/settings/settings.md#load_balancing)
