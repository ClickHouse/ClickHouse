---
slug: /ja/sql-reference/statements/alter/
sidebar_position: 35
sidebar_label: ALTER
---

# ALTER

ほとんどの`ALTER TABLE`クエリはテーブルの設定やデータを変更します:

- [カラム](/docs/ja/sql-reference/statements/alter/column.md)
- [パーティション](/docs/ja/sql-reference/statements/alter/partition.md)
- [DELETE](/docs/ja/sql-reference/statements/alter/delete.md)
- [UPDATE](/docs/ja/sql-reference/statements/alter/update.md)
- [ORDER BY](/docs/ja/sql-reference/statements/alter/order-by.md)
- [インデックス](/docs/ja/sql-reference/statements/alter/skipping-index.md)
- [制約](/docs/ja/sql-reference/statements/alter/constraint.md)
- [有効期限 (TTL)](/docs/ja/sql-reference/statements/alter/ttl.md)
- [統計](/docs/ja/sql-reference/statements/alter/statistics.md)
- [DELETED MASKの適用](/docs/ja/sql-reference/statements/alter/apply-deleted-mask.md)

:::note
ほとんどの`ALTER TABLE`クエリは、[\*MergeTree](/docs/ja/engines/table-engines/mergetree-family/index.md)テーブル、[Merge](/docs/ja/engines/table-engines/special/merge.md)および[分散テーブル](/docs/ja/engines/table-engines/special/distributed.md)でのみサポートされています。
:::

これらの`ALTER`ステートメントはビューを操作します：

- [ALTER TABLE ... MODIFY QUERY](/docs/ja/sql-reference/statements/alter/view.md) — [Materialized View](/docs/ja/sql-reference/statements/create/view.md/#materialized)構造を変更します。
- [ALTER LIVE VIEW](/docs/ja/sql-reference/statements/alter/view.md/#alter-live-view) — [Live View](/docs/ja/sql-reference/statements/create/view.md/#live-view)を更新します。

これらの`ALTER`ステートメントは、ロールベースのアクセス制御に関連するエンティティを修正します：

- [USER](/docs/ja/sql-reference/statements/alter/user.md)
- [ROLE](/docs/ja/sql-reference/statements/alter/role.md)
- [QUOTA](/docs/ja/sql-reference/statements/alter/quota.md)
- [行のポリシー](/docs/ja/sql-reference/statements/alter/row-policy.md)
- [設定プロファイル](/docs/ja/sql-reference/statements/alter/settings-profile.md)

[ALTER TABLE ... MODIFY COMMENT](/docs/ja/sql-reference/statements/alter/comment.md)ステートメントは、テーブルに対しコメントを追加、修正、または削除します。これは、以前に設定されていたかどうかに関係なく行われます。

[ALTER NAMED COLLECTION](/docs/ja/sql-reference/statements/alter/named-collection.md)ステートメントは[Named Collections](/docs/ja/operations/named-collections.md)を修正します。

## 変異（Mutations）

テーブルデータを操作することを意図した`ALTER`クエリは、「変異」と呼ばれるメカニズムで実装されています。代表的なものに、[ALTER TABLE ... DELETE](/docs/ja/sql-reference/statements/alter/delete.md)や[ALTER TABLE ... UPDATE](/docs/ja/sql-reference/statements/alter/update.md)があります。これらは[MergeTree](/docs/ja/engines/table-engines/mergetree-family/index.md)テーブルのマージに似た非同期のバックグラウンドプロセスで、新しい「変化した」パーツのバージョンを生成します。

`*MergeTree`テーブルでは、変異は**データ全体のパーツを書き換えること**で実行されます。アトミシティはなく、パーツは変異された部分が準備ができ次第、入れ替えられ、変異中に開始された`SELECT`クエリは既に変異されたパーツからのデータと、まだ変異されていないパーツからのデータの両方を参照します。

変異はその作成順に完全に順序付けられ、その順で各パーツに適用されます。また、変異は`INSERT INTO`クエリと部分的に順序付けられます：変異が提出される前にテーブルに挿入されたデータは変異され、後に挿入されたデータは変異されません。なお、変異はインサートを一切ブロックしません。

変異クエリは、変異エントリが追加されるとすぐに（レプリケートされたテーブルの場合はZooKeeper、非レプリケートされたテーブルの場合はファイルシステムに）応答を返します。変異自体はシステムのプロファイル設定を使用して非同期で実行されます。変異の進行状況を追跡するためには、[`system.mutations`](/docs/ja/operations/system-tables/mutations.md/#system_tables-mutations)テーブルを使用できます。正常に提出された変異は、ClickHouseサーバーが再起動されても実行を続けます。変異が一度提出されると巻き戻す方法はありませんが、何らかの理由で変異が詰まった場合は、[`KILL MUTATION`](/docs/ja/sql-reference/statements/kill.md/#kill-mutation)クエリでキャンセルできます。

終了した変異のエントリはすぐに削除されることはなく、保存されるエントリ数は`finished_mutations_to_keep`ストレージエンジンパラメータで決定されます。古い変異エントリは削除されます。

## ALTERクエリの同期性（Synchronicity）

非レプリケートテーブルでは、すべての`ALTER`クエリは同期的に実行されます。レプリケートされたテーブルでは、クエリは適切なアクションに対する指示を`ZooKeeper`に追加するだけで、そのアクション自体はできるだけ早く実行されます。ただし、クエリはこれらのアクションがすべてのレプリカで完了するのを待つことができます。

変異を作成する`ALTER`クエリ（例：`UPDATE`、`DELETE`、`MATERIALIZE INDEX`、`MATERIALIZE PROJECTION`、`MATERIALIZE COLUMN`、`APPLY DELETED MASK`、`CLEAR STATISTIC`、`MATERIALIZE STATISTIC`を含むがこれに限定されない）の同期性は、[mutations_sync](/docs/ja/operations/settings/settings.md/#mutations_sync)設定で定義されます。

メタデータのみを修正するその他の`ALTER`クエリについては、[alter_sync](/docs/ja/operations/settings/settings.md/#alter-sync)設定を使用して待機を設定できます。

非アクティブなレプリカがすべての`ALTER`クエリを実行するのを待機する時間（秒）を指定するには、[replication_wait_for_inactive_replica_timeout](/docs/ja/operations/settings/settings.md/#replication-wait-for-inactive-replica-timeout)設定を使用できます。

:::note
すべての`ALTER`クエリで、`alter_sync = 2`かつ`replication_wait_for_inactive_replica_timeout`設定で指定された時間を超えて非アクティブなレプリカがある場合、例外`UNFINISHED`がスローされます。
:::

## 関連コンテンツ

- ブログ: [ClickHouseにおける更新と削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
