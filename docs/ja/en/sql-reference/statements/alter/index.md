---
description: 'ALTER のドキュメント'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

ほとんどの `ALTER TABLE` クエリは、テーブル設定またはデータを変更します。

| Modifier                                                                    |
| --------------------------------------------------------------------------- |
| [COLUMN](/ja/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/ja/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/ja/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/ja/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/ja/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/ja/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/ja/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/ja/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/ja/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/ja/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/ja/sql-reference/statements/alter/apply-patches.md)           |

:::note
ほとんどの `ALTER TABLE` クエリは、[*MergeTree](/ja/engines/table-engines/mergetree-family/index.md)、[Merge](/ja/engines/table-engines/special/merge.md)、および [Distributed](/ja/engines/table-engines/special/distributed.md) テーブルでのみサポートされています。
:::

これらの `ALTER` ステートメントはビューを操作します。

| Statement                                                               | Description                                                           |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY QUERY](/ja/sql-reference/statements/alter/view.md) | [materialized view](/ja/sql-reference/statements/create/view) の構造を変更します。 |

これらの `ALTER` ステートメントは、ロールベースのアクセス制御に関連するエンティティを変更します。

| Statement                                                               |
| ----------------------------------------------------------------------- |
| [USER](/ja/sql-reference/statements/alter/user.md)                         |
| [ROLE](/ja/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/ja/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/ja/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/ja/sql-reference/statements/alter/settings-profile.md) |

| Statement                                                                     | Description                                                   |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/ja/sql-reference/statements/alter/comment.md)  | 以前に設定されていたかどうかにかかわらず、テーブルのコメントを追加、変更、または削除します。                |
| [ALTER NAMED COLLECTION](/ja/sql-reference/statements/alter/named-collection.md) | [Named Collections](/ja/operations/named-collections.md) を変更します。 |

<div id="mutations">
  ## ミューテーション
</div>

テーブルデータを操作することを目的とした `ALTER` クエリは、「ミューテーション」と呼ばれる仕組みで実装されています。代表的なものは [ALTER TABLE ... DELETE](/ja/sql-reference/statements/alter/delete.md) と [ALTER TABLE ... UPDATE](/ja/sql-reference/statements/alter/update.md) です。これらは [MergeTree](/ja/engines/table-engines/mergetree-family/index.md) テーブルのマージに似た非同期のバックグラウンド処理で、新しい「mutated」バージョンのパーツを生成します。

`*MergeTree` テーブルでは、ミューテーションは **データパーツ全体を書き換える**ことで実行されます。
アトミック性はありません。mutated パーツの準備ができ次第、元のパーツはそれに置き換えられます。そのため、ミューテーションの実行中に開始された `SELECT` クエリでは、すでにミューテーション済みのパーツのデータと、まだミューテーションされていないパーツのデータの両方が見えることになります。

ミューテーションは作成順に完全に順序付けられ、その順序で各パーツに適用されます。また、`INSERT INTO` クエリとは部分的な順序関係があります。つまり、ミューテーションが送信される前にテーブルへ挿入されたデータはミューテーションの対象になりますが、それ以降に挿入されたデータは対象になりません。なお、ミューテーションによって insert がブロックされることはありません。

ミューテーションクエリは、ミューテーションのエントリが追加されるとすぐに返ります (レプリケートテーブルの場合は ZooKeeper に、非レプリケートテーブルの場合は filesystem に追加されます) 。ミューテーション自体は、system profile の設定を使用して非同期に実行されます。ミューテーションの進行状況を追跡するには、[`system.mutations`](/ja/operations/system-tables/mutations) テーブルを使用できます。正常に送信されたミューテーションは、ClickHouse servers が再起動された場合でも実行が継続されます。いったん送信されたミューテーションをロールバックする方法はありませんが、何らかの理由でミューテーションが停止している場合は、[`KILL MUTATION`](/ja/sql-reference/statements/kill.md/#kill-mutation) クエリでキャンセルできます。

完了したミューテーションのエントリは、すぐには削除されません (保持されるエントリ数は `finished_mutations_to_keep` ストレージエンジンのパラメーターで決まります) 。古いミューテーションのエントリは削除されます。

<div id="synchronicity-of-alter-queries">
  ## ALTERクエリの同期性
</div>

非レプリケートテーブルでは、すべての `ALTER` クエリは同期的に実行されます。レプリケートテーブルでは、クエリは対応する操作の命令を `ZooKeeper` に追加するだけで、実際の操作は可能な限り速やかに実行されます。ただし、クエリがそれらの操作の完了をすべてのレプリカで待機することもできます。

ミューテーション を作成する `ALTER` クエリ (`UPDATE`、`DELETE`、`MATERIALIZE INDEX`、`MATERIALIZE PROJECTION`、`MATERIALIZE COLUMN`、`APPLY DELETED MASK`、`APPLY PATCHES`、`CLEAR STATISTIC`、`MATERIALIZE STATISTIC` など) については、同期性は [mutations&#95;sync](/ja/operations/settings/settings.md/#mutations_sync) 設定で定義されます。

メタデータのみを変更するその他の `ALTER` クエリについては、待機方法の設定に [alter&#95;sync](/ja/operations/settings/settings#alter_sync) 設定を使用できます。

非アクティブなレプリカがすべての `ALTER` クエリを実行するまでの待機時間 (秒) は、[replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ja/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 設定で指定できます。

:::note
すべての `ALTER` クエリにおいて、`alter_sync = 2` であり、かつ一部のレプリカが `replication_wait_for_inactive_replica_timeout` 設定で指定された時間を超えて非アクティブな場合は、例外 `UNFINISHED` がスローされます。
:::

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseにおける更新と削除への対応](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)