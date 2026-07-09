---
description: 'Distributed DDL のドキュメント'
sidebar_label: 'Distributed DDL'
sidebar_position: 3
slug: /sql-reference/distributed-ddl
title: '分散 DDL クエリ（ON CLUSTER 句）'
doc_type: 'reference'
---

デフォルトでは、`CREATE`、`DROP`、`ALTER`、`RENAME` クエリは、実行された現在のサーバーにのみ影響します。クラスター構成では、`ON CLUSTER` 句を使用して、これらのクエリを分散実行できます。

たとえば、次のクエリは `cluster` 内の各ホストに `all_hits` `Distributed` テーブルを作成します。

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

これらのクエリを正しく実行するには、各ホストで同じクラスター定義が設定されている必要があります (設定の同期を簡単にするために、ZooKeeper の置換機能を使用できます) 。また、各ホストは ZooKeeper サーバーに接続できる必要があります。

クエリのローカル版は、一部のホストが現在利用できない場合でも、最終的にはクラスター内の各ホストで実行されます。

:::important
1 台のホスト内でのクエリ実行順序は保証されます。
:::