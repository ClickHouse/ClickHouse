---
slug: /ja/sql-reference/distributed-ddl
sidebar_position: 3
sidebar_label: 分散DDL
---

# 分散DDLクエリ (ON CLUSTER句)

デフォルトでは、`CREATE`、`DROP`、`ALTER`、および`RENAME`クエリは、その実行が行われたサーバーのみに影響します。クラスタ設定では、`ON CLUSTER`句を用いて、これらのクエリを分散的に実行することが可能です。

例えば、以下のクエリは、`cluster`内の各ホストに`all_hits`という`分散テーブル`を作成します。

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

これらのクエリを正しく実行するためには、各ホストが同じクラスタ定義を持っている必要があります（設定ファイルの同期を簡略化するためにZooKeeperの代替機能を使用できます）。また、ZooKeeperサーバーへの接続も必要です。

クエリのローカルバージョンは、クラスタ内の各ホストで最終的に実行されますが、たとえ一部のホストが現在利用可能でなくても、後で実行されます。

:::important    
単一ホスト内でのクエリ実行の順序は保証されます。
:::
