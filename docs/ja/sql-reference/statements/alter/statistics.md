---
slug: /ja/sql-reference/statements/alter/statistics
sidebar_position: 45
sidebar_label: STATISTICS
---

# カラム統計の操作

以下の操作が利用可能です:

-   `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (カラムリスト) TYPE (種類リスト)` - 統計の説明をテーブルのメタデータに追加します。

-   `ALTER TABLE [db].table MODIFY STATISTICS (カラムリスト) TYPE (種類リスト)` - 統計の説明をテーブルのメタデータに変更します。

-   `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (カラムリスト)` - 指定されたカラムのメタデータから統計を削除し、指定されたカラムの全パーツの統計オブジェクトをすべて削除します。

-   `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (カラムリスト)` - 指定されたカラムの全パーツの統計オブジェクトをすべて削除します。統計オブジェクトは `ALTER TABLE MATERIALIZE STATISTICS` を使用して再構築できます。

-   `ALTER TABLE [db.]table MATERIALIZE STATISTICS [IF EXISTS] (カラムリスト)` - カラムの統計を再構築します。[ミューテーション](../../../sql-reference/statements/alter/index.md#mutations)として実装されています。

最初の2つのコマンドは、メタデータの変更やファイルの削除のみを行うため、軽量です。

また、これらはZooKeeperを介して統計メタデータを同期するレプリケーションが行われます。

## 例:

2つの統計タイプを2つのカラムに追加する例:

```
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
統計は、高度な*MergeTree（[レプリケート](../../../engines/table-engines/mergetree-family/replication.md)バリアントを含む）エンジンテーブルのみをサポートしています。
:::

