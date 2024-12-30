---
slug: /ja/sql-reference/statements/alter/apply-deleted-mask
sidebar_position: 46
sidebar_label: APPLY DELETED MASK
---

# 削除された行のマスクを適用

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

このコマンドは、[論理削除](/docs/ja/sql-reference/statements/delete)によって作成されたマスクを適用し、削除としてマークされた行をディスクから強制的に削除します。このコマンドは重量級の変換であり、その意味論的にはクエリ ```ALTER TABLE [db].name DELETE WHERE _row_exists = 0``` に等しいです。

:::note
この機能は、[`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブル（[レプリケーションされた](../../../engines/table-engines/mergetree-family/replication.md)テーブルを含む）のみで機能します。
:::

**関連項目**

- [論理削除](/docs/ja/sql-reference/statements/delete)
- [物理削除](/docs/ja/sql-reference/statements/alter/delete.md)
