---
description: '削除済み行のマスク適用に関するドキュメント'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: '削除済み行のマスクを適用'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

このコマンドは、[論理削除](/ja/sql-reference/statements/delete)で作成されたマスクを適用し、削除済みとしてマークされた行をディスクから強制的に削除します。このコマンドは高負荷なミューテーションであり、意味的にはクエリ `ALTER TABLE [db].name DELETE WHERE _row_exists = 0` と同等です。

:::note
これは[`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md)ファミリーのテーブル ([レプリケートされた](../../../engines/table-engines/mergetree-family/replication.md)テーブルを含む) でのみ動作します。
:::

**関連項目**

* [論理削除](/ja/sql-reference/statements/delete)
* [高負荷な削除](/ja/sql-reference/statements/alter/delete.md)