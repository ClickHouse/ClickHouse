---
slug: /ja/sql-reference/statements/alter/constraint
sidebar_position: 43
sidebar_label: CONSTRAINT
---

# Constraintの操作

制約は次の構文を使用して追加または削除できます。

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name CHECK expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

詳細は[制約](../../../sql-reference/statements/create/table.md#constraints)を参照してください。

クエリはテーブルの制約に関するメタデータを追加または削除しますので、それらは即座に処理されます。

:::tip
制約チェックは、追加された場合でも**既存のデータには実行されません**。
:::

レプリケートされたテーブルに対するすべての変更はZooKeeperに通知され、他のレプリカにも適用されます。
