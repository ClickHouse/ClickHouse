---
description: '論理更新のパッチを適用するためのドキュメント'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: '論理更新のパッチを適用'
doc_type: 'リファレンス'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

このコマンドは、[論理更新 `UPDATE`](/ja/sql-reference/statements/update)ステートメントによって作成されたパッチパートの物理的なマテリアライズを手動でトリガーします。影響を受けるカラムだけを書き換えることで、保留中のパッチをデータパーツに強制的に適用します。

:::note

* [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md)ファミリーのテーブル ([replicated](../../../engines/table-engines/mergetree-family/replication.md)テーブルを含む) でのみ使用できます。
* これはミューテーション操作であり、バックグラウンドで非同期に実行されます。
  :::

<div id="when-to-use">
  ## APPLY PATCHES を使用する場合
</div>

:::tip
通常、`APPLY PATCHES` を使う必要はありません
:::

パッチパートは通常、[`apply_patches_on_merge`](/ja/operations/settings/merge-tree-settings#apply_patches_on_merge) 設定が有効 (デフォルト) な場合、マージ中に自動的に適用されます。ただし、次のようなケースでは、パッチの適用を手動でトリガーしたいことがあります。

* `SELECT` クエリ中にパッチを適用する際のオーバーヘッドを減らすため
* 複数のパッチパートが蓄積する前に集約するため
* パッチがすでにマテリアライズされた状態で、バックアップやエクスポートに向けてデータを準備するため
* `apply_patches_on_merge` が無効で、パッチを適用するタイミングを自分で制御したい場合

<div id="examples">
  ## 例
</div>

テーブルの未適用のパッチをすべて適用します:

```sql
ALTER TABLE my_table APPLY PATCHES;
```

特定のパーティションにのみパッチを適用します:

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

他の操作と組み合わせてください：

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## パッチ適用状況の監視
</div>

パッチ適用の進行状況は、[`system.mutations`](/ja/operations/system-tables/mutations) テーブルを使用して監視できます。

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## 関連項目
</div>

* [論理更新 `UPDATE`](/ja/sql-reference/statements/update) - 論理更新によってパッチパートを作成
* [`apply_patches_on_merge` 設定](/ja/operations/settings/merge-tree-settings#apply_patches_on_merge) - マージ時のパッチの自動適用を制御