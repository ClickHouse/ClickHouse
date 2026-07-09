---
description: 'カラム STATISTICS の操作に関するドキュメント'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: 'カラム STATISTICS の操作'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # カラム STATISTICS の操作
</div>

<CloudNotSupportedBadge />

以下の操作を利用できます。

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - テーブルのメタデータに統計情報の定義を追加します。

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - テーブルのメタデータ内の統計情報の定義を変更します。

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - 指定したカラムのメタデータから統計情報を削除し、指定したカラムに対応するすべてのパーツ内の統計オブジェクトもすべて削除します。

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - 指定したカラムに対応するすべてのパーツ内の統計オブジェクトをすべて削除します。統計オブジェクトは `ALTER TABLE MATERIALIZE STATISTICS` を使用して再構築できます。

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - カラムの統計情報を再構築します。[mutation](../../../sql-reference/statements/alter/index.md#mutations) として実装されています。

最初の 2 つのコマンドは、メタデータの変更またはファイルの削除のみを行うため、軽量です。

また、これらはレプリケートされ、ZooKeeper 経由で統計メタデータが同期されます。

<div id="example">
  ## 例:
</div>

2 つのカラムに 2 種類の統計を追加します:

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
統計は、[`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) エンジンのテーブル ([レプリケーション対応](../../../engines/table-engines/mergetree-family/replication.md) のバリアントを含む) でのみサポートされています。
:::