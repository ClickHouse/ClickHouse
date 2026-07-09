---
description: 'データスキッピングインデックスの操作に関するドキュメント'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: 'データスキッピングインデックスの操作'
toc_hidden_folder: true
doc_type: 'reference'
---

次の操作を使用できます:

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - テーブルのメタデータに索引の定義を追加します。

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - テーブルのメタデータから索引の定義を削除し、ディスク上の索引ファイルを削除します。[ミューテーション](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 指定した`partition_name`のセカンダリ索引`name`を再構築します。[ミューテーション](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。`IN PARTITION`の部分を省略した場合は、テーブル全体のデータに対して索引を再構築します。

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 説明を削除せずに、ディスク上のセカンダリ索引ファイルを削除します。[ミューテーション](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

`ADD`、`DROP`、`CLEAR` コマンドは、メタデータを変更するかファイルを削除するだけなので、軽量です。
また、これらはレプリケーションされ、ClickHouse Keeper または ZooKeeper を介して索引のメタデータを同期します。

:::note
索引の操作は、[`*MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree.md) エンジン ([replicated](/ja/engines/table-engines/mergetree-family/replication.md) バリアントを含む) を使用するテーブルでのみサポートされています。
:::