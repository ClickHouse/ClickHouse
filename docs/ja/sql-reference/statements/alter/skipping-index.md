---
slug: /ja/sql-reference/statements/alter/skipping-index

toc_hidden_folder: true
sidebar_position: 42
sidebar_label: INDEX
---

# データスキップインデックスの操作

以下の操作が利用可能です。

## ADD INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - テーブルのメタデータにインデックスの説明を追加します。

## DROP INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - テーブルのメタデータからインデックスの説明を削除し、ディスクからインデックスファイルを削除します。[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

## MATERIALIZE INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 指定された`partition_name`に対して二次インデックス`name`を再構築します。[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。`IN PARTITION`部分が省略された場合、テーブル全体のデータに対してインデックスを再構築します。

## CLEAR INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - ディスクから二次インデックスファイルを削除しますが、説明は削除しません。[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

コマンド`ADD`、`DROP`、`CLEAR`は軽量で、メタデータの変更やファイルの削除のみを行います。また、これらはレプリケートされており、インデックスのメタデータをClickHouse KeeperやZooKeeperを通じて同期します。

:::note    
インデックス操作は、[`*MergeTree`](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)エンジン（[replicated](/docs/ja/engines/table-engines/mergetree-family/replication.md)バリアントを含む）を持つテーブルでのみサポートされています。
:::
