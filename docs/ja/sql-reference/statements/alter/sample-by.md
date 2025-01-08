---
slug: /ja/sql-reference/statements/alter/sample-by
sidebar_position: 41
sidebar_label: SAMPLE BY
title: "SAMPLE BY式の操作"
---

# SAMPLE BY式の操作

以下の操作が利用可能です：

## 変更

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

このコマンドは、テーブルの[サンプリングキー](../../../engines/table-engines/mergetree-family/mergetree.md)を`new_expression`（式または式のタプル）に変更します。主キーは新しいサンプルキーを含む必要があります。

## 削除

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

このコマンドは、テーブルの[サンプリングキー](../../../engines/table-engines/mergetree-family/mergetree.md)を削除します。

`MODIFY`および`REMOVE`コマンドは、メタデータの変更またはファイルの削除に限定されるため、軽量です。

:::note    
この機能は[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)ファミリーのテーブル（[レプリケート](../../../engines/table-engines/mergetree-family/replication.md)テーブルを含む）のみで動作します。
:::
