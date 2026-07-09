---
description: 'SAMPLE BY 式の操作に関するドキュメント'
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: 'サンプリングキー式の操作'
doc_type: 'reference'
---

以下の操作を使用できます。

<div id="modify">
  ## MODIFY
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

このコマンドは、テーブルの[サンプリングキー](../../../engines/table-engines/mergetree-family/mergetree.md)を`new_expression` (式、または式のタプル) に変更します。主キーには、新しいサンプリングキーを含める必要があります。

<div id="remove">
  ## REMOVE
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

このコマンドは、テーブルの[サンプリングキー](../../../engines/table-engines/mergetree-family/mergetree.md)を削除します。

`MODIFY` コマンドと `REMOVE` コマンドは、メタデータの変更またはファイルの削除しか行わないため、軽量です。

:::note
これは、[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブル ([レプリケート](../../../engines/table-engines/mergetree-family/replication.md)テーブルを含む) でのみ使用できます。
:::