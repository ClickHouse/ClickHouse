---
description: 'キー式の操作に関するドキュメント'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: 'キー式の操作'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

このコマンドは、テーブルの[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)を `new_expression` (1 つの式、または複数の式のタプル) に変更します。主キーは変わりません。

このコマンドは、変更されるのがメタデータだけであるという意味で軽量です。データパートの行がソートキー式に従って順序付けされるという性質を維持するため、既存のカラムを含む式をソートキーに追加することはできません (追加できるのは、同じ `ALTER` クエリ内の `ADD COLUMN` コマンドで追加され、かつデフォルトのカラム値を持たないカラムのみです) 。

:::note
これは [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブル ([レプリケーションされた](../../../engines/table-engines/mergetree-family/replication.md)テーブルを含む) でのみ機能します。
:::