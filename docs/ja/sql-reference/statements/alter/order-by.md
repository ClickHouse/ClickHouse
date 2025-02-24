---
slug: /ja/sql-reference/statements/alter/order-by
sidebar_position: 41
sidebar_label: ORDER BY
---

# キー式の操作

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

このコマンドは、テーブルの[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)を`new_expression`（式または式のタプル）に変更します。主キーは同じままです。

このコマンドは軽量で、メタデータのみを変更します。データ部分の行がソートキーの式で整列されるという特性を保持するために、既存のカラムを含む式をソートキーに追加することはできません（デフォルトのカラム値なしで、同じ`ALTER`クエリ内で`ADD COLUMN`コマンドによって追加されたカラムのみ）。

:::note    
これは[`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md)ファミリーのテーブル（[レプリケーション](../../../engines/table-engines/mergetree-family/replication.md)されたテーブルを含む）でのみ機能します。
:::
