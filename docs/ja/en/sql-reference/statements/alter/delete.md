---
description: 'ALTER TABLE ... DELETE ステートメントに関するドキュメント'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'ALTER TABLE ... DELETE ステートメント'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

指定した filtering expression に一致するデータを削除します。[mutation](/ja/sql-reference/statements/alter/index.md#mutations) として実装されています。

:::note
`ALTER TABLE` プレフィックスがあるため、この構文は SQL をサポートする他の多くのシステムとは異なります。これは、OLTP データベースの類似のクエリとは異なり、頻繁な使用を想定していない高コストな操作であることを示すためのものです。`ALTER TABLE` は、削除前に基になるデータを merge する必要がある heavyweight な操作と見なされます。MergeTree テーブルでは、論理削除を行い、大幅に高速になる可能性がある [`DELETE FROM` query](/ja/sql-reference/statements/delete.md) の使用を検討してください。
:::

`filter_expr` は `UInt8` 型でなければなりません。この expression が 0 以外の値を返すテーブル内の行が、クエリによって削除されます。

1 つのクエリには、カンマで区切られた複数のコマンドを含めることができます。

クエリ処理の同期性は、[mutations&#95;sync](/ja/operations/settings/settings.md/#mutations_sync) setting で定義されます。デフォルトでは非同期です。

**関連項目**

* [Mutations](/ja/sql-reference/statements/alter/index.md#mutations)
* [ALTER クエリの同期性](/ja/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* [mutations&#95;sync](/ja/operations/settings/settings.md/#mutations_sync) setting

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse における更新と削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)