---
slug: /ja/sql-reference/statements/alter/delete
sidebar_position: 39
sidebar_label: DELETE
---

# ALTER TABLE ... DELETE ステートメント

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

指定されたフィルタリング式に一致するデータを削除します。これは[ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

:::note
`ALTER TABLE` プレフィックスは、SQLをサポートする他の多くのシステムとこの構文を異ならせます。これは、OLTPデータベースの類似のクエリとは異なり、頻繁に使用することを意図していない重い操作であることを示すためです。`ALTER TABLE` は、基礎データが削除される前にマージされる必要がある重い操作と見なされます。MergeTree テーブルの場合は、論理削除を行い、かなり速く操作できる [`DELETE FROM` クエリ](/docs/ja/sql-reference/statements/delete.md) の使用を検討してください。
:::

`filter_expr` は `UInt8` 型である必要があります。この式がゼロ以外の値を取るテーブルの行を削除します。

1つのクエリには、コンマで区切られた複数のコマンドを含めることができます。

クエリ処理の同期性は、[mutations_sync](/docs/ja/operations/settings/settings.md/#mutations_sync) 設定によって定義されます。デフォルトでは非同期です。

**関連情報**

- [ミューテーション](/docs/ja/sql-reference/statements/alter/index.md#mutations)
- [ALTER クエリの同期性](/docs/ja/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [mutations_sync](/docs/ja/operations/settings/settings.md/#mutations_sync) 設定

## 関連コンテンツ

- ブログ: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
