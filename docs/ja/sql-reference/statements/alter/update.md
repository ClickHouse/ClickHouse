---
slug: /ja/sql-reference/statements/alter/update
sidebar_position: 40
sidebar_label: UPDATE
---

# ALTER TABLE ... UPDATE 文

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

指定されたフィルタリング式に一致するデータを操作します。これは[mutation](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

:::note    
`ALTER TABLE`というプレフィックスが付いていることで、SQLをサポートする他の多くのシステムとは異なる構文となっています。これは、OLTPデータベースの類似したクエリと異なり、頻繁な使用を目的としない重い操作であることを示すために意図されています。
:::

`filter_expr`は型`UInt8`である必要があります。このクエリは、`filter_expr`がゼロ以外の値を取る行の指定されたカラムの値を対応する式の値に更新します。値は`CAST`演算子を使用してカラム型にキャストされます。主キーまたはパーティションキーの計算に使用されるカラムを更新することはサポートされていません。

1つのクエリには、カンマで区切られた複数のコマンドを含めることができます。

クエリ処理の同期性は、[mutations_sync](/docs/ja/operations/settings/settings.md/#mutations_sync)設定によって定義されます。デフォルトでは非同期です。

**関連情報**

- [Mutations](/docs/ja/sql-reference/statements/alter/index.md#mutations)
- [ALTER クエリの同期性](/docs/ja/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [mutations_sync](/docs/ja/operations/settings/settings.md/#mutations_sync) 設定


## 関連コンテンツ

- ブログ: [ClickHouseでの更新と削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
