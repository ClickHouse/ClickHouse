---
description: 'ALTER TABLE ... UPDATE 文に関するドキュメント'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'ALTER TABLE ... UPDATE 文'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

指定したフィルタ式に一致するデータを変更します。[mutation](/ja/sql-reference/statements/alter/index.md#mutations) として実装されています。

:::note
`ALTER TABLE` プレフィックスにより、この構文は SQL をサポートする他の多くのシステムとは異なります。これは、OLTP データベースの類似したクエリとは異なり、頻繁な実行を想定していない重い操作であることを示すためのものです。
:::

`filter_expr` は `UInt8` 型でなければなりません。このクエリは、`filter_expr` が 0 以外に評価される行について、指定したカラムの値を対応する式の値に更新します。値は `CAST` 演算子を使用してカラム型にキャストされます。プライマリキーまたはパーティションキーの計算に使用されるカラムの更新はサポートされていません。

1 つのクエリに、カンマで区切られた複数のコマンドを含めることができます。

クエリ処理の同期性は、[mutations&#95;sync](/ja/operations/settings/settings.md/#mutations_sync) 設定によって決まります。デフォルトでは非同期です。

**関連項目**

* [Mutations](/ja/sql-reference/statements/alter/index.md#mutations)
* [ALTER クエリの同期性](/ja/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* [mutations&#95;sync](/ja/operations/settings/settings.md/#mutations_sync) 設定
* [論理更新 `UPDATE`](/ja/sql-reference/statements/update) - パッチパートを使用する代替の軽量更新
* [`APPLY PATCHES`](/ja/sql-reference/statements/alter/apply-patches) - 論理更新のパッチを手動で適用

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseにおける更新と削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)