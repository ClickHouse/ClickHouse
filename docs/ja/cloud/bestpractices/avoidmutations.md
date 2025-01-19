---
slug: /ja/cloud/bestpractices/avoid-mutations
sidebar_label: ミューテーションを避ける
title: ミューテーションを避ける
---

ミューテーションとは、テーブルデータを削除または更新する [ALTER](/docs/ja/sql-reference/statements/alter/) クエリのことを指します。主に ALTER TABLE … DELETE, UPDATE などのクエリがこれに該当します。このようなクエリを実行すると、データパーツの新しい変異バージョンが生成されます。つまり、このようなステートメントはミューテーションの前に挿入されたすべてのデータに対してデータパーツの全体を書き直すことを引き起こし、大量の書き込み要求に繋がります。

更新に関しては、デフォルトの MergeTree のテーブルエンジンの代わりに、[ReplacingMergeTree](/docs/ja/engines/table-engines/mergetree-family/replacingmergetree.md) または [CollapsingMergeTree](/docs/ja/engines/table-engines/mergetree-family/collapsingmergetree.md) などの専用のテーブルエンジンを使用することで、この大量の書き込み要求を回避できます。

## 関連コンテンツ

- ブログ: [ClickHouseでの更新と削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
