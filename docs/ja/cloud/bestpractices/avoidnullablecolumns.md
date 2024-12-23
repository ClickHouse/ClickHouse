---
slug: /ja/cloud/bestpractices/avoid-nullable-columns
sidebar_label: Nullableカラムを避ける
title: Nullableカラムを避ける
---

[`Nullable`カラム](/docs/ja/sql-reference/data-types/nullable/)（例: `Nullable(String)`)は`UInt8`型の別カラムを作成します。この追加のカラムは、ユーザーがNullableカラムを操作するたびに処理される必要があります。これにより、追加のストレージスペースが必要になり、ほとんどの場合、パフォーマンスに悪影響を及ぼします。

`Nullable`カラムを避けるには、そのカラムにデフォルト値を設定することを検討してください。例えば、以下のようにする代わりに：

```sql
CREATE TABLE default.sample
(
    `x` Int8,
    # highlight-next-line
    `y` Nullable(Int8)
)
ENGINE = MergeTree
ORDER BY x
```
次のように使用します

```sql
CREATE TABLE default.sample2
(
    `x` Int8,
    # highlight-next-line
    `y` Int8 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY x
```

:::note
ユースケースを考慮し、デフォルト値が不適切である可能性もあります。
:::
