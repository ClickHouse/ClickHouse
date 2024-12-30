---
slug: /ja/sql-reference/statements/select/qualify
sidebar_label: QUALIFY
---

# QUALIFY句

ウィンドウ関数の結果をフィルタリングすることができます。これは[WHERE](../../../sql-reference/statements/select/where.md)句に似ていますが、違いは`WHERE`がウィンドウ関数を評価する前に実行されるのに対し、`QUALIFY`はそれが評価された後に実行される点です。

`SELECT`句でエイリアスを使用して、`QUALIFY`句からウィンドウ関数の結果を参照することができます。もしくは、クエリの結果に含まれない追加のウィンドウ関数の結果に基づいてフィルタリングすることも可能です。

## 制限事項

評価すべきウィンドウ関数がない場合、`QUALIFY`は使用できません。その場合は`WHERE`を使用してください。

## 例

例:

``` sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

``` text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```
