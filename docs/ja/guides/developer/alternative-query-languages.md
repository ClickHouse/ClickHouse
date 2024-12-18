---
slug: /ja/guides/developer/alternative-query-languages
sidebar_label: 代替クエリ言語
title: 代替クエリ言語
description: ClickHouseで代替クエリ言語を使用する
---

`dialect` 設定を使用して、ClickHouseで他のクエリ言語を使用してデータをクエリすることができます。
`dialect` を変更した後、新しく設定された方言でクエリを実行できます。

現在サポートされている方言は次の通りです:
- `clickhouse`: デフォルトの [ClickHouse SQL 方言](../../sql-reference/syntax.md)

エクスペリメンタルな方言:
- `prql`: [Pipelined Relational Query Language](https://prql-lang.org/)
- `kusto`: [Kusto Query Language (KQL)](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query)

### ClickHouse SQL

ClickHouseのデフォルトのSQL方言です。

```sql
SET dialect = 'clickhouse'
```

## エクスペリメンタルな方言

これらの方言は完全にはサポートされていないか、またはその元の仕様のすべての機能を持っていない可能性があります。

### Pipelined Relational Query Language (PRQL)

方言を `prql` に設定した後、PRQL言語を使用してクエリを実行することができます:
```sql
SET dialect = 'prql'
```

その後、組み込まれているコンパイラがサポートするすべてのPRQL機能を使用できます:

```prql
from trips
aggregate {
    ct = count this
    total_days = sum days 
}
```

内部的にはClickHouseがPRQLクエリをSQLクエリに変換して実行します。

### Kusto Query Language (KQL)

KustoがClickHouseで定義されているすべての関数にアクセスできるわけではないかもしれません。

Kustoを有効にする:
```sql
SET dialect = 'kusto'
```

`system.numbers(10)` から選択するクエリの例:
```sql
numbers(10) | project number
```

```sql
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```


