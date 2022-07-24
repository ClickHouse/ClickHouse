---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "\u4F55\u3082\u306A\u3044"
---

# 何もない {#nothing}

このデータ型の唯一の目的は、値が期待されないケースを表すことです。 だから、作成することはできません `Nothing` タイプ値。

たとえば、リテラル [NULL](../../../sql-reference/syntax.md#null-literal) タイプをの有する `Nullable(Nothing)`. 詳細はこちら [Null可能](../../../sql-reference/data-types/nullable.md).

その `Nothing` 型は空の配列を表すためにも使用できます:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[元の記事](https://clickhouse.com/docs/en/data_types/special_data_types/nothing/) <!--hide-->
