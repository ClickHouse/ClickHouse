---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 60
toc_title: "\u4F55\u3082\u306A\u3044"
---

# 何もない {#nothing}

このデータ型の唯一の目的は、値が予期されないケースを表すことです。 だから、作成することはできません `Nothing` タイプ値。

たとえば、リテラル [NULL](../../../sql-reference/syntax.md#null-literal) はタイプの `Nullable(Nothing)`. 詳細はこちら [Nullable](../../../sql-reference/data-types/nullable.md).

その `Nothing` 型は、空の配列を示すためにも使用できます:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/data_types/special_data_types/nothing/) <!--hide-->
