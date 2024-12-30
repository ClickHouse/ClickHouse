---
slug: /ja/sql-reference/data-types/special-data-types/nothing
sidebar_position: 60
sidebar_label: Nothing
---

# Nothing

このデータ型の唯一の目的は、値が期待されない場合を表すことです。そのため、`Nothing` 型の値を作成することはできません。

たとえば、リテラル [NULL](../../../sql-reference/syntax.md#null-literal) は `Nullable(Nothing)` 型を持っています。[Nullable](../../../sql-reference/data-types/nullable.md) について詳しくは参照してください。

`Nothing` 型は、空の配列を示すためにも使用されます:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

