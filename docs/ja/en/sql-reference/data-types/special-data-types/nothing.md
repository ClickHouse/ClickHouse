---
description: 'Nothing データ型のドキュメント'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'reference'
---

このデータ型の唯一の用途は、値が存在しないことが想定されるケースを表すことです。したがって、`Nothing` 型の値を作成することはできません。

たとえば、リテラル [NULL](/ja/sql-reference/syntax#null) の型は `Nullable(Nothing)` です。[Nullable](../../../sql-reference/data-types/nullable.md) も参照してください。

`Nothing` 型は、空の配列を表すためにも使用できます。

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```