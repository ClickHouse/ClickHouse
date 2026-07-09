---
description: 'Nothing 数据类型文档'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'reference'
---

此数据类型的唯一用途是表示不应出现值的情况。因此，你无法创建 `Nothing` 类型的值。

例如，字面量 [NULL](/zh/sql-reference/syntax#null) 的类型为 `Nullable(Nothing)`。有关 [Nullable](../../../sql-reference/data-types/nullable.md) 的更多信息，请参见相关文档。

`Nothing` 类型也可用于表示空数组：

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```