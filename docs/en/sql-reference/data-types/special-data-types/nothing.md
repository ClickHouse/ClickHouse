---
description: 'Documentation for the Nothing special data type'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
---

# Nothing

The only purpose of this data type is to represent cases where a value is not expected. So you can't create a `Nothing` type value.

For example, literal [NULL](/sql-reference/syntax#null) has type of `Nullable(Nothing)`. See more about [Nullable](../../../sql-reference/data-types/nullable.md).

The `Nothing` type can also used to denote empty arrays:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```
