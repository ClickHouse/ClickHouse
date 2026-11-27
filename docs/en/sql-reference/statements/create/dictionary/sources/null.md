---
slug: /sql-reference/statements/create/dictionary/sources/null
title: 'Null dictionary source'
sidebar_position: 14
sidebar_label: 'Null'
description: 'Configure a Null (empty) dictionary source in ClickHouse for testing.'
doc_type: 'reference'
---

A special source that can be used to create dummy (empty) dictionaries.
Dummy dictionaries can be useful for testing purposes or for setups with separate data and query nodes with distributed tables.

```sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```
