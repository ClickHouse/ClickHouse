---
slug: /en/sql-reference/statements/alter/named-collection
sidebar_label: NAMED COLLECTION
---

# ALTER NAMED COLLECTION

This query intends to modify already existing named collections.

**Syntax**

```sql
ALTER NAMED COLLECTION [IF EXISTS] name [ON CLUSTER cluster]
[ SET
key_name1 = 'some value',
key_name2 = 'some value',
key_name3 = 'some value',
... ] |
[ DELETE key_name4, key_name5, ... ]
```

**Example**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';

ALTER NAMED COLLECTION foobar SET a = '2', c = '3';

ALTER NAMED COLLECTION foobar DELETE b;
```
