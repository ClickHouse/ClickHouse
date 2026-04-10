---
description: 'Documentation for ALTER NAMED COLLECTION'
sidebar_label: 'NAMED COLLECTION'
slug: /sql-reference/statements/alter/named-collection
title: 'ALTER NAMED COLLECTION'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

# ALTER NAMED COLLECTION

This query intends to modify already existing named collections.

**Syntax**

```sql
ALTER NAMED COLLECTION [IF EXISTS] name [ON CLUSTER cluster]
[ SET
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
... ] |
[ DELETE key_name4, key_name5, ... ]
```

**Example**

```sql
CREATE NAMED COLLECTION foobar AS a = '1' NOT OVERRIDABLE, b = '2';

ALTER NAMED COLLECTION foobar SET a = '2' OVERRIDABLE, c = '3';

ALTER NAMED COLLECTION foobar DELETE b;
```
