---
description: 'Documentation for CREATE NAMED COLLECTION'
sidebar_label: 'NAMED COLLECTION'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

# CREATE NAMED COLLECTION

Creates a new named collection.

**Syntax**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

Overriding a stored key when using the collection requires `SHOW NAMED COLLECTIONS SECRETS` on that collection,
including keys marked `OVERRIDABLE`. Keys marked `NOT OVERRIDABLE` cannot be overridden.
Dictionary sources follow the same rule. The privilege is checked when the dictionary is created, attached, or restored.
When the dictionary is loaded, only keys marked `NOT OVERRIDABLE` are enforced.

**Example**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**Related statements**

- [CREATE NAMED COLLECTION](/sql-reference/statements/alter/named-collection)
- [DROP NAMED COLLECTION](/sql-reference/statements/drop#drop-function)

**See Also**

- [Named collections guide](/operations/named-collections.md)
