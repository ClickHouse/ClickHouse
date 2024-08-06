---
slug: /en/sql-reference/statements/create/named-collection
sidebar_label: NAMED COLLECTION
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

**Example**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**Related statements**

- [CREATE NAMED COLLECTION](https://clickhouse.com/docs/en/sql-reference/statements/alter/named-collection)
- [DROP NAMED COLLECTION](https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-function)


**See Also**

- [Named collections guide](/docs/en/operations/named-collections.md)
