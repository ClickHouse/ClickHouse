---
description: 'Documentación de CREATE NAMED COLLECTION'
sidebar_label: 'NAMED COLLECTION'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

Crea una nueva colección nombrada.

**Sintaxis**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**Ejemplo**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**Sentencias relacionadas**

* [CREATE NAMED COLLECTION](/es/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/es/sql-reference/statements/drop#drop-function)

**Véase también**

* [Guía de colecciones con nombre](/es/operations/named-collections.md)