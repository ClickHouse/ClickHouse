---
description: 'Documentation de CREATE NAMED COLLECTION'
sidebar_label: 'COLLECTION NOMMÉE'
slug: /sql-reference/statements/create/named-collection
title: 'CREATE NAMED COLLECTION'
doc_type: 'Référence'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="create-named-collection">
  # CREATE NAMED COLLECTION
</div>

Crée une nouvelle collection nommée.

**Syntaxe**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**Exemple**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**Instructions connexes**

* [CREATE NAMED COLLECTION](/fr/sql-reference/statements/alter/named-collection)
* [DROP NAMED COLLECTION](/fr/sql-reference/statements/drop#drop-function)

**Voir aussi**

* [Guide des collections nommées](/fr/operations/named-collections.md)