---
description: 'Documentation de référence pour ALTER politique de masquage'
sidebar_label: 'politique de masquage'
sidebar_position: 48
slug: /sql-reference/statements/alter/masking-policy
title: 'ALTER politique de masquage'
doc_type: 'référence'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="alter-masking-policy">
  # ALTER MASKING POLICY
</div>

Modifie une politique de masquage existante.

Syntaxe :

```sql
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
```

Toutes les clauses sont facultatives. Seules les clauses indiquées seront mises à jour.