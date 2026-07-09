---
description: 'Documentación sobre ALTER MASKING POLICY'
sidebar_label: 'MASKING POLICY'
sidebar_position: 48
slug: /sql-reference/statements/alter/masking-policy
title: 'ALTER MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="alter-masking-policy">
  # ALTER MASKING POLICY
</div>

Modifica una política de enmascaramiento existente.

Sintaxis:

```sql
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
```

Todas las cláusulas son opcionales. Solo se actualizarán las cláusulas especificadas.