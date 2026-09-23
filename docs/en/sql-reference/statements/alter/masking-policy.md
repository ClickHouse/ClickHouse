---
description: 'Documentation for ALTER MASKING POLICY'
sidebar_label: 'MASKING POLICY'
sidebar_position: 48
slug: /sql-reference/statements/alter/masking-policy
title: 'ALTER MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge/>

# ALTER MASKING POLICY

Modifies an existing masking policy.

Syntax:

```sql
ALTER MASKING POLICY [IF EXISTS] policy_name ON [database.]table
    [UPDATE column1 = expression1 [, column2 = expression2 ...]]
    [WHERE condition]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
    [PRIORITY priority_number]
```

All clauses are optional. Only the specified clauses will be updated.
