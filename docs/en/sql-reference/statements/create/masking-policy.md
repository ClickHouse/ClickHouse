---
description: 'Documentation for Masking Policy'
sidebar_label: 'MASKING POLICY'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge/>

Creates a masking policy, which allows dynamically transforming or masking column values for specific users or roles when they query a table.

:::tip
Masking policies provide column-level data security by transforming sensitive data at query time without modifying the stored data.
:::

Syntax:

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

## UPDATE Clause {#update-clause}

The `UPDATE` clause specifies which columns to mask and how to transform them. You can mask multiple columns in a single policy.

Examples:
- Simple masking: `UPDATE email = '***masked***'`
- Partial masking: `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
- Hash-based masking: `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
- Multiple columns: `UPDATE email = '***@***.***', phone = '***-***-****'`

## WHERE Clause {#where-clause}

The optional `WHERE` clause allows conditional masking based on row values. Only rows matching the condition will have the masking applied.

Example:
```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

## TO Clause {#to-clause}

In the `TO` section, specify which users and roles the policy should apply to.

- `TO user1, user2`: Apply to specific users/roles
- `TO ALL`: Apply to all users
- `TO ALL EXCEPT user1, user2`: Apply to all users except specified ones

:::note
Unlike row policies, masking policies do not affect users who don't have the policy applied. If no masking policy applies to a user, they see the original data.
:::

## PRIORITY Clause {#priority-clause}

When multiple masking policies target the same column for a user, the `PRIORITY` clause determines the application order. Policies are applied in order from highest to lowest priority.

Default priority is 0. Policies with the same priority are applied in an undefined order.

Example:
```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note Performance Considerations
- Masking policies may impact query performance depending on expression complexity
- Some optimizations may be disabled for tables with active masking policies
:::
