---
description: 'Documentation for ALL Clause'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'ALL Clause'
---

# ALL Clause

If there are multiple matching rows in the table, then `ALL` returns all of them. `SELECT ALL` is identical to `SELECT` without `DISTINCT`. If both `ALL` and `DISTINCT` specified, exception will be thrown.


`ALL` can also be specified inside aggregate function with the same effect(noop), for instance:

```sql
SELECT sum(ALL number) FROM numbers(10);
```
equals to

```sql
SELECT sum(number) FROM numbers(10);
```
