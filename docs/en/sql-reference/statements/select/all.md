---
description: 'Documentation for ALL Clause'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'ALL Clause'
doc_type: 'reference'
---

# ALL Clause

If there are multiple matching rows in a table, then `ALL` returns all of them. `SELECT ALL` is identical to `SELECT` without `DISTINCT`. If both `ALL` and `DISTINCT` are specified, then an exception will be thrown.

`ALL` can be specified inside aggregate functions, although it has no practical effect on the query's result.

For example:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

Is equivalent to:

```sql
SELECT sum(number) FROM numbers(10);
```
