---
slug: /sql-reference/aggregate-functions/reference/max
sidebar_position: 162
title: max
description: "Aggregate function that calculates the maximum across a group of values."
---

Aggregate function that calculates the maximum across a group of values.

Example:

```sql
SELECT max(salary) FROM employees;
```

```sql
SELECT department, max(salary) FROM employees GROUP BY department;
```

If you need non-aggregate function to choose a maximum of two values, see `greatest`:

```sql
SELECT greatest(a, b) FROM table;
```
