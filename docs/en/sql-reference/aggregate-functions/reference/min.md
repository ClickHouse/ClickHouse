---
description: 'Aggregate function that calculates the minimum across a group of values.'
sidebar_position: 168
slug: /sql-reference/aggregate-functions/reference/min
title: 'min'
---

Aggregate function that calculates the minimum across a group of values.

Example:

```sql
SELECT min(salary) FROM employees;
```

```sql
SELECT department, min(salary) FROM employees GROUP BY department;
```

If you need non-aggregate function to choose a minimum of two values, see `least`:

```sql
SELECT least(a, b) FROM table;
```
