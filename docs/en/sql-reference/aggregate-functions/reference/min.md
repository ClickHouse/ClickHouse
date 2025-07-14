---
slug: /en/sql-reference/aggregate-functions/reference/min
sidebar_position: 168
title: min
---

Aggregate function that calculates the minimum across a group of values.

Example:

```
SELECT min(salary) FROM employees;
```

```
SELECT department, min(salary) FROM employees GROUP BY department;
```

If you need non-aggregate function to choose a minimum of two values, see `least`:

```
SELECT least(a, b) FROM table;
```
