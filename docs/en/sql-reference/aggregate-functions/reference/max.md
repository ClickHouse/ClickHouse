---
sidebar_position: 3
---

# max {#agg_function-max}

Aggregate function that calculates the maximum across a group of values.

Example:

```
SELECT max(salary) FROM employees;
```

```
SELECT department, max(salary) FROM employees GROUP BY department;
```

If you need non-aggregate function to choose a maximum of two values, see `greatest`:

```
SELECT greatest(a, b) FROM table;
```

