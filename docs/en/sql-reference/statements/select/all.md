---
toc_title: ALL
---

# ALL Clause {#select-all}

`SELECT ALL` is identical to `SELECT` without `DISTINCT`.

- If `ALL` specified, ignore it.
- If both `ALL` and `DISTINCT` specified, exception will be thrown.

`ALL` can also be specified inside aggregate function with the same effect(noop), for instance:

```sql
SELECT sum(ALL number) FROM numbers(10);
```
equals to

```sql
SELECT sum(number) FROM numbers(10);
```
