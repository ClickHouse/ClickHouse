---
toc_title: HAVING
---

# HAVING Clause {#having-clause}

Allows filtering the aggregation results produced by [GROUP BY](group-by.md). It is similar to the [WHERE](where.md) clause, but the difference is that `WHERE` is performed before aggregation, while `HAVING` is performed after it.

It is possible to reference aggregation results from `SELECT` clause in `HAVING` clause by their alias. Alternatively, `HAVING` clause can filter on results of additional aggregates that are not returned in query results.

## Limitations

`HAVING` can’t be used if aggregation is not performed. Use `WHERE` instead.
