---
slug: /en/sql-reference/statements/select/qualify
sidebar_label: QUALIFY
---

# QUALIFY Clause

Allows filtering window functions results. It is similar to the [WHERE](../../../sql-reference/statements/select/where.md) clause, but the difference is that `WHERE` is performed before window functions evaluation, while `QUALIFY` is performed after it.

It is possible to reference window functions results from `SELECT` clause in `QUALIFY` clause by their alias. Alternatively, `QUALIFY` clause can filter on results of additional window functions that are not returned in query results.

## Limitations

`QUALIFY` can’t be used if there are no window functions to evaluate. Use `WHERE` instead.
