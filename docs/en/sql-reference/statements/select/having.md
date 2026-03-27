---
description: 'Documentation for HAVING Clause'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'HAVING Clause'
doc_type: 'reference'
---

# HAVING Clause

Allows filtering the aggregation results produced by [GROUP BY](/sql-reference/statements/select/group-by). It is similar to the [WHERE](../../../sql-reference/statements/select/where.md) clause, but the difference is that `WHERE` is performed before aggregation, while `HAVING` is performed after it.

It is possible to reference aggregation results from `SELECT` clause in `HAVING` clause by their alias. Alternatively, `HAVING` clause can filter on results of additional aggregates that are not returned in query results.

## Example {#example}
If you have a `sales` table as follows:
```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

You can query it like so:
```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```
This will list sales people with greater than 10,000 in total sales in their region.
## Limitations {#limitations}

`HAVING` can't be used if aggregation is not performed. Use `WHERE` instead.
