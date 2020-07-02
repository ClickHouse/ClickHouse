---
toc_title: UNION ALL
---

# UNION ALL Clause {#union-all-clause}

You can use `UNION ALL` to combine any number of `SELECT` queries by extending their results. Example:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Result columns are matched by their index (order inside `SELECT`). If column names do not match, names for the final result are taken from the first query.

Type casting is performed for unions. For example, if two queries being combined have the same field with non-`Nullable` and `Nullable` types from a compatible type, the resulting `UNION ALL` has a `Nullable` type field.

Queries that are parts of `UNION ALL` can’t be enclosed in round brackets. [ORDER BY](../../../sql-reference/statements/select/order-by.md) and [LIMIT](../../../sql-reference/statements/select/limit.md) are applied to separate queries, not to the final result. If you need to apply a conversion to the final result, you can put all the queries with `UNION ALL` in a subquery in the [FROM](../../../sql-reference/statements/select/from.md) clause.

## Limitations {#limitations}

Only `UNION ALL` is supported. The regular `UNION` (`UNION DISTINCT`) is not supported. If you need `UNION DISTINCT`, you can write `SELECT DISTINCT` from a subquery containing `UNION ALL`.

## Implementation Details {#implementation-details}

Queries that are parts of `UNION ALL` can be run simultaneously, and their results can be mixed together.
