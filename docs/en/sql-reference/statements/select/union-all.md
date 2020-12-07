---
toc_title: UNION
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

# UNION DISTINCT Clause {#union-distinct-clause}
The difference between `UNION ALL` and `UNION DISTINCT` is that `UNION DISTINCT` will do a distinct transform for union result, it is equivalent to `SELECT DISTINCT` from a subquery containing `UNION ALL`.

# UNION Clause {#union-clause}
By default, `UNION` has the same behavior as `UNION DISTINCT`, but you can specify union mode by setting `union_default_mode`, values can be 'ALL', 'DISTINCT' or empty string. However, if you use `UNION` with setting `union_default_mode` to empty string, it will throw an exception.


## Implementation Details {#implementation-details}

Queries that are parts of `UNION/UNION ALL/UNION DISTINCT` can be run simultaneously, and their results can be mixed together.
