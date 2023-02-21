---
sidebar_label: UNION
---

# UNION Clause

You can use `UNION` with explicitly specifying `UNION ALL` or `UNION DISTINCT`.

If you don't specify `ALL` or `DISTINCT`, it will depend on the `union_default_mode` setting. The difference between `UNION ALL` and `UNION DISTINCT` is that `UNION DISTINCT` will do a distinct transform for union result, it is equivalent to `SELECT DISTINCT` from a subquery containing `UNION ALL`.

You can use `UNION` to combine any number of `SELECT` queries by extending their results. Example:

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

Type casting is performed for unions. For example, if two queries being combined have the same field with non-`Nullable` and `Nullable` types from a compatible type, the resulting `UNION` has a `Nullable` type field.

Queries that are parts of `UNION` can be enclosed in round brackets. [ORDER BY](../../../sql-reference/statements/select/order-by.md) and [LIMIT](../../../sql-reference/statements/select/limit.md) are applied to separate queries, not to the final result. If you need to apply a conversion to the final result, you can put all the queries with `UNION` in a subquery in the [FROM](../../../sql-reference/statements/select/from.md) clause.

If you use `UNION` without explicitly specifying `UNION ALL` or `UNION DISTINCT`, you can specify the union mode using the [union_default_mode](../../../operations/settings/settings.md#union-default-mode) setting. The setting values can be `ALL`, `DISTINCT` or an empty string. However, if you use `UNION` with `union_default_mode` setting to empty string, it will throw an exception. The following examples demonstrate the results of queries with different values setting.

Query:

```sql
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

Result:

```text
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Query:

```sql
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

Result:

```text
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Queries that are parts of `UNION/UNION ALL/UNION DISTINCT` can be run simultaneously, and their results can be mixed together.

**See Also**

-   [insert_null_as_default](../../../operations/settings/settings.md#insert_null_as_default) setting.
-   [union_default_mode](../../../operations/settings/settings.md#union-default-mode) setting.


[Original article](https://clickhouse.com/docs/en/sql-reference/statements/select/union/) <!-- hide -->
