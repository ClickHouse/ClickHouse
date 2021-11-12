# EXISTS {#exists-operator}

The `EXISTS` operator tests how many records are in the result of a subquery. If it is empty, then the operator returns `0`. Otherwise it returns `1`.

!!! warning "Warning"
    References to main query tables and columns are not supported in a subquery.

**Syntax**

```sql
WHERE EXISTS(subquery)
```

**Example**

Query:

``` sql
SELECT 'Exists' WHERE EXISTS (SELECT * FROM numbers(10) WHERE number < 2);
SELECT 'Empty subquery' WHERE EXISTS (SELECT * FROM numbers(10) WHERE number > 12);
```

The first query returns one row. The second query does not return rows because the result of the subquery is empty.

``` text
┌─'Exists'─┐
│ Exists   │
└──────────┘
```
