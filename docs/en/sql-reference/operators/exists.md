# EXISTS {#exists-operator}

The `EXISTS` operator checks how many records are in the result of a subquery. If it is empty, then the operator returns `0`. Otherwise, it returns `1`.

`EXISTS` can be used in a [WHERE](../../sql-reference/statements/select/where.md) clause.

:::warning    
References to main query tables and columns are not supported in a subquery.
:::

**Syntax**

```sql
WHERE EXISTS(subquery)
```

**Example**

Query with a subquery returning several rows:

``` sql
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

Result:

``` text
┌─count()─┐
│      10 │
└─────────┘
```

Query with a subquery that returns an empty result:

``` sql
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

Result:

``` text
┌─count()─┐
│       0 │
└─────────┘
```
