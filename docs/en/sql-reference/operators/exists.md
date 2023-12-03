---
slug: /en/sql-reference/operators/exists
---
# EXISTS

The `EXISTS` operator checks how many records are in the result of a subquery. If it is empty, then the operator returns `0`. Otherwise, it returns `1`.

`EXISTS` can also be used in a [WHERE](../../sql-reference/statements/select/where.md) clause.

:::tip    
References to main query tables and columns are not supported in a subquery.
:::

**Syntax**

``` sql
EXISTS(subquery)
```

**Example**

Query checking existence of values in a subquery:

``` sql
SELECT EXISTS(SELECT * FROM numbers(10) WHERE number > 8), EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

Result:

``` text
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

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
