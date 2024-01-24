---
slug: /en/sql-reference/statements/select/limit
sidebar_label: LIMIT
---

# LIMIT Clause

The `LIMIT` clause specifies the number of records to return. Two syntaxes are available for use:

- `LIMIT m` returns at most `m` result rows.
- `LIMIT n, m` returns at most `m` result rows, after skipping `n` rows . An equivalent syntax is `LIMIT m OFFSET n`.

`n` and `m` must be non-negative integers.

The rows returned by `LIMIT` are a arbitrary and non-deterministic subset of all result rows. If you need a stable result order, make sure
to combine `LIMIT` with an [ORDER BY](../../../sql-reference/statements/select/order-by.md) clause.

Setting [limit](../../../operations/settings/settings.md#limit) is an alternative method to limit the number of result rows for all queries in a session.

## LIMIT … WITH TIES Modifier

When you set `WITH TIES` modifier for `LIMIT n[,m]` and specify `ORDER BY expr_list`, you will get in result first `n` or `n,m` rows and all rows with same `ORDER BY` fields values equal to row at position `n` for `LIMIT n` and `m` for `LIMIT n,m`.

This modifier also can be combined with [ORDER BY … WITH FILL modifier](../../../sql-reference/statements/select/order-by.md#orderby-with-fill).

For example, the following query

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5
```

returns

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

but after apply `WITH TIES` modifier

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5 WITH TIES
```

it returns another rows set

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

cause row number 6 have same value “2” for field `n` as row number 5

## LIMIT with BY Clause

A query with `LIMIT n BY expr1[, expr2, [...]]` clause selects the first `n` rows for each distinct combination of [expression](../../../sql-reference/syntax.md#syntax-expressions) 1, expression 2, etc.

The following syntax variants are supported

- `LIMIT m BY expressions`
- `LIMIT m, n BY expressions`
- `LIMIT n OFFSET offset_value BY expressions`

The `BY` clause is followed by

- a list of expressions, e.g. `LIMIT n BY visits, search_phrase`, or
- a list of numbers referring to expressions in the `SELECT` clause, e.g. `LIMIT n BY 2, 1`.

To disable positional arguments of `BY`, set setting [enable_positional_arguments](../../../operations/settings/settings.md#enable-positional-arguments) = 0.

During query processing, ClickHouse selects data ordered by sorting key. The sorting key is set explicitly using an [ORDER BY](order-by.md#select-order-by) clause or implicitly as a property of the table engine (row order is only guaranteed when using [ORDER BY](order-by.md#select-order-by), otherwise the row blocks will not be ordered due to multi-threading). Then ClickHouse applies `LIMIT n BY expressions` and returns the first `n` rows for each distinct combination of `expressions`. If `OFFSET` is specified, then for each data block that belongs to a distinct combination of `expressions`, ClickHouse skips `offset_value` number of rows from the beginning of the block and returns a maximum of `n` rows as a result. If `offset_value` is bigger than the number of rows in the data block, ClickHouse returns zero rows from the block.

## Examples

Sample table:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Queries:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

The `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query returns the same result.

The following query returns the top 5 referrers for each `domain, device_type` pair with a maximum of 100 rows in total (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
