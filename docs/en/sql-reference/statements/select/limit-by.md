---
description: 'Documentation for LIMIT BY Clause'
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: 'LIMIT BY Clause'
doc_type: 'reference'
---

# LIMIT BY Clause

A query with the `LIMIT n BY expressions` clause selects the first `n` rows for each distinct value of `expressions`. The key for `LIMIT BY` can contain any number of [expressions](/sql-reference/syntax#expressions).

ClickHouse supports the following syntax variants:

- `LIMIT [offset_value, ]n BY expressions`
- `LIMIT n OFFSET offset_value BY expressions`

During query processing, ClickHouse selects data ordered by sorting key. The sorting key is set explicitly using an [ORDER BY](/sql-reference/statements/select/order-by) clause or implicitly as a property of the table engine (row order is only guaranteed when using [ORDER BY](/sql-reference/statements/select/order-by), otherwise the row blocks will not be ordered due to multi-threading). Then ClickHouse applies `LIMIT n BY expressions` and returns the first `n` rows for each distinct combination of `expressions`. If `OFFSET` is specified, then for each data block that belongs to a distinct combination of `expressions`, ClickHouse skips `offset_value` number of rows from the beginning of the block and returns a maximum of `n` rows as a result. If `offset_value` is bigger than the number of rows in the data block, ClickHouse returns zero rows from the block.

:::note    
`LIMIT BY` is not related to [LIMIT](../../../sql-reference/statements/select/limit.md). They can both be used in the same query.
:::

If you want to use column numbers instead of column names in the `LIMIT BY` clause, enable the setting [enable_positional_arguments](/operations/settings/settings#enable_positional_arguments).    

## Examples {#examples}

Sample table:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Queries:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

The `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query returns the same result.

The following query returns the top 5 referrers for each `domain, device_type` pair with a maximum of 100 rows in total (`LIMIT n BY + LIMIT`).

```sql
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
```

## LIMIT BY ALL {#limit-by-all}

`LIMIT BY ALL` is equivalent to listing all the SELECT-ed expressions that are not aggregate functions.

For example:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL
```

is the same as

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3
```

For a special case that if there is a function having both aggregate functions and other fields as its arguments, the `LIMIT BY` keys will contain the maximum non-aggregate fields we can extract from it.

For example:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL
```

is the same as

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2)
```

## Examples {#examples-limit-by-all}

Sample table:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Queries:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

The `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query returns the same result.

Using `LIMIT BY ALL`:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL
```

This is equivalent to:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val
```

The following query returns the top 5 referrers for each `domain, device_type` pair with a maximum of 100 rows in total (`LIMIT n BY + LIMIT`).

```sql
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
```
