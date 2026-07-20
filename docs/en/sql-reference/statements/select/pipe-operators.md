---
description: 'Documentation for pipe operators'
sidebar_label: 'Pipe operators'
sidebar_position: 55
slug: /sql-reference/statements/select/pipe-operators
title: 'Pipe Operators'
doc_type: 'reference'
---

# Pipe operators {#pipe-operators}

Pipe operators allow writing queries as a linear chain of transformations that reads from top to bottom, similar to the [pipe syntax of GoogleSQL](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/):

```sql
FROM orders
|> WHERE cancelled = 0
|> AGGREGATE sum(amount) AS total GROUP BY customer
|> ORDER BY total DESC
|> LIMIT 3
```

Any `SELECT` query can be followed by a chain of pipe operators. Each operator starts with the `|>` token, takes the result of the query before it as input, and applies one more transformation to it. Inside every operator, the regular ClickHouse syntax is used.

Pipe operators are a syntax extension: every operator wraps the query before it into a subquery, so the resulting AST is the same as the AST of the equivalent query written with nested subqueries, and the query above is equivalent to:

```sql
SELECT * FROM
(
    SELECT customer, sum(amount) AS total FROM
    (
        SELECT * FROM
        (
            SELECT * FROM orders
        )
        WHERE cancelled = 0
    )
    GROUP BY customer
)
ORDER BY total DESC
LIMIT 3
```

## FROM queries {#from-queries}

A query can start with the `FROM` clause, and the `SELECT` clause is optional in such queries - when it is omitted, the query works as if `SELECT *` was written:

```sql
FROM orders;
FROM orders WHERE amount > 100;
FROM orders |> WHERE amount > 100;
```

## Operators {#operators}

### WHERE {#where}

`|> WHERE condition` filters the input rows. When it is applied after an aggregation, it works like `HAVING`:

```sql
FROM orders
|> AGGREGATE sum(amount) AS total GROUP BY customer
|> WHERE total > 100
```

### SELECT {#select}

`|> SELECT [DISTINCT] expr1 [AS alias1], ...` leaves only the listed expressions as the output columns:

```sql
FROM orders |> SELECT customer, amount * 2 AS doubled
```

### EXTEND {#extend}

`|> EXTEND expr1 [AS alias1], ...` appends the listed expressions to the input columns; it is equivalent to `SELECT *, expr1 AS alias1, ...`:

```sql
FROM orders |> EXTEND amount * 10 AS big
```

### SET {#set}

`|> SET column1 = expr1, ...` replaces the values of the listed columns; it is equivalent to `SELECT * REPLACE (expr1 AS column1, ...)`:

```sql
FROM orders |> SET amount = amount + 1000
```

### DROP {#drop}

`|> DROP column1, ...` removes the listed columns; it is equivalent to `SELECT * EXCEPT (column1, ...)`:

```sql
FROM orders |> DROP cancelled
```

### AS {#as}

`|> AS alias` gives an alias to the input of the next operator, so it can be referenced in that operator, which is mostly useful for joins:

```sql
FROM orders
|> AGGREGATE sum(amount) AS total GROUP BY customer
|> AS agg
|> JOIN orders AS o ON agg.customer = o.customer
```

### AGGREGATE {#aggregate}

`|> AGGREGATE agg1 [AS alias1], ... [GROUP BY expr1 [AS alias1], ...]` aggregates the input rows. The output columns are the grouping columns followed by the aggregate columns. Without `GROUP BY`, the whole input is aggregated to a single row:

```sql
FROM orders |> AGGREGATE count() AS c, sum(amount) AS total GROUP BY customer;
FROM orders |> AGGREGATE count() AS c;
```

### DISTINCT {#distinct}

`|> DISTINCT` removes duplicate rows; it is equivalent to `SELECT DISTINCT *`.

### ORDER BY {#order-by}

`|> ORDER BY expr1 [ASC/DESC], ...` sorts the input rows:

```sql
FROM orders |> ORDER BY amount DESC
```

### LIMIT and OFFSET {#limit-and-offset}

`|> LIMIT length [OFFSET offset]` and `|> OFFSET offset` limit the number of rows:

```sql
FROM orders |> ORDER BY amount DESC |> LIMIT 3 OFFSET 1
```

### JOIN and ARRAY JOIN {#join-and-array-join}

`|> [GLOBAL] [ANY/ALL/ASOF/SEMI/ANTI] [INNER/LEFT/RIGHT/FULL/CROSS] JOIN table [ON expr | USING (columns)]` joins the input with another table, subquery, or table function. All kinds of [JOIN](../../../sql-reference/statements/select/join.md) and [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) are supported, and a single operator can contain several joins, like a `FROM` clause:

```sql
FROM customers
|> AS c
|> LEFT JOIN orders AS o ON c.name = o.customer
|> ARRAY JOIN tags
```

Since every operator is a new subquery scope, table aliases are visible only inside the same operator (in the `ON` condition). The following operators see the combined columns of the join result, as after `SELECT *`.

### UNION, INTERSECT, and EXCEPT {#union-intersect-and-except}

`|> UNION [ALL/DISTINCT] (query1) [, (query2), ...]`, `|> INTERSECT [ALL/DISTINCT] ...`, and `|> EXCEPT [ALL/DISTINCT] ...` combine the input with the results of other queries:

```sql
FROM orders
|> SELECT customer
|> UNION ALL (FROM customers |> SELECT name)
|> DISTINCT
```

## Notes {#notes}

- Pipe operators bind to the whole query before them, including set operations: in `SELECT 1 UNION ALL SELECT 2 |> AGGREGATE count()`, the aggregation is applied to the result of the `UNION ALL`. To continue a query with `UNION` after a pipe operator, use the `|> UNION` operator or parentheses.
- Pipe operators can be used everywhere a `SELECT` query is expected: in subqueries, in `INSERT ... SELECT` (including the form `INSERT INTO t FROM src |> ...`), in `CREATE VIEW`, in the `view` table function, and so on.
- The renaming of columns in place is not provided as a separate operator; use `|> SELECT * EXCEPT (old_name), old_name AS new_name` or the `SET` and `DROP` operators.
