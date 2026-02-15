---
description: 'Documentation for WITH Clause'
sidebar_label: 'WITH'
slug: /sql-reference/statements/select/with
title: 'WITH Clause'
doc_type: 'reference'
---

# WITH Clause

ClickHouse supports Common Table Expressions ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), Common Scalar Expressions and Recursive Queries.

## Common Table Expressions {#common-table-expressions}

Common Table Expressions represent named subqueries.
They can be referenced by name anywhere in a `SELECT` query where a table expression is allowed.
Named subqueries can be referenced by name in the scope of the current query or in the scopes of child subqueries.

Every reference to a Common Table Expression in `SELECT` queries is always replaced by the subquery from it's definition.
Recursion is prevented by hiding the current CTE from the identifier resolution process.

Please note that CTEs do not guarantee the same results in all places they are called because the query will be re-executed for each use case.

### Syntax {#common-table-expressions-syntax}

```sql
WITH <identifier> AS <subquery expression>
```

### Example {#common-table-expressions-example}

An example of when a subquery is re-executed:
```sql
WITH cte_numbers AS
(
    SELECT
        num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT
    count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers)
```
If CTEs were to pass exactly the results and not just a piece of code, you would always see `1000000`

However, due to the fact that we are referring `cte_numbers` twice, random numbers are generated each time and, accordingly, we see different random results, `280501, 392454, 261636, 196227` and so on...

## Common Scalar Expressions {#common-scalar-expressions}

ClickHouse allows you to declare aliases to arbitrary scalar expressions in the `WITH` clause.
Common scalar expressions can be referenced in any place in the query.

:::note
If a common scalar expression references something other than a constant literal, the expression may lead to the presence of [free variables](https://en.wikipedia.org/wiki/Free_variables_and_bound_variables).
ClickHouse resolves any identifier in the closest scope possible, meaning that free variables can reference unexpected entities in case of name clashes or may lead to a correlated subquery.
It is recommended to define CSE as a [lambda function](/sql-reference/functions/overview#arrow-operator-and-lambda) (possible only with the [analyzer](/operations/analyzer) enabled) binding all the used identifiers to achieve a more predictable behavior of expression identifiers resolution.
:::

### Syntax {#common-scalar-expressions-syntax}

```sql
WITH <expression> AS <identifier>
```

### Examples {#common-scalar-expressions-examples}

**Example 1:** Using constant expression as "variable"

```sql
WITH '2019-08-01 15:23:00' AS ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**Example 2:** Using higher-order functions to bound the identifiers

```sql
WITH
    '.txt' as extension,
    (id, extension) -> concat(lower(id), extension) AS gen_name
SELECT gen_name('test', '.sql') as file_name;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**Example 3:** Using higher-order functions with free variables

The following example queries show that unbound identifiers resolve into an entity in the closest scope.
Here, `extension` is not bound in the `gen_name` lambda function body.
Although `extension` is defined to `'.txt'` as a common scalar expression in the scope of `generated_names` definition and usage, it is resolved into a column of the table `extension_list`, because it is available in the `generated_names` subquery.

```sql
CREATE TABLE extension_list
(
    extension String
)
ORDER BY extension
AS SELECT '.sql';

WITH
    '.txt' as extension,
    generated_names as (
        WITH
            (id) -> concat(lower(id), extension) AS gen_name
        SELECT gen_name('test') as file_name FROM extension_list
    )
SELECT file_name FROM generated_names;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**Example 4:** Evicting a sum(bytes) expression result from the SELECT clause column list

```sql
WITH sum(bytes) AS s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**Example 5:** Using results of a scalar subquery

```sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10;
```

**Example 6:** Reusing expression in a subquery

```sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

## Recursive Queries {#recursive-queries}

The optional `RECURSIVE` modifier allows for a WITH query to refer to its own output. Example:

**Example:** Sum integers from 1 through 100

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table WHERE number < 100
)
SELECT sum(number) FROM test_table;
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

:::note
Recursive CTEs rely on the [query analyzer](/operations/analyzer) introduced in version **`24.3`**. If you're using version **`24.3+`** and encounter a **`(UNKNOWN_TABLE)`** or **`(UNSUPPORTED_METHOD)`** exception, it suggests that the analyzer is disabled on your instance, role, or profile. To activate the analyzer, enable the setting **`allow_experimental_analyzer`** or update the **`compatibility`** setting to a more recent version.
Starting from version `24.8` the analyzer has been fully promoted to production, and the setting `allow_experimental_analyzer` has been renamed to `enable_analyzer`.
:::

The general form of a recursive `WITH` query is always a non-recursive term, then `UNION ALL`, then a recursive term, where only the recursive term can contain a reference to the query's own output. Recursive CTE query is executed as follows:

1. Evaluate the non-recursive term. Place result of non-recursive term query in a temporary working table.
2. As long as the working table is not empty, repeat these steps:
    1. Evaluate the recursive term, substituting the current contents of the working table for the recursive self-reference. Place result of recursive term query in a temporary intermediate table.
    2. Replace the contents of the working table with the contents of the intermediate table, then empty the intermediate table.

Recursive queries are typically used to work with hierarchical or tree-structured data. For example, we can write a query that performs tree traversal:

**Example:** Tree traversal

First let's create tree table:

```sql
DROP TABLE IF EXISTS tree;
CREATE TABLE tree
(
    id UInt64,
    parent_id Nullable(UInt64),
    data String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');
```

We can traverse those tree with such query:

**Example:** Tree traversal
```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree;
```

```text
┌─id─┬─parent_id─┬─data──────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │
│  1 │         0 │ Child_1   │
│  2 │         0 │ Child_2   │
│  3 │         1 │ Child_1_1 │
└────┴───────────┴───────────┘
```

### Search order {#search-order}

To create a depth-first order, we compute for each result row an array of rows that we have already visited:

**Example:** Tree traversal depth-first order
```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id])
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY path;
```

```text
┌─id─┬─parent_id─┬─data──────┬─path────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │ [0]     │
│  1 │         0 │ Child_1   │ [0,1]   │
│  3 │         1 │ Child_1_1 │ [0,1,3] │
│  2 │         0 │ Child_2   │ [0,2]   │
└────┴───────────┴───────────┴─────────┘
```

To create a breadth-first order, standard approach is to add column that tracks the depth of the search:

**Example:** Tree traversal breadth-first order
```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path, toUInt64(0) AS depth
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id]), depth + 1
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY depth;
```

```text
┌─id─┬─link─┬─data──────┬─path────┬─depth─┐
│  0 │ ᴺᵁᴸᴸ │ ROOT      │ [0]     │     0 │
│  1 │    0 │ Child_1   │ [0,1]   │     1 │
│  2 │    0 │ Child_2   │ [0,2]   │     1 │
│  3 │    1 │ Child_1_1 │ [0,1,3] │     2 │
└────┴──────┴───────────┴─────────┴───────┘
```

### Cycle detection {#cycle-detection}

First let's create graph table:

```sql
DROP TABLE IF EXISTS graph;
CREATE TABLE graph
(
    from UInt64,
    to UInt64,
    label String
) ENGINE = MergeTree ORDER BY (from, to);

INSERT INTO graph VALUES (1, 2, '1 -> 2'), (1, 3, '1 -> 3'), (2, 3, '2 -> 3'), (1, 4, '1 -> 4'), (4, 5, '4 -> 5');
```

We can traverse that graph with such query:

**Example:** Graph traversal without cycle detection
```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
    UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```
```text
┌─from─┬─to─┬─label──┐
│    1 │  4 │ 1 -> 4 │
│    1 │  2 │ 1 -> 2 │
│    1 │  3 │ 1 -> 3 │
│    2 │  3 │ 2 -> 3 │
│    4 │  5 │ 4 -> 5 │
└──────┴────┴────────┘
```

But if we add cycle in that graph, previous query will fail with `Maximum recursive CTE evaluation depth` error:

```sql
INSERT INTO graph VALUES (5, 1, '5 -> 1');

WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
Code: 306. DB::Exception: Received from localhost:9000. DB::Exception: Maximum recursive CTE evaluation depth (1000) exceeded, during evaluation of search_graph AS (SELECT from, to, label FROM graph AS g UNION ALL SELECT g.from, g.to, g.label FROM graph AS g, search_graph AS sg WHERE g.from = sg.to). Consider raising max_recursive_cte_evaluation_depth setting.: While executing RecursiveCTESource. (TOO_DEEP_RECURSION)
```

The standard method for handling cycles is to compute an array of the already visited nodes:

**Example:** Graph traversal with cycle detection
```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label, false AS is_cycle, [tuple(g.from, g.to)] AS path FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label, has(path, tuple(g.from, g.to)), arrayConcat(sg.path, [tuple(g.from, g.to)])
    FROM graph g, search_graph sg
    WHERE g.from = sg.to AND NOT is_cycle
)
SELECT * FROM search_graph WHERE is_cycle ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┬─is_cycle─┬─path──────────────────────┐
│    1 │  4 │ 1 -> 4 │ true     │ [(1,4),(4,5),(5,1),(1,4)] │
│    4 │  5 │ 4 -> 5 │ true     │ [(4,5),(5,1),(1,4),(4,5)] │
│    5 │  1 │ 5 -> 1 │ true     │ [(5,1),(1,4),(4,5),(5,1)] │
└──────┴────┴────────┴──────────┴───────────────────────────┘
```

### Infinite queries {#infinite-queries}

It is also possible to use infinite recursive CTE queries if `LIMIT` is used in outer query:

**Example:** Infinite recursive CTE query
```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table
)
SELECT sum(number) FROM (SELECT number FROM test_table LIMIT 100);
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```
