---
toc_priority: 39
toc_title: EXPLAIN
---

# EXPLAIN Statement {#explain}

Show the execution plan of a statement.

Syntax:

```sql
EXPLAIN [AST | SYNTAX | PLAN | PIPELINE] [setting = value, ...] SELECT ... [FORMAT ...]
```
## EXPLAIN Types {#explain-types}

-  `AST` — Abstract syntax tree.
-  `SYNTAX` — Query text after AST-level optimizations.
-  `PLAN` — Query execution plan.
-  `PIPELINE` — Query execution pipeline.

### EXPLAIN AST {#explain-ast}

Dump query AST.

```sql
EXPLAIN AST SELECT 1;
```

```sql
SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 1)
   ExpressionList (children 1)
    Literal UInt64_1
```

### EXPLAIN SYNTAX {#explain-syntax}

Return query after syntax optimisations.

```sql
EXPLAIN SYNTAX SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c;
```

```sql
SELECT 
    `--a.number` AS `a.number`,
    `--b.number` AS `b.number`,
    number AS `c.number`
FROM 
(
    SELECT 
        number AS `--a.number`,
        b.number AS `--b.number`
    FROM system.numbers AS a
    CROSS JOIN system.numbers AS b
) AS `--.s`
CROSS JOIN system.numbers AS c
```
### EXPLAIN PLAN {#explain-plan}

Dump query plan steps.

Settings:

-  `header` — Print output header for step. Default: 0.
-  `description` — Print step description. Default: 1.
-  `actions` — Print detailed information about step actions. Default: 0.

Example:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) GROUP BY number % 4;
```

```sql
Union
  Expression (Projection)
    Expression (Before ORDER BY and SELECT)
      Aggregating
        Expression (Before GROUP BY)
          ReadFromStorage (Read from SystemNumbers)
```

### EXPLAIN PIPELINE {#explain-pipeline}

Settings:

-   `header` — print header for each output port. Default: 0.
-   `graph` — use DOT graph description language. Default: 0.
-   `compact` — print graph in compact mode if graph is enabled. Default: 1.

Example:

```sql
EXPLAIN PIPELINE SELECT sum(number) FROM numbers_mt(100000) GROUP BY number % 4;
```

```sql
(Union)
Converting
  (Expression)
  ExpressionTransform
    (Expression)
    ExpressionTransform
      (Aggregating)
      Resize 32 → 1
        AggregatingTransform × 32
          (Expression)
          ExpressionTransform × 32
            (ReadFromStorage)
            NumbersMt × 32 0 → 1
```

[Оriginal article](https://clickhouse.tech/docs/en/sql-reference/statements/explain/) <!--hide-->
