---
toc_priority: 39
toc_title: EXPLAIN
---

# EXPLAIN Statement {#explain}

Shows the execution plan of a statement.

Syntax:

```sql
EXPLAIN [AST | SYNTAX | PLAN | PIPELINE] [setting = value, ...] SELECT ... [FORMAT ...]
```

Example:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV;
```

```sql
Union
  Expression (Projection)
    Expression (Before ORDER BY and SELECT)
      Aggregating
        Expression (Before GROUP BY)
          SettingQuotaAndLimits (Set limits and quota after reading from storage)
            ReadFromStorage (SystemNumbers)
  Expression (Projection)
    MergingSorted (Merge sorted streams for ORDER BY)
      MergeSorting (Merge sorted blocks for ORDER BY)
        PartialSorting (Sort each block for ORDER BY)
          Expression (Before ORDER BY and SELECT)
            Aggregating
              Expression (Before GROUP BY)
                SettingQuotaAndLimits (Set limits and quota after reading from storage)
                  ReadFromStorage (SystemNumbers)
```

## EXPLAIN Types {#explain-types}

-  `AST` — Abstract syntax tree.
-  `SYNTAX` — Query text after AST-level optimizations.
-  `PLAN` — Query execution plan.
-  `PIPELINE` — Query execution pipeline.

### EXPLAIN AST {#explain-ast}

Dump query AST. Supports all types of queries, not only `SELECT`.

Examples:

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

```sql
EXPLAIN AST ALTER TABLE t1 DELETE WHERE date = today();
```

```sql
  explain
  AlterQuery  t1 (children 1)
   ExpressionList (children 1)
    AlterCommand 27 (children 1)
     Function equals (children 1)
      ExpressionList (children 2)
       Identifier date
       Function today (children 1)
        ExpressionList
```

### EXPLAIN SYNTAX {#explain-syntax}

Returns query after syntax optimizations.

Example:

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

-  `header` — Prints output header for step. Default: 0.
-  `description` — Prints step description. Default: 1.
-  `actions` — Prints detailed information about step actions. Default: 0.

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
        SettingQuotaAndLimits (Set limits and quota after reading from storage)
          ReadFromStorage (SystemNumbers)
```

!!! note "Note"
    Step and query cost estimation is not supported.

### EXPLAIN PIPELINE {#explain-pipeline}

Settings:

-   `header` — Prints header for each output port. Default: 0.
-   `graph` — Prints a graph described in the [DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) graph description language. Default: 0.
-   `compact` — Prints graph in compact mode if `graph` setting is enabled. Default: 1.
-   `indexes` — Shows used indexes, the number of filtered parts, and granules for every index applied. Default: 0. Supported for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Example:

```sql
EXPLAIN PIPELINE SELECT sum(number) FROM numbers_mt(100000) GROUP BY number % 4;
```

```sql
(Union)
(Expression)
ExpressionTransform
  (Expression)
  ExpressionTransform
    (Aggregating)
    Resize 2 → 1
      AggregatingTransform × 2
        (Expression)
        ExpressionTransform × 2
          (SettingQuotaAndLimits)
            (ReadFromStorage)
            NumbersMt × 2 0 → 1
```

[Оriginal article](https://clickhouse.tech/docs/en/sql-reference/statements/explain/) <!--hide-->
