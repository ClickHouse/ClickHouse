---
slug: /en/sql-reference/statements/explain
sidebar_position: 39
sidebar_label: EXPLAIN
title: "EXPLAIN Statement"
---

Shows the execution plan of a statement.

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/hP6G2Nlz_cA"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

Syntax:

```sql
EXPLAIN [AST | SYNTAX | QUERY TREE | PLAN | PIPELINE | ESTIMATE | TABLE OVERRIDE] [setting = value, ...]
    [
      SELECT ... |
      tableFunction(...) [COLUMNS (...)] [ORDER BY ...] [PARTITION BY ...] [PRIMARY KEY] [SAMPLE BY ...] [TTL ...]
    ]
    [FORMAT ...]
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

## EXPLAIN Types

- `AST` — Abstract syntax tree.
- `SYNTAX` — Query text after AST-level optimizations.
- `QUERY TREE` — Query tree after Query Tree level optimizations.
- `PLAN` — Query execution plan.
- `PIPELINE` — Query execution pipeline.

### EXPLAIN AST

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

### EXPLAIN SYNTAX

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

### EXPLAIN QUERY TREE

Settings:

- `run_passes` — Run all query tree passes before dumping the query tree. Default: `1`.
- `dump_passes` — Dump information about used passes before dumping the query tree. Default: `0`.
- `passes` — Specifies how many passes to run. If set to `-1`, runs all the passes. Default: `-1`.

Example:
```sql
EXPLAIN QUERY TREE SELECT id, value FROM test_table;
```

```
QUERY id: 0
  PROJECTION COLUMNS
    id UInt64
    value String
  PROJECTION
    LIST id: 1, nodes: 2
      COLUMN id: 2, column_name: id, result_type: UInt64, source_id: 3
      COLUMN id: 4, column_name: value, result_type: String, source_id: 3
  JOIN TREE
    TABLE id: 3, table_name: default.test_table
```

### EXPLAIN PLAN

Dump query plan steps.

Settings:

- `header` — Prints output header for step. Default: 0.
- `description` — Prints step description. Default: 1.
- `indexes` — Shows used indexes, the number of filtered parts and the number of filtered granules for every index applied. Default: 0. Supported for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.
- `actions` — Prints detailed information about step actions. Default: 0.
- `json` — Prints query plan steps as a row in [JSON](../../interfaces/formats.md#json) format. Default: 0. It is recommended to use [TSVRaw](../../interfaces/formats.md#tabseparatedraw) format to avoid unnecessary escaping.

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

:::note    
Step and query cost estimation is not supported.
:::

When `json = 1`, the query plan is represented in JSON format. Every node is a dictionary that always has the keys `Node Type` and `Plans`. `Node Type` is a string with a step name. `Plans` is an array with child step descriptions. Other optional keys may be added depending on node type and settings.

Example:

```sql
EXPLAIN json = 1, description = 0 SELECT 1 UNION ALL SELECT 2 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Union",
      "Plans": [
        {
          "Node Type": "Expression",
          "Plans": [
            {
              "Node Type": "SettingQuotaAndLimits",
              "Plans": [
                {
                  "Node Type": "ReadFromStorage"
                }
              ]
            }
          ]
        },
        {
          "Node Type": "Expression",
          "Plans": [
            {
              "Node Type": "SettingQuotaAndLimits",
              "Plans": [
                {
                  "Node Type": "ReadFromStorage"
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
```

With `description` = 1, the `Description` key is added to the step:

```json
{
  "Node Type": "ReadFromStorage",
  "Description": "SystemOne"
}
```

With `header` = 1, the `Header` key is added to the step as an array of columns.

Example:

```sql
EXPLAIN json = 1, description = 0, header = 1 SELECT 1, 2 + dummy;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Header": [
        {
          "Name": "1",
          "Type": "UInt8"
        },
        {
          "Name": "plus(2, dummy)",
          "Type": "UInt16"
        }
      ],
      "Plans": [
        {
          "Node Type": "SettingQuotaAndLimits",
          "Header": [
            {
              "Name": "dummy",
              "Type": "UInt8"
            }
          ],
          "Plans": [
            {
              "Node Type": "ReadFromStorage",
              "Header": [
                {
                  "Name": "dummy",
                  "Type": "UInt8"
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
```

With `indexes` = 1, the `Indexes` key is added. It contains an array of used indexes. Each index is described as JSON with `Type` key (a string `MinMax`, `Partition`, `PrimaryKey` or `Skip`) and optional keys:

- `Name` — The index name (currently only used for `Skip` indexes).
- `Keys` — The array of columns used by the index.
- `Condition` —  The used condition.
- `Description` — The index description (currently only used for `Skip` indexes).
- `Parts` — The number of parts before/after the index is applied.
- `Granules` — The number of granules before/after the index is applied.

Example:

```json
"Node Type": "ReadFromMergeTree",
"Indexes": [
  {
    "Type": "MinMax",
    "Keys": ["y"],
    "Condition": "(y in [1, +inf))",
    "Parts": 5/4,
    "Granules": 12/11
  },
  {
    "Type": "Partition",
    "Keys": ["y", "bitAnd(z, 3)"],
    "Condition": "and((bitAnd(z, 3) not in [1, 1]), and((y in [1, +inf)), (bitAnd(z, 3) not in [1, 1])))",
    "Parts": 4/3,
    "Granules": 11/10
  },
  {
    "Type": "PrimaryKey",
    "Keys": ["x", "y"],
    "Condition": "and((x in [11, +inf)), (y in [1, +inf)))",
    "Parts": 3/2,
    "Granules": 10/6
  },
  {
    "Type": "Skip",
    "Name": "t_minmax",
    "Description": "minmax GRANULARITY 2",
    "Parts": 2/1,
    "Granules": 6/2
  },
  {
    "Type": "Skip",
    "Name": "t_set",
    "Description": "set GRANULARITY 2",
    "": 1/1,
    "Granules": 2/1
  }
]
```

With `actions` = 1, added keys depend on step type.

Example:

```sql
EXPLAIN json = 1, actions = 1, description = 0 SELECT 1 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Expression": {
        "Inputs": [],
        "Actions": [
          {
            "Node Type": "Column",
            "Result Type": "UInt8",
            "Result Type": "Column",
            "Column": "Const(UInt8)",
            "Arguments": [],
            "Removed Arguments": [],
            "Result": 0
          }
        ],
        "Outputs": [
          {
            "Name": "1",
            "Type": "UInt8"
          }
        ],
        "Positions": [0],
        "Project Input": true
      },
      "Plans": [
        {
          "Node Type": "SettingQuotaAndLimits",
          "Plans": [
            {
              "Node Type": "ReadFromStorage"
            }
          ]
        }
      ]
    }
  }
]
```

### EXPLAIN PIPELINE

Settings:

- `header` — Prints header for each output port. Default: 0.
- `graph` — Prints a graph described in the [DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) graph description language. Default: 0.
- `compact` — Prints graph in compact mode if `graph` setting is enabled. Default: 1.

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
### EXPLAIN ESTIMATE

Shows the estimated number of rows, marks and parts to be read from the tables while processing the query. Works with tables in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree) family. 

**Example**

Creating a table:

```sql
CREATE TABLE ttt (i Int64) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity = 16, write_final_mark = 0;
INSERT INTO ttt SELECT number FROM numbers(128);
OPTIMIZE TABLE ttt;
```

Query:

```sql
EXPLAIN ESTIMATE SELECT * FROM ttt;
```

Result:

```text
┌─database─┬─table─┬─parts─┬─rows─┬─marks─┐
│ default  │ ttt   │     1 │  128 │     8 │
└──────────┴───────┴───────┴──────┴───────┘
```

### EXPLAIN TABLE OVERRIDE

Shows the result of a table override on a table schema accessed through a table function.
Also does some validation, throwing an exception if the override would have caused some kind of failure.

**Example**

Assume you have a remote MySQL table like this:

```sql
CREATE TABLE db.tbl (
    id INT PRIMARY KEY,
    created DATETIME DEFAULT now()
)
```

```sql
EXPLAIN TABLE OVERRIDE mysql('127.0.0.1:3306', 'db', 'tbl', 'root', 'clickhouse')
PARTITION BY toYYYYMM(assumeNotNull(created))
```

Result:

```text
┌─explain─────────────────────────────────────────────────┐
│ PARTITION BY uses columns: `created` Nullable(DateTime) │
└─────────────────────────────────────────────────────────┘
```

:::note    
The validation is not complete, so a successful query does not guarantee that the override would not cause issues.
:::
