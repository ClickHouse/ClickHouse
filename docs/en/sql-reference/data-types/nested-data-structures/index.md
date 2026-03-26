---
description: 'Overview of nested data structures in ClickHouse'
sidebar_label: 'Nested(Name1 Type1, Name2 Type2, ...)'
sidebar_position: 57
slug: /sql-reference/data-types/nested-data-structures/nested
title: 'Nested(name1 Type1, Name2 Type2, ...)'
doc_type: 'guide'
---

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a [CREATE TABLE](../../../sql-reference/statements/create/table.md) query. Each table row can correspond to any number of rows in a nested data structure.

:::tip[Avoid using dots in column names]
Column names containing dots, columns sharing a common dot-prefix, and columns with the `Array` type can each be interpreted as part of a flattened Nested structure when `flatten_nested = 1` (the default). This can cause unexpected array-length validation on inserts and renaming restrictions.

Avoid using dots in column names if possible.
Use underscores (`_`) or another separator instead of dots in column names unless you intentionally need `Nested` semantics.
:::

Example:

```sql
CREATE TABLE test.visits(
  CounterID UInt32,
  StartDate Date,
  Sign Int8,
  IsNew UInt8,
  VisitID UInt64,
  UserID UInt64,
--highlight-start
  Goals Nested(
    ID UInt32,
    Serial UInt32,
    EventTime DateTime,
    Price Int64,
    OrderID String,
    CurrencyID UInt32
  )
--highlight-end
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY (StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID));

INSERT INTO test.visits
(CounterID, StartDate, Sign, IsNew, VisitID, UserID, Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-17', 1, 1, 1001, 100001, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 16:38:10', '2014-03-17 16:38:48', '2014-03-17 16:42:27'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1002, 100002, [1073752], [1], ['2014-03-17 00:28:25'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 0, 1003, 100003, [1073752], [1], ['2014-03-17 10:46:20'], [0], [''], [0]),
    (101500, '2014-03-17', 1, 1, 1004, 100004, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:59:20', '2014-03-17 22:17:55', '2014-03-17 22:18:07', '2014-03-17 22:18:51'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1005, 100005, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1006, 100006, [1073752, 591325, 591325], [1, 2, 3], ['2014-03-17 11:37:06', '2014-03-17 14:07:47', '2014-03-17 14:36:21'], [0, 0, 0], ['', '', ''], [0, 0, 0]),
    (101500, '2014-03-17', 1, 0, 1007, 100007, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 0, 1008, 100008, [], [], [], [], [], []),
    (101500, '2014-03-17', 1, 1, 1009, 100009, [591325, 1073752], [1, 2], ['2014-03-17 00:46:05', '2014-03-17 00:46:05'], [0, 0], ['', ''], [0, 0]),
    (101500, '2014-03-17', 1, 1, 1010, 100010, [1073752, 591325, 591325, 591325], [1, 2, 3, 4], ['2014-03-17 13:28:33', '2014-03-17 13:30:26', '2014-03-17 18:51:21', '2014-03-17 18:51:45'], [0, 0, 0, 0], ['', '', '', ''], [0, 0, 0, 0]);
```

The `CREATE TABLE` DDL statement above declares the `Goals` nested data structure, which contains data about conversions, or goals reached.
Each row in the 'visits' table corresponds to zero or more conversions.

When the [`flatten_nested`](/operations/settings/settings#flatten_nested) setting is set to `0` (`flatten_nested=1` by default) arbitrary levels of nesting are supported.

In most cases, when working with a nested data structure, its columns are specified with column names separated by a dot.
These columns make up an array of matching types.
All column arrays of a single nested data structure have the same length.

For example:

```sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

```text
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goals.ID                       ┃ Goals.EventTime                                                                           ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
 1. │ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 2. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 3. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 4. │ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 5. │ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 6. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 7. │ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 8. │ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
 9. │ []                             │ []                                                                                        │
    ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
10. │ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
    └────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

:::tip
It is easiest to think of a nested data structure as a set of multiple column arrays of equal length.
:::

### Filtering Nested columns in WHERE {#filtering-nested-columns-in-where}

Because each column of a `Nested` structure is stored as an `Array`, referencing it in a `WHERE` clause gives you the whole array for every row, not an individual element. You cannot compare a nested column directly to a scalar value so you must use [array functions](/sql-reference/functions/array-functions) instead.

For example, this query **will not** find rows whose `Goals` contain the ID `591325`:

```sql
-- WRONG: compares the entire Array to a scalar
SELECT * FROM test.visits
WHERE Goals.ID = 591325;
```

Use [`has`](/sql-reference/functions/array-functions#has) to check whether an array contains a specific value:

```sql
-- Find visits that have at least one goal with ID 591325
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE has(Goals.ID, 591325);
```

Use [`arrayExists`](/sql-reference/functions/array-functions#arrayexists) when the condition is more complex:

```sql
-- Find visits that have at least one goal with ID greater than 1000000
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE arrayExists(id -> id > 1000000, Goals.ID);
```

You can filter by array length with `length` or exclude empty arrays with `notEmpty`:

```sql
-- Visits with at least 3 goals
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE length(Goals.ID) >= 3;

-- Visits with at least one goal (non-empty array)
SELECT CounterID, VisitID, Goals.ID
FROM test.visits
WHERE notEmpty(Goals.ID);
```

To filter on individual elements of a nested structure rather than on whole rows, use `ARRAY JOIN` to unfold the arrays first.
After `ARRAY JOIN`, each element becomes a separate row, so the `WHERE` clause applies to scalar values.
For more information, see [`ARRAY JOIN` clause](/sql-reference/statements/select/array-join). Example:

```sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

```text
    ┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Goal.ID ┃      Goal.EventTime ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
 1. │ 1073752 │ 2014-03-17 10:46:20 │
    ├─────────┼─────────────────────┤
 2. │ 1073752 │ 2014-03-17 13:28:33 │
    ├─────────┼─────────────────────┤
 3. │  591325 │ 2014-03-17 13:30:26 │
    ├─────────┼─────────────────────┤
 4. │  591325 │ 2014-03-17 18:51:21 │
    ├─────────┼─────────────────────┤
 5. │  591325 │ 2014-03-17 18:51:45 │
    ├─────────┼─────────────────────┤
 6. │ 1073752 │ 2014-03-17 16:38:10 │
    ├─────────┼─────────────────────┤
 7. │  591325 │ 2014-03-17 16:38:48 │
    ├─────────┼─────────────────────┤
 8. │  591325 │ 2014-03-17 16:42:27 │
    ├─────────┼─────────────────────┤
 9. │  591325 │ 2014-03-17 00:46:05 │
    ├─────────┼─────────────────────┤
10. │ 1073752 │ 2014-03-17 00:46:05 │
    └─────────┴─────────────────────┘
```

You can't perform `SELECT` for an entire nested data structure. You can only explicitly list individual columns that are part of it.

### Inserting data {#inserting-data}

For an `INSERT` query, you should pass all the component column arrays of a nested data structure separately (as if they were individual column arrays). During insertion, the system checks that they have the same length.

Each nested sub-column is listed in the column list using dot notation (`Goals.ID`, `Goals.Serial`, ...), and the corresponding values are arrays:

```sql
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    -- A visit with two goals: each nested sub-column gets an array of length 2
    (101500, '2014-03-18', 1, 1, 2001, 200001,
     [1073752, 591325], [1, 2],
     ['2014-03-18 10:00:00', '2014-03-18 10:05:00'],
     [100, 200], ['order_a', 'order_b'], [1, 2]),
    -- A visit with no goals: all nested sub-columns get empty arrays
    (101500, '2014-03-18', 1, 0, 2002, 200002,
     [], [], [], [], [], []);
```

All nested sub-column arrays within a single row must have the same length. Mismatched lengths cause an error:

```sql
-- ERROR: Goals.ID has 2 elements, but Goals.Serial has 1
INSERT INTO test.visits
    (CounterID, StartDate, Sign, IsNew, VisitID, UserID,
     Goals.ID, Goals.Serial, Goals.EventTime, Goals.Price, Goals.OrderID, Goals.CurrencyID)
VALUES
    (101500, '2014-03-18', 1, 1, 2003, 200003,
     [1073752, 591325], [1],
     ['2014-03-18 12:00:00'], [0], [''], [0]);
```

For a `DESCRIBE` query, the columns in a nested data structure are listed separately in the same way.

### ALTER limitations {#alter-limitations}

`ALTER` queries on nested data structures have the following limitations:

**Adding sub-columns** works normally. You can add a new sub-column to an existing `Nested` structure:

```sql
ALTER TABLE test.visits ADD COLUMN Goals.Revenue Float64;
```

**Dropping sub-columns** works for individual sub-columns:

```sql
ALTER TABLE test.visits DROP COLUMN Goals.Revenue;
```

**Modifying the type** of a sub-column works and triggers a mutation (data rewrite):

```sql
ALTER TABLE test.visits MODIFY COLUMN Goals.Price Int32;
```

**Renaming** has restrictions. You can rename a sub-column within the same nested structure:

```sql
-- OK: stays within the Goals structure
ALTER TABLE test.visits RENAME COLUMN Goals.Price TO Goals.Amount;
```

However, you **cannot**:

- Rename the entire nested structure itself (e.g., `Goals` to `Conversions`).
- Move a sub-column to a different nested structure (e.g., `Goals.ID` to `OtherNested.ID`).
- Move a sub-column out of or into a nested structure (e.g., `Goals.ID` to `GoalID` or vice versa).
