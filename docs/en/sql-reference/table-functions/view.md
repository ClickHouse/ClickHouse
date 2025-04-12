---
description: 'Turns a subquery into a table. The function implements views.'
sidebar_label: 'view'
sidebar_position: 210
slug: /sql-reference/table-functions/view
title: 'view'
---

# view Table Function

Turns a subquery into a table. The function implements views (see [CREATE VIEW](/sql-reference/statements/create/view)). The resulting table does not store data, but only stores the specified `SELECT` query. When reading from the table, ClickHouse executes the query and deletes all unnecessary columns from the result.

**Syntax**

```sql
view(subquery)
```

**Arguments**

- `subquery` — `SELECT` query.

**Returned value**

- A table.

**Example**

Input table:

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

Query:

```sql
SELECT * FROM view(SELECT name FROM months);
```

Result:

```text
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

You can use the `view` function as a parameter of the [remote](/sql-reference/table-functions/remote) and [cluster](/sql-reference/table-functions/cluster) table functions:

```sql
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

**See Also**

- [View Table Engine](/engines/table-engines/special/view/)
