---
slug: /en/sql-reference/table-functions/view
sidebar_position: 210
sidebar_label: view
title: view
---

Turns a subquery into a table. The function implements views (see [CREATE VIEW](/docs/en/sql-reference/statements/create/view/#create-view)). The resulting table does not store data, but only stores the specified `SELECT` query. When reading from the table, ClickHouse executes the query and deletes all unnecessary columns from the result.

**Syntax**

``` sql
view(subquery)
```

**Arguments**

- `subquery` — `SELECT` query.

**Returned value**

- A table.

**Example**

Input table:

``` text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

Query:

``` sql
SELECT * FROM view(SELECT name FROM months);
```

Result:

``` text
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

You can use the `view` function as a parameter of the [remote](/docs/en/sql-reference/table-functions/remote/#remote-remotesecure) and [cluster](/docs/en/sql-reference/table-functions/cluster/#cluster-clusterallreplicas) table functions:

``` sql
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

``` sql
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

**See Also**

- [View Table Engine](/docs/en/engines/table-engines/special/view/)
