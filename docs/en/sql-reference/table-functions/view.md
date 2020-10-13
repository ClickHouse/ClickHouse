---
toc_priority: 51
toc_title: view
---

## view {#view}

Turns an subquery into a table. Used for implementing views (for more information, see the `CREATE VIEW query`). It does not store data, but only stores the specified `SELECT` query. When reading from a table, it runs this query (and deletes all unnecessary columns from the query).

**Syntax**

``` sql
view(subquery)
```

**Parameters**

-   `subquery` — Subquery of the `SELECT` type only.

**Returned value**

-   A table.

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
SELECT * FROM view(SELECT name FROM months)
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

Function `view` can be used in `remote` or `cluster` table functions:

``` sql
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name))
```

``` sql
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name))
```

**See Also**

-   [View Table Engine](https://clickhouse.tech/docs/en/engines/table-engines/special/view/)
[Original article](https://clickhouse.tech/docs/en/query_language/table_functions/view/) <!--hide-->