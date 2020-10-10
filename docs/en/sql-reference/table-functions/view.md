## View {#view}

Turns an subquery into a table object.

**Syntax**

``` sql
view(`subquery`)
```

**Parameters**

-   `subquery` — An example: SELECT a, b, c FROM table_name.

**Returned value**

-   A table object.

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

The function view helps passing queries around. For instance, it can be used in remote/cluster table functions:

``` sql
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name))
```

``` sql
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name))
```

**See Also**

-   [CREATE VIEW](https://clickhouse.tech/docs/en/sql-reference/statements/create/view/)