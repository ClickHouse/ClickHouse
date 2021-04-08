---
toc_priority: 33
toc_title: INSERT INTO
---

## INSERT INTO Statement {#insert}

Adding data.

Basic query format:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

You can specify a list of columns to insert using  the `(c1, c2, c3)` or `COLUMNS(c1,c2,c3)` syntax. 

Instead of listing all the required columns you can use the `(* EXCEPT(column_list))` syntax.

For example, consider the table:

``` sql
SHOW CREATE insert_select_testtable;
```

```
┌─statement────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
SETTINGS index_granularity = 8192 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

If you want to insert data in all the columns, except 'b', you need to pass so many values how many columns you chose in parenthesis then:

``` sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

``` sql
SELECT * FROM insert_select_testtable;
```

```
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```
 
In this example, we see that the second inserted row has `a` and `c` columns filled by the passed values, and `b` filled with value by default.

If a list of columns doesn't include all existing columns, the rest of the columns are filled with:

-   The values calculated from the `DEFAULT` expressions specified in the table definition.
-   Zeros and empty strings, if `DEFAULT` expressions are not defined.

If [strict\_insert\_defaults=1](../../operations/settings/settings.md), columns that do not have `DEFAULT` defined must be listed in the query.

Data can be passed to the INSERT in any [format](../../interfaces/formats.md#formats) supported by ClickHouse. The format must be specified explicitly in the query:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse removes all spaces and one line feed (if there is one) before the data. When forming a query, we recommend putting the data on a new line after the query operators (this is important if the data begins with spaces).

Example:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

You can insert data separately from the query by using the command-line client or the HTTP interface. For more information, see the section “[Interfaces](../../interfaces/index.md#interfaces)”.

### Constraints {#constraints}

If table has [constraints](../../sql-reference/statements/create/table.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

### Inserting the Results of `SELECT` {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Columns are mapped according to their position in the SELECT clause. However, their names in the SELECT expression and the table for INSERT may differ. If necessary, type casting is performed.

None of the data formats except Values allow setting values to expressions such as `now()`, `1 + 2`, and so on. The Values format allows limited use of expressions, but this is not recommended, because in this case inefficient code is used for their execution.

Other queries for modifying data parts are not supported: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
However, you can delete old data using `ALTER TABLE ... DROP PARTITION`.

`FORMAT` clause must be specified in the end of query if `SELECT` clause contains table function [input()](../../sql-reference/table-functions/input.md).

### Performance Considerations {#performance-considerations}

`INSERT` sorts the input data by primary key and splits them into partitions by a partition key. If you insert data into several partitions at once, it can significantly reduce the performance of the `INSERT` query. To avoid this:

-   Add data in fairly large batches, such as 100,000 rows at a time.
-   Group data by a partition key before uploading it to ClickHouse.

Performance will not decrease if:

-   Data is added in real time.
-   You upload data that is usually sorted by time.

[Original article](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
