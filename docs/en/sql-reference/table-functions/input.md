---
description: 'Table function that allows effectively converting and inserting data
  sent to the server with a given structure to a table with another structure.'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

# input Table Function

`input(structure)` or `input(structure, format)` - table function that allows effectively converting and inserting data sent to the
server with a given structure to a table with another structure.

`structure` - structure of data sent to the server in following format `'column1_name column1_type, column2_name column2_type, ...'`.
For example, `'id UInt32, name String'`. Use `'auto'` to infer the structure from the input stream (requires `format` to be specified).

`format` - optional second argument specifying the data format (e.g., `'Native'`). When specified with `structure` set to `'auto'`,
the schema is inferred from the input stream. When specified with an explicit structure, the format is used directly
without extracting it from the outer query's `FORMAT` clause.

This function can be used only in `INSERT SELECT` query and only once but otherwise behaves like ordinary table function
(for example, it can be used in subquery, etc.).

Data can be sent in any way like for ordinary `INSERT` query and passed in any available [format](/sql-reference/formats)
that must be specified in the end of query (unlike ordinary `INSERT SELECT`), or via the optional `format` argument.

The main feature of this function is that when server receives data from client it simultaneously converts it
according to the list of expressions in the `SELECT` clause and inserts into the target table. Temporary table
with all transferred data is not created.

## Examples {#examples}

- Let the `test` table has the following structure `(a String, b String)`
    and data in `data.csv` has a different structure `(col1 String, col2 Date, col3 Int32)`. Query for insert
    data from the `data.csv` into the `test` table with simultaneous conversion looks like this:

<!-- -->

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

- If `data.csv` contains data of the same structure `test_structure` as the table `test` then these two queries are equal:

<!-- -->

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

- You can infer the schema from a Native format stream:

```bash
$ clickhouse-client --query="SELECT 1 AS x, 'hello' AS y FORMAT Native" \
    | clickhouse-client --query="SELECT * FROM input('auto', 'Native')"
```
