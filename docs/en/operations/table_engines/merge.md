# Merge

The `Merge`  engine (not to be confused with `MergeTree`) does not store data itself, but allows reading from any number of other tables simultaneously.
Reading is automatically parallelized. Writing to a table is not supported. When reading, the indexes of tables that are actually being read are used, if they exist.
The `Merge`  engine accepts parameters: the database name and a regular expression for tables.

Example:

```
Merge(hits, '^WatchLog')
```

Data will be read from the tables in the `hits`  database that have names that match the regular expression '`^WatchLog`'.

Instead of the database name, you can use a constant expression that returns a string. For example, `currentDatabase()`.

Regular expressions — [re2](https://github.com/google/re2) (supports a subset of PCRE), case-sensitive.
See the notes about escaping symbols in regular expressions in the "match" section.

When selecting tables to read, the `Merge`  table itself will not be selected, even if it matches the regex. This is to avoid loops.
It is possible to create two `Merge`  tables that will endlessly try to read each others' data, but this is not a good idea.

The typical way to use the `Merge`  engine is for working with a large number of `TinyLog`  tables as if with a single table.

## Virtual Columns

Virtual columns are columns that are provided by the table engine, regardless of the table definition. In other words, these columns are not specified in `CREATE TABLE`, but they are accessible for `SELECT`.

Virtual columns differ from normal columns in the following ways:

- They are not specified in table definitions.
- Data can't be added to them with `INSERT`.
- When using `INSERT` without specifying the list of columns, virtual columns are ignored.
- They are not selected when using the asterisk (`SELECT *`).
- Virtual columns are not shown in `SHOW CREATE TABLE` and `DESC TABLE` queries.

The `Merge` type table contains a virtual `_table` column of the `String` type. (If the table already has a `_table` column, the virtual column is called `_table1`; if you already have `_table1`, it's called `_table2`, and so on.) It contains the name of the table that data was read from.

If the `WHERE/PREWHERE`  clause contains conditions for the `_table`  column that do not depend on other table columns (as one of the conjunction elements, or as an entire expression), these conditions are used as an index. The conditions are performed on a data set of table names to read data from, and the read operation will be performed from only those tables that the condition was triggered on.

