<a name="table_engines-mysql"></a>

# MySQL

The MySQL engine allows you to perform `SELECT` queries on data that is stored on a remote MySQL server.

Call format:

```
MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**Call parameters**

- `host:port` — Address of the MySQL server.
- `database` — Database name on the MySQL server.
- `table` — Name of the table.
- `user` — The MySQL User.
- `password` — User password.
- `replace_query` — Flag that sets query substitution `INSERT INTO` to `REPLACE INTO`. If `replace_query=1`, the query is replaced.
- `'on_duplicate_clause'` — Adds the `ON DUPLICATE KEY UPDATE 'on_duplicate_clause'` expression to the `INSERT` query. For example: `impression = VALUES(impression) + impression`. To specify `'on_duplicate_clause'` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `'on_duplicate_clause'`, ClickHouse generates an exception.

At this time, simple `WHERE`  clauses such as ` =, !=, >, >=, <, <=` are executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

The `MySQL` engine does not support the [Nullable](../../data_types/nullable.md#data_type-nullable) data type, so when reading data from MySQL tables, `NULL` is converted to default values for the specified column type (usually 0 or an empty string).

