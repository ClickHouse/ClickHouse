<a name="table_engines-mysql"></a>

# MySQL

The MySQL engine allows you to perform SELECT queries on data that is stored on a remote MySQL server.

The engine takes 5-7 parameters: the server address (host and port); the name of the database; the name of the table; the user's name; the user's password; whether to use replace query; the on duplcate clause. Example:

```text
MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

At this time, simple WHERE clauses such as ```=, !=, >, >=, <, <=``` are executed on the MySQL server.

The rest of the conditions and the LIMIT sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

If `replace_query` is specified to 1, then `INSERT INTO` query to this table would be transformed to `REPLACE INTO`.
If `on_duplicate_clause` is specified, eg `update impression = values(impression) + impression`, it would add `on_duplicate_clause` to the end of the MySQL insert sql.
Notice that only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them.
