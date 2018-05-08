<a name="table_engines-mysql"></a>

# MySQL

The MySQL engine allows you to perform SELECT queries on data that is stored on a remote MySQL server.

The engine takes 4 parameters: the server address (host and port); the name of the database; the name of the table; the user's name; the user's password. Example:

```text
MySQL('host:port', 'database', 'table', 'user', 'password');
```

At this time, simple WHERE clauses such as ```=, !=, >, >=, <, <=``` are executed on the MySQL server.

The rest of the conditions and the LIMIT sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

