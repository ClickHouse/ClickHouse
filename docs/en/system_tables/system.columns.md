# system.columns

Contains information about the columns in all tables.
You can use this table to get information similar to `DESCRIBE TABLE`, but for multiple tables at once.

```text
database String           - Name of the database the table is located in.
table String              - Table name.
name String               - Column name.
type String               - Column type.
default_type String       - Expression type (DEFAULT, MATERIALIZED, ALIAS) for the default value, or an empty string if it is not defined.
default_expression String - Expression for the default value, or an empty string if it is not defined.
```

