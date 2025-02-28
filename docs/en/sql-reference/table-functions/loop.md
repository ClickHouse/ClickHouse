# loop

**Syntax**

``` sql
SELECT ... FROM loop(database, table);
SELECT ... FROM loop(database.table);
SELECT ... FROM loop(table);
SELECT ... FROM loop(other_table_function(...));
```

**Parameters**

- `database` — database name.
- `table` — table name.
- `other_table_function(...)` — other table function.
  Example: `SELECT * FROM loop(numbers(10));`
  `other_table_function(...)` here is `numbers(10)`.

**Returned Value**

Infinite loop to return query results.

**Examples**

Selecting data from ClickHouse:

``` sql
SELECT * FROM loop(test_database, test_table);
SELECT * FROM loop(test_database.test_table);
SELECT * FROM loop(test_table);
```

Or using other table function:

``` sql
SELECT * FROM loop(numbers(3)) LIMIT 7;
   ┌─number─┐
1. │      0 │
2. │      1 │
3. │      2 │
   └────────┘
   ┌─number─┐
4. │      0 │
5. │      1 │
6. │      2 │
   └────────┘
   ┌─number─┐
7. │      0 │
   └────────┘
``` 
``` sql
SELECT * FROM loop(mysql('localhost:3306', 'test', 'test', 'user', 'password'));
...
```