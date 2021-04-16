---
toc_priority: 4
toc_title: MySQL
---

# MySQL {#mysql}

The MySQL engine allows you to perform `SELECT` queries on data that is stored on a remote MySQL server.


## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original MySQL table structure:

-   Column names should be the same as in the original MySQL table, but you can use just some of these columns and in any order.
-   Column types may differ from those in the original MySQL table. ClickHouse tries to [cast](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) values to the ClickHouse data types.
-   Setting `external_table_functions_use_nulls` defines how to handle Nullable columns. Default is true, if false - table function will not make nullable columns and will insert default values instead of nulls. This is also applicable for null values inside array data types.

**Engine Parameters**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` queries to `REPLACE INTO`. If `replace_query=1`, the query is substituted.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expression that is added to the `INSERT` query.

    Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.

    To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

Simple `WHERE` clauses such as `=, !=, >, >=, <, <=` are executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.


Instead of `host:port` parameter it is possible to use:

-   `addresses_expr` —  An expression that generates addresses of remote servers, which are regarded as replicas of the same priority. ([Detailed description](../../../sql-reference/table_functions/remote.md))

**Engine configuration**

Connection creadetials can be added via server configuration. Server config is always checked first. If there are no credentials in config for a given address, then credentials from table definition are used. In case of `addresses_expr` some of the addresses' credentials can be added via config, other, if absent, will be taken from definition.

``` xml
 <mysql>
    <host1>
        <user>user1</user>
        <password>pass1</password>
    <host1>
    <host2>
        <user>user2</user>
        <password>pass2</password>
    <host2>
    ...
 </mysql>
```

Credentials configuration via config is useful in case of `addresses_expr` and `INSERT QUERY`: if there is more than one replica for a table, `INSERT` query will be sent to a replica, where `user` has been granted an `INSERT` privilege on the table. This behavior can be turned of by setting `external_storage_check_insert_privilege`. By default is `true` and if a table has multiple replicas, it will always be checked.


## Usage Example {#usage-example}

Table in MySQL:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Table in ClickHouse, retrieving data from the MySQL table created above:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```


## See Also {#see-also}

-   [The ‘mysql’ table function](../../../sql-reference/table-functions/mysql.md)
-   [Using MySQL as a source of external dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[Original article](https://clickhouse.tech/docs/en/engines/table-engines/integrations/mysql/) <!--hide-->
