# odbc

Returns table that is connected via ODBC driver.

```
odbc(connection_settings, external_database, external_table)
```

**Function Parameters**

- `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
- `external_database` — Database in an external DBMS.
- `external_table` — A name of the table in `external_database`.

To implement ODBC connection, ClickHouse uses the separate program `clickhouse-odbc-bridge`. ClickHouse starts this program automatically when it is required. The ODBC bridge program is installed by the same package with the ClickHouse server.

This function supports the [Nullable](../../data_types/nullable.md) data type (based on DDL of remote table that is queried).

**Examples**

```
select * from odbc('DSN=connection_settings_name', 'external_database_name', 'external_table_name')
```

Some examples of how to use external dictionaries through ODBC you can find in the [ODBC](../../query_language/dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-odbc) section of external dictionaries configuration.

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/jdbc/) <!--hide-->
