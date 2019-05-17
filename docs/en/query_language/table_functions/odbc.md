# odbc {#table_functions-odbc}

Returns table that is connected via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

```
odbc(connection_settings, external_database, external_table)
```

**Function Parameters**

- `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
- `external_database` — Name of a database in an external DBMS.
- `external_table` — Name of a table in the `external_database`.

To implement ODBC connection, ClickHouse uses the separate program `clickhouse-odbc-bridge`. ClickHouse starts this program automatically when it is required. The ODBC bridge program is installed by the same package as the `clickhouse-server`. ClickHouse use the ODBC bridge program because it is unsafe to load ODBC driver. In case of problems with ODBC driver it can crash the `clickhouse-server`.

This function supports the [Nullable](../../data_types/nullable.md) data type (based on DDL of remote table that is queried).

**Examples**

```
select * from odbc('DSN=connection_settings_name', 'external_database_name', 'external_table_name')
```

## See Also

- [ODBC external dictionaries](../../query_language/dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-odbc)
- [ODBC table engine](../../operations/table_engines/odbc.md).

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/jdbc/) <!--hide-->
