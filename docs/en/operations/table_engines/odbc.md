# ODBC

Allows ClickHouse to connect to external databases via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

To implement ODBC connection, ClickHouse uses the separate program `clickhouse-odbc-bridge`. ClickHouse starts this program automatically when it is required. The ODBC bridge program is installed by the same package with the ClickHouse server.

This engine supports the [Nullable](../../data_types/nullable.md) data type.

## Creating a Table

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name  ENGINE = ODBC(connection_settings, external_database, external_table)
```

**Engine Parameters**

- `connection_settings` — Name of the sections with connection settings in the `odbc.ini` file.
- `external_database` — Database in an external DBMS.
- `external_table` — A name of the table in `external_database`.

## Usage Example

Some examples of how to use external dictionaries through ODBC you can find in the [ODBC](../../query_language/dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-odbc) section of external dictionaries configuration.

## See Also

- [ODBC table function](../../query_language/table_functions/odbc.md).

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/jdbc/) <!--hide-->
