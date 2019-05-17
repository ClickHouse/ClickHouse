# ODBC {#table_engine-odbc}

Allows ClickHouse to connect to external databases via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

To implement ODBC connection, ClickHouse uses the separate program `clickhouse-odbc-bridge`. ClickHouse starts this program automatically when it is required. The ODBC bridge program is installed by the same package as the `clickhouse-server`. ClickHouse use the ODBC bridge program because it is unsafe to load ODBC driver. In case of problems with ODBC driver it can crash the `clickhouse-server`.

This engine supports the [Nullable](../../data_types/nullable.md) data type.

## Creating a Table

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name  ENGINE = ODBC(connection_settings, external_database, external_table)
```

**Engine Parameters**

- `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
- `external_database` — Name of a database in an external DBMS.
- `external_table` — Name of a table in the `external_database`.

## Usage Example

## See Also

- [ODBC external dictionaries](../../query_language/dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-odbc)
- [ODBC table function](../../query_language/table_functions/odbc.md).

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/jdbc/) <!--hide-->
