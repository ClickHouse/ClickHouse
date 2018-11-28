<a name="permissions_for_queries"></a>

# Permissions for queries

Queries in ClickHouse can be divided into several groups:

1. Read data queries: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
1. Write data queries: `INSERT`, `OPTIMIZE`.
1. Change settings queries: `SET`, `USE`.
1. [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
1. Particular queries: `KILL QUERY`.

The following settings regulate user permissions for the groups of queries:

- [readonly](#settings_readonly) — Restricts permissions for all groups of queries excepting DDL.
- [allow_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` performs with any settings.

<a name="settings_readonly"></a>

## readonly

Restricts permissions for read data, write data and change settings queries.

See [above](#permissions_for_queries) for the division of queries into groups.

**Possible values**

- 0 — All queries are allowed. Default value.
- 1 — Read data queries only are allowed.
- 2 — Read data and change settings queries are allowed.

After setting `readonly = 1`, a user can't change `readonly` and `allow_ddl` settings in the current session.

When using the `GET` method in the [HTTP interface](../../interfaces/http.md#http_interface), `readonly = 1` is set automatically. To modify data use the `POST` method.

<a name="settings_allow_ddl"></a>

## allow_ddl

Allows/denies [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries.

See [above](#permissions_for_queries) for the division of queries into groups.

**Possible values**

- 0 — DDL queries are not allowed.
- 1 — DDL queries are allowed. Default value.

You can not execute `SET allow_ddl = 1` if `allow_ddl = 0` for current session.

[Original article](https://clickhouse.yandex/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
