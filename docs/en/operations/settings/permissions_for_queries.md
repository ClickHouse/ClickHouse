# Permissions for queries {#permissions_for_queries}

Queries in ClickHouse can be divided into several types:

1. Read data queries: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
1. Write data queries: `INSERT`, `OPTIMIZE`.
1. Change settings queries: `SET`, `USE`.
1. [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
1. `KILL QUERY`.

The following settings regulate user permissions by the type of query:

- [readonly](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
- [allow_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` can be performed with any settings.

## readonly {#settings_readonly}

Restricts permissions for read data, write data and change settings queries.

See how the queries are divided into types [above](#permissions_for_queries).

**Possible values**

- 0 — All queries are allowed.
- 1 — Only read data queries are allowed.
- 2 — Read data and change settings queries are allowed.

After setting `readonly = 1`, the user can't change `readonly` and `allow_ddl` settings in the current session.

When using the `GET` method in the [HTTP interface](../../interfaces/http.md), `readonly = 1` is set automatically. To modify data, use the `POST` method.

**Default value**

0

## allow_ddl {#settings_allow_ddl}

Allows/denies [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries.

See how the queries are divided into types [above](#permissions_for_queries).

**Possible values**

- 0 — DDL queries are not allowed.
- 1 — DDL queries are allowed.

You cannot execute `SET allow_ddl = 1` if `allow_ddl = 0` for the current session.

**Default value**

1

[Original article](https://clickhouse.yandex/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
