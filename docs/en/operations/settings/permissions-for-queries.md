---
toc_priority: 58
toc_title: Permissions for Queries
---

# Permissions for Queries {#permissions_for_queries}

Queries in ClickHouse can be divided into several types:

1.  Read data queries: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  Write data queries: `INSERT`, `OPTIMIZE`.
3.  Change settings query: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

The following settings regulate user permissions by the type of query:

-   [readonly](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [allow\_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` can be performed with any settings.

## readonly {#settings_readonly}

Restricts permissions for reading data, write data and change settings queries.

See how the queries are divided into types [above](#permissions_for_queries).

Possible values:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

After setting `readonly = 1`, the user can’t change `readonly` and `allow_ddl` settings in the current session.

When using the `GET` method in the [HTTP interface](../../interfaces/http.md), `readonly = 1` is set automatically. To modify data, use the `POST` method.

Setting `readonly = 1` prohibit the user from changing all the settings. There is a way to prohibit the user
from changing only specific settings, for details see [constraints on settings](../../operations/settings/constraints-on-settings.md).

Default value: 0

## allow\_ddl {#settings_allow_ddl}

Allows or denies [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries.

See how the queries are divided into types [above](#permissions_for_queries).

Possible values:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

You can’t execute `SET allow_ddl = 1` if `allow_ddl = 0` for the current session.

Default value: 1

[Original article](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
