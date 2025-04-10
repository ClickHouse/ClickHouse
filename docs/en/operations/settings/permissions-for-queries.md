---
slug: /en/operations/settings/permissions-for-queries
sidebar_position: 58
sidebar_label: Permissions for Queries
---

# Permissions for Queries

Queries in ClickHouse can be divided into several types:

1.  Read data queries: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  Write data queries: `INSERT`, `OPTIMIZE`.
3.  Change settings query: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

The following settings regulate user permissions by the type of query:

## readonly
Restricts permissions for read data, write data, and change settings queries.

When set to 1, allows:

- All types of read queries (like SELECT and equivalent queries).
- Queries that modify only session context (like USE).

When set to 2, allows the above plus:
- SET and CREATE TEMPORARY TABLE

  :::tip
  Queries like EXISTS, DESCRIBE, EXPLAIN, SHOW PROCESSLIST, etc are equivalent to SELECT, because they just do select from system tables.
  :::

Possible values:

- 0 — Read, Write, and Change settings queries are allowed.
- 1 — Only Read data queries are allowed.
- 2 — Read data and Change settings queries are allowed.

Default value: 0

:::note
After setting `readonly = 1`, the user can’t change `readonly` and `allow_ddl` settings in the current session.

When using the `GET` method in the [HTTP interface](../../interfaces/http.md), `readonly = 1` is set automatically. To modify data, use the `POST` method.

Setting `readonly = 1` prohibits the user from changing settings. There is a way to prohibit the user from changing only specific settings. Also there is a way to allow changing only specific settings under `readonly = 1` restrictions. For details see [constraints on settings](../../operations/settings/constraints-on-settings.md).
:::


## allow_ddl {#allow_ddl}

Allows or denies [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries.

Possible values:

- 0 — DDL queries are not allowed.
- 1 — DDL queries are allowed.

Default value: 1

:::note
You cannot run `SET allow_ddl = 1` if `allow_ddl = 0` for the current session.
:::


:::note KILL QUERY
`KILL QUERY` can be performed with any combination of readonly and allow_ddl settings.
:::
