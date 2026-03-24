---
description: 'Documentation for CREATE HANDLER, ALTER HANDLER, and DROP HANDLER statements'
sidebar_label: 'HANDLER'
slug: /sql-reference/statements/create/handler
title: 'CREATE HANDLER'
doc_type: 'reference'
---

Manages SQL-defined custom HTTP handlers. Handlers allow exposing pre-defined SQL queries as HTTP endpoints without editing XML configuration files.

:::note
This feature is experimental. Enable it with `SET allow_experimental_sql_handlers = 1`.
:::

## CREATE HANDLER {#create-handler}

Creates a new HTTP handler.

**Syntax**

```sql
CREATE [IF NOT EXISTS] HANDLER name URL [PREFIX|REGEXP] '/path' [METHODS (GET, POST, ...)] AS 'query'
```

**Parameters**

- `name` — Handler identifier. Used for matching priority (lexicographic order) and management.
- `URL '/path'` — Exact URL path to match. Default matching mode.
- `URL PREFIX '/path'` — Match any URL that starts with the given path prefix.
- `URL REGEXP '/pattern'` — Match URLs using an RE2 regular expression.
- `METHODS (GET, POST, ...)` — Allowed HTTP methods. Defaults to `GET` if omitted.
- `AS 'query'` — The SQL query to execute when the handler matches.

**Example**

```sql
SET allow_experimental_sql_handlers = 1;

-- Exact URL match, GET only
CREATE HANDLER my_uptime URL '/api/uptime' AS 'SELECT uptime()';

-- Prefix match, multiple methods
CREATE HANDLER my_api URL PREFIX '/api/v1/' METHODS (GET, POST) AS 'SELECT version()';

-- Regexp match
CREATE HANDLER versioned_api URL REGEXP '^/api/v[0-9]+/status$' AS 'SELECT 1 AS ok';
```

Test with curl:

```bash
curl http://localhost:8123/api/uptime
```

## ALTER HANDLER {#alter-handler}

Modifies an existing handler. Only specified clauses are updated; omitted clauses retain their current values.

**Syntax**

```sql
ALTER HANDLER [IF EXISTS] name [URL [PREFIX|REGEXP] '/path'] [METHODS (GET, POST, ...)] [AS 'query']
```

**Example**

```sql
-- Change only the query
ALTER HANDLER my_uptime AS 'SELECT uptime(), version()';

-- Change URL and methods
ALTER HANDLER my_uptime URL '/api/v2/uptime' METHODS (GET, POST);
```

## DROP HANDLER {#drop-handler}

Removes an existing handler.

**Syntax**

```sql
DROP HANDLER [IF EXISTS] name
```

**Example**

```sql
DROP HANDLER my_uptime;

-- No error if handler does not exist
DROP HANDLER IF EXISTS my_uptime;
```

## Persistence {#persistence}

Handlers are persisted to local disk (under the `handlers_metadata/` directory in the ClickHouse data path) and restored on server startup. The storage backend can be configured in the server config:

```xml
<custom_handlers_storage>
    <type>local</type>
    <!-- Optional: override the default path -->
    <path>/var/lib/clickhouse/handlers_metadata/</path>
</custom_handlers_storage>
```

## Matching Order {#matching-order}

SQL-defined handlers are matched **after** config-defined handlers (`<http_handlers>` section) but **before** the default dynamic query handler. Among SQL-defined handlers, matching is done in **lexicographic order by handler name**.

## Access Control {#access-control}

Three new privileges control handler management:

- `CREATE_HANDLER` — required for `CREATE HANDLER`
- `ALTER_HANDLER` — required for `ALTER HANDLER`
- `DROP_HANDLER` — required for `DROP HANDLER`

Grant example:

```sql
GRANT CREATE_HANDLER, ALTER_HANDLER, DROP_HANDLER ON *.* TO my_user;
```
