---
description: 'Documentation for CREATE HANDLER'
sidebar_label: 'HANDLER'
sidebar_position: 49
slug: /sql-reference/statements/create/handler
title: 'CREATE HANDLER'
doc_type: 'reference'
---

Creates a custom HTTP handler defined from SQL, without editing the server configuration file. SQL-defined handlers are an alternative to the configuration-based [HTTP interface handlers](/interfaces/http).

## Syntax {#syntax}

```sql
CREATE [IF NOT EXISTS] HANDLER name
[PROTOCOL protocol_name]
URL [PREFIX|REGEXP] '/path'
[METHODS (GET, POST)]
[TYPE query]
AS [SELECT|INSERT|...] ...
```

Creates a handler with a specified `name`. The name is used for managing handlers with SQL queries, for diagnostic messages, and for ordering handlers.

## Clauses {#clauses}

- `PROTOCOL` — optional. If a protocol name is specified, the handler is active only for the specified [composable protocol](/operations/settings/composable-protocols). Otherwise, it is active for all of http/https protocols.
- `URL` — mandatory. Can be in the form of an exact URL, a `URL PREFIX`, or a `URL REGEXP`. For exact URLs and prefixes, ambiguity is checked at creation/alter time and an exception is thrown if there is ambiguity. For regexp, ambiguity cannot be checked. The URL is matched without the `?` query string and the `#` fragment identifier.
- `METHODS` — optional. The list of allowed HTTP methods. By default, it is only `GET`. The supported methods are `GET`, `POST`, `PUT` and `DELETE`.
- `TYPE` — optional. The only supported type for now is `query`.
- `AS` — the SQL query that will be invoked by this handler. The query can be parameterized. The query is parsed for syntactic correctness during handler creation/alter, but not analyzed - for example, the tables referenced by the query can be missing at the time of the handler creation. The `FORMAT` and similar clauses belong to the query, not to the whole `CREATE`/`ALTER` statement. The query can be put in parentheses for disambiguation. An `INSERT` query should not contain embedded data; the data is expected to be provided in the HTTP body.

## Priority {#priority}

Handlers defined in the server configuration have priority over SQL-defined handlers. SQL-defined handlers are matched in the lexicographical order of their names.

## Parameters {#parameters}

Any HTTP URL parameters in the query string, HTTP form variables in `POST`, and HTTP headers can be used as parameters for parameterized queries, just as with configuration-defined handlers. Named capture groups in a `URL REGEXP` are exposed as query parameters as well.

The functions [`currentHandler`](/sql-reference/functions/other-functions#currentHandler) and [`currentRequestURL`](/sql-reference/functions/other-functions#currentRequestURL) can be used to customize query behavior depending on the invoked handler and request URL.

## Access control {#access-control}

`CREATE HANDLER`, `DROP HANDLER` and `ALTER HANDLER` require the `CREATE HANDLER`, `DROP HANDLER` and `ALTER HANDLER` grants respectively.

Invoking a handler does not require any separate grant, but grants are checked as usual during the query invocation, and authentication works in the usual way. To encapsulate access to certain queries, create a [`VIEW` with `SQL SECURITY DEFINER`](/sql-reference/statements/create/view#sql_security) and define a handler that selects from that view.

## Storage {#storage}

Handlers are saved in a storage, which can be a local or Keeper storage, similarly to [named collections](/operations/named-collections), configured in the `query_rules_storage` section of the configuration file:

```xml
<query_rules_storage>
    <type>local</type> <!-- or zookeeper -->
    <path>/var/lib/clickhouse/handlers/</path>
</query_rules_storage>
```

## ALTER HANDLER {#alter-handler}

```sql
ALTER HANDLER name
[PROTOCOL protocol_name]
[URL '/path']
[METHODS (GET, POST)]
[TYPE query]
[AS SELECT ...]
```

Replaces the handler with a new one. The `ALTER` query can include only a subset of clauses, e.g., it can be used to only change the URL or the query. The unspecified clauses keep their previous values.

## DROP HANDLER {#drop-handler}

```sql
DROP [IF EXISTS] HANDLER name
```

Drops the handler with the specified name.

## Introspection {#introspection}

The [`system.handlers`](/operations/system-tables/handlers) table lists all SQL-defined handlers. The [`system.query_log`](/operations/system-tables/query_log) table records the handler name and the HTTP request URL of each query in the `http_handler_name` and `http_request_url` columns.

## Example {#example}

```sql
CREATE HANDLER my_handler URL '/my_handler' AS SELECT version();
```

```bash
$ curl 'http://localhost:8123/my_handler'
```

A parameterized handler with a regexp URL:

```sql
CREATE HANDLER get_user URL REGEXP '/users/(?P<id>\d+)' AS SELECT * FROM users WHERE id = {id:UInt64};
```

```bash
$ curl 'http://localhost:8123/users/42'
```
