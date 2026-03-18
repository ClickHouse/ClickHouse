---
description: 'Run non-readonly queries in a background thread and return query_id immediately'
sidebar_label: 'detach_non_readonly_queries'
sidebar_position: 65
slug: /operations/settings/detach-non-readonly-queries
title: 'detach_non_readonly_queries'
doc_type: 'reference'
---

# detach_non_readonly_queries {#detach-non-readonly-queries}

When enabled, non-readonly queries (e.g. `INSERT`, `INSERT ... SELECT`, `DELETE`, `UPDATE`) are **detached**: the server returns immediately with the `query_id` while the query runs in a background thread.

When using a **server** (queries sent to a running ClickHouse server), behavior depends on the interface:
- **HTTP:** response is HTTP 200 with `query_id` in the `X-ClickHouse-Query-Id` header and in the response body.
- **Native protocol (clickhouse-client):** the client receives a result block containing the `query_id` and then end-of-stream.

For [clickhouse-local](/operations/utilities/clickhouse-local), this setting is supported **only in interactive mode** (when running without `-q` and reading queries from the terminal). In non-interactive mode (e.g. `clickhouse-local -q "INSERT INTO ..."`), queries always run synchronously.

This is **distinct from [async_insert](/operations/settings/settings#async_insert)**:
- **async_insert** queues small inserts and may batch them; it changes insert semantics and supports `wait_for_async_insert`.
- **detach_non_readonly_queries** only detaches execution (return immediately with `query_id`, run in a background thread); it does not batch or queue inserts.

**Possible values:** `0` (default), `1`

**Default value:** `0`

## Enabling the setting {#enabling-the-setting}

You can enable it in one of these ways:

**1. In the request URL (per request):**

```bash
curl -sS "http://localhost:8123/?detach_non_readonly_queries=1&query=INSERT+INTO+my_table+SELECT+1" -X POST -d ""
```

**2. In the server config** (e.g. `config.xml` or `users.xml`):

```xml
<profiles>
    <default>
        <detach_non_readonly_queries>1</detach_non_readonly_queries>
    </default>
</profiles>
```

**3. In a session (SQL):**

```sql
SET detach_non_readonly_queries = 1;
```

**4. Native client (clickhouse-client):** pass the setting on the command line or in the query (when connected to a server):

```bash
clickhouse-client --detach_non_readonly_queries 1 -q "INSERT INTO my_table SELECT 1"
# or in the query:
clickhouse-client -q "INSERT INTO my_table SELECT 1 SETTINGS detach_non_readonly_queries=1"
```

## Response {#response}

When a non-readonly query is detached:

- **HTTP:** status `200 OK`, header `X-ClickHouse-Query-Id: <query_id>`, body is the same `<query_id>` (e.g. a UUID string).
- **Native client:** the client receives a single result row with column `query_id` (e.g. printed as one line in TSV), then end-of-stream.

The connection can be closed immediately; the query continues on the server. You can use the `query_id` for logging or to look up the query in `system.processes` / `system.query_log` while it runs.

## Examples {#examples}

### Query in URL (INSERT ... SELECT) {#query-in-url}

```bash
# Detached INSERT ... SELECT; server returns immediately with query_id
curl -sS "http://localhost:8123/?detach_non_readonly_queries=1&query=INSERT+INTO+my_table+SELECT+number+FROM+numbers(1000)" -X POST -d ""
```

Example response body: `a1b2c3d4-e5f6-7890-abcd-ef1234567890`

### Query in POST body only (no ?query= in URL) {#query-in-post}

Useful when the query is long or you prefer to send it in the body. The body must be **uncompressed** (do not use `decompress=1`); see [Limitations](#limitations).

```bash
# Query only in body; no query= in URL (body must be uncompressed)
curl -sS "http://localhost:8123/?detach_non_readonly_queries=1" -X POST --data-binary "INSERT INTO my_table SELECT number FROM numbers(1000)"
```

### INSERT with data in body (query in URL, data in body) {#insert-with-data-in-body}

```bash
# Query in URL, INSERT data in body
curl -sS "http://localhost:8123/?detach_non_readonly_queries=1&query=INSERT+INTO+my_table+FORMAT+TabSeparated" -X POST -d "1
2
3"
```

### Read-only queries (SELECT) are not detached {#read-only-queries}

SELECT and other read-only queries always run synchronously; the response contains the result as usual:

```bash
# SELECT is unchanged: result is in the response body
curl -sS "http://localhost:8123/?detach_non_readonly_queries=1&query=SELECT+count()+FROM+my_table" -X POST -d ""
```

### Native client (clickhouse-client) {#native-client}

With the setting enabled, an `INSERT ... SELECT` returns immediately with the `query_id` in the result block (one row, one column):

```bash
# Enable via command line
clickhouse-client --detach_non_readonly_queries 1 -q "INSERT INTO my_table SELECT number FROM numbers(10)"
# Output (example):  a1b2c3d4-e5f6-7890-abcd-ef1234567890

# Or enable in the query
clickhouse-client -q "INSERT INTO my_table SELECT 1 SETTINGS detach_non_readonly_queries=1"
```

SELECT is never detached; you get the normal result:

```bash
clickhouse-client --detach_non_readonly_queries 1 -q "SELECT count() FROM my_table"
# Output: normal result (e.g. 10)
```

## Limitations {#limitations}

- **clickhouse-local non-interactive:** This setting does **not** apply when running clickhouse-local in non-interactive mode (e.g. with `-q "query"`). In that case queries run synchronously. Detach is supported only in [interactive mode](/operations/utilities/clickhouse-local).
- **async_insert:** If [async_insert](/operations/settings/settings#async_insert) is enabled, the detach path is **not** used (async-insert queue semantics are preserved). The POST body is always treated as query+data for the async-insert queue, so “query in POST body only” does not apply.
- **Compressed body (decompress=1):** When the request uses the `decompress=1` parameter (ClickHouse native compression), the server does **not** treat the body as “query in POST body only”. The body is consumed by the decompression pipeline. For detached execution with the query in the body, send an **uncompressed** body (omit `decompress=1`). For compressed INSERT data, put the query in the URL (`?query=...`) and send the compressed data in the body.
- **Request type:** Only HTTP POST is supported; GET requests are not detached.
- **External data:** Requests with multipart/form-data (e.g. for external tables) are not detached.
- **Body size:** The request body used as query or data is limited (e.g. 128 MiB); see server limits.

## See also {#see-also}

- [async_insert](/operations/settings/settings#async_insert) — asynchronous insert batching (different feature)
- [wait_for_async_insert](/operations/settings/settings#wait_for_async_insert) — wait for async insert flush
- [INSERT INTO](/sql-reference/statements/insert-into) — INSERT statement reference
