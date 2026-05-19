---
description: 'Dispatch a query to a background thread and return its `query_id` immediately'
sidebar_label: 'allow_experimental_detach_queries'
sidebar_position: 65
slug: /operations/settings/detach-queries
title: 'allow_experimental_detach_queries'
doc_type: 'reference'
---

# allow_experimental_detach_queries {#detach-queries}

When enabled, any detachable query is dispatched to a background thread and the server returns immediately with its `query_id`. Detachable kinds include `SELECT`, `INSERT ... SELECT`, `DELETE`, `UPDATE`, `ALTER`, `CREATE`, `OPTIMIZE`, `SHOW`, `EXPLAIN`, `SYSTEM`, and `CHECK`. Session-mutating queries (`SET`, `USE`, `BEGIN`/`COMMIT`/`ROLLBACK`, `KILL QUERY`) are **not** detached — they always run synchronously so they take effect in the calling session.

Detaching a `SELECT` is mainly useful for benchmarks, soak tests and warm-up runs where only "did it finish" matters; the result rows are discarded.

When using a **server** (queries sent to a running ClickHouse server), behavior depends on the interface:
- **HTTP:** response is `HTTP 200` with `query_id` in the `X-ClickHouse-Query-Id` header and in the response body.
- **Native protocol (clickhouse-client):** the client receives a result block containing the `query_id` and then end-of-stream.

For [clickhouse-local](/operations/utilities/clickhouse-local), this setting is supported **only in interactive mode** (when running without `-q` and reading queries from the terminal). In non-interactive mode (e.g. `clickhouse-local -q "INSERT INTO ..."`), queries always run synchronously.

This is **distinct from [async_insert](/operations/settings/settings#async_insert)**:
- `async_insert` queues small inserts and may batch them; it changes insert semantics and supports `wait_for_async_insert`.
- `allow_experimental_detach_queries` only detaches execution (return immediately with `query_id`, run in a background thread); it does not batch or queue inserts.

:::important Mutually exclusive with `async_insert`
The two settings are **mutually exclusive**. When both are enabled the server logs a one-line warning and runs the query through the synchronous async-insert path — including for non-INSERT queries (e.g. `SELECT`), which fall through to the regular synchronous path.

[`async_insert`](/operations/settings/settings#async_insert) is **enabled by default** (`async_insert=1`), so to use `allow_experimental_detach_queries` you must explicitly disable it: set `async_insert=0` in the URL, on the command line, in the SETTINGS clause, or in the session/profile. Every example below sets `async_insert=0` for this reason.

Use `async_insert` for INSERT fire-and-forget; use this setting for everything else.
:::

**Possible values:** `0` (default), `1`

**Default value:** `0`

## Enabling the setting {#enabling-the-setting}

You can enable it in one of these ways:

**1. In the request URL (per request):**

```bash
curl -sS "http://localhost:8123/?allow_experimental_detach_queries=1&async_insert=0&query=INSERT+INTO+my_table+SELECT+1" -X POST -d ""
```

**2. In the server config** (e.g. `config.xml` or `users.xml`):

```xml
<profiles>
    <default>
        <allow_experimental_detach_queries>1</allow_experimental_detach_queries>
        <async_insert>0</async_insert>
    </default>
</profiles>
```

**3. In a session (SQL):**

```sql
SET allow_experimental_detach_queries = 1, async_insert = 0;
```

**4. Native client (clickhouse-client):** pass the setting on the command line or in the query (when connected to a server):

```bash
clickhouse-client --allow_experimental_detach_queries 1 --async_insert 0 -q "INSERT INTO my_table SELECT 1"
# or in the query:
clickhouse-client -q "INSERT INTO my_table SELECT 1 SETTINGS allow_experimental_detach_queries=1, async_insert=0"
```

## Response {#response}

When a query is detached:

- **HTTP:** status `200 OK`, header `X-ClickHouse-Query-Id: <query_id>`, body is the same `<query_id>` (e.g. a UUID string).
- **Native client:** the client receives a single result row with column `query_id` (e.g. printed as one line in TSV), then end-of-stream.

The connection can be closed immediately; the query continues on the server. You can use the `query_id` for logging or to look up the query in `system.processes` / `system.query_log` while it runs.

## Examples {#examples}

### `INSERT ... SELECT` with query in URL {#query-in-url}

```bash
# Detached INSERT ... SELECT; server returns immediately with query_id
curl -sS "http://localhost:8123/?allow_experimental_detach_queries=1&async_insert=0&query=INSERT+INTO+my_table+SELECT+number+FROM+numbers(1000)" -X POST -d ""
```

Example response body: `a1b2c3d4-e5f6-7890-abcd-ef1234567890`

### Query in POST body only (no `?query=` in URL) {#query-in-post}

Useful when the query is long or you prefer to send it in the body. The body must be **uncompressed** (do not use `decompress=1`); see [Limitations](#limitations).

```bash
# Query only in body; no query= in URL (body must be uncompressed)
curl -sS "http://localhost:8123/?allow_experimental_detach_queries=1&async_insert=0" -X POST --data-binary "INSERT INTO my_table SELECT number FROM numbers(1000)"
```

### `INSERT` with data in body (query in URL, data in body) {#insert-with-data-in-body}

```bash
# Query in URL, INSERT data in body
curl -sS "http://localhost:8123/?allow_experimental_detach_queries=1&async_insert=0&query=INSERT+INTO+my_table+FORMAT+TabSeparated" -X POST -d "1
2
3"
```

### Detached `SELECT` (benchmark / warm-up use case) {#select-detach}

When the setting is on, even read-only queries are detached — useful when you only care that the query completed (e.g. warming caches or measuring server-side cost without paying for result transfer):

```bash
# Detached SELECT — response is just the query_id, the result rows are discarded
curl -sS "http://localhost:8123/?allow_experimental_detach_queries=1&async_insert=0&query=SELECT+count()+FROM+huge_table" -X POST -d ""
# Output: <query_id>
```

For the rows themselves, run the same `SELECT` without the setting (the default).

### Native client (clickhouse-client) {#native-client}

With the setting enabled, the client receives one row containing the `query_id`:

```bash
# Enable via command line
clickhouse-client --allow_experimental_detach_queries 1 --async_insert 0 -q "INSERT INTO my_table SELECT number FROM numbers(10)"
# Output (example):  a1b2c3d4-e5f6-7890-abcd-ef1234567890

# Or enable in the query
clickhouse-client -q "INSERT INTO my_table SELECT 1 SETTINGS allow_experimental_detach_queries=1, async_insert=0"
```

### Session-mutating queries are not detached {#session-mutating}

`SET`, `USE`, `BEGIN`, `COMMIT`, `ROLLBACK`, and `KILL QUERY` always run synchronously even with the setting on — otherwise they would silently no-op on a detached context:

```bash
# SET runs synchronously; max_threads is actually updated in this session
clickhouse-client --allow_experimental_detach_queries 1 --async_insert 0 -q "SET max_threads=4; SELECT value FROM system.settings WHERE name='max_threads'"
```

## Limitations {#limitations}

- **clickhouse-local non-interactive:** This setting does **not** apply when running clickhouse-local in non-interactive mode (e.g. with `-q "query"`). In that case queries run synchronously. Detach is supported only in [interactive mode](/operations/utilities/clickhouse-local).
- **async_insert:** [`async_insert`](/operations/settings/settings#async_insert) is **enabled by default** (`async_insert=1`) and is mutually exclusive with this setting. When both are on, the detach path is **not** used (including for non-INSERT queries) and the server logs a one-line warning. Set `async_insert=0` whenever you use `allow_experimental_detach_queries`.
- **`INSERT ... FORMAT ...` on native/local:** When the data is delivered on a separate native packet (clickhouse-client, clickhouse-local), the background thread cannot drain it after the handler has returned. Such inserts run synchronously. HTTP buffers the body before deciding, so HTTP `INSERT ... FORMAT ...` is detachable.
- **Compressed body (`decompress=1`):** When the request uses the `decompress=1` parameter (ClickHouse native compression), the server does **not** treat the body as "query in POST body only". The body is consumed by the decompression pipeline. For detached execution with the query in the body, send an **uncompressed** body (omit `decompress=1`). For compressed INSERT data, put the query in the URL (`?query=...`) and send the compressed data in the body.
- **Request type (HTTP):** Only HTTP POST is supported; GET requests are not detached.
- **External data:** Requests with multipart/form-data (e.g. for external tables) are not detached.
- **Body size:** The request body used as query or data is limited (e.g. 128 MiB); see server limits.

## See also {#see-also}

- [async_insert](/operations/settings/settings#async_insert) — asynchronous insert batching (different feature)
- [wait_for_async_insert](/operations/settings/settings#wait_for_async_insert) — wait for async insert flush
- [INSERT INTO](/sql-reference/statements/insert-into) — INSERT statement reference
