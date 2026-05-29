---
description: 'Documentation for RESET SESSION'
sidebar_label: 'RESET SESSION'
sidebar_position: 54
slug: /sql-reference/statements/reset-session
title: 'RESET SESSION Statement'
doc_type: 'reference'
---

## Syntax {#syntax}

```sql
RESET SESSION
```

## Description {#description}

Restores the current session to the state it was in immediately after authentication, without dropping the connection or re-authenticating.

The statement:

- Resets all session-level settings to the user's profile defaults plus any settings supplied by the authentication server.
- Restores the current roles to the user's default roles and drops any externally-granted roles applied at session start.
- Restores the current database. The candidate chain is: the database the connection was opened with (captured only by the native TCP handler at handshake; all other interfaces — HTTP, gRPC, MySQL, PostgreSQL, Arrow Flight, and `clickhouse-local` — skip this step because they have no equivalent handshake hook), then the user's `DEFAULT DATABASE`, then the server's current database (matching what a fresh authentication leaves in place when the user has no profile default). The first existing candidate is selected, so an admin-dropped database does not break the reset.
- Drops every temporary table created in the session.
- Clears all query parameters set with `SET param_name = ...`.
- Clears all scalars sent over the protocol.
- Drops every in-memory backup created in the session with `BACKUP ... TO Memory(name)`. Subsequent `RESTORE ... FROM Memory(name)` calls fail with `BACKUP_NOT_FOUND` and the name becomes reusable.

The authenticated user identity, the connection's client information, and the output format negotiated at handshake (e.g. `MySQLWire`, `PostgreSQLWire`) are preserved.

The user's profiles are re-read from access control on every `RESET SESSION` call, so any changes an administrator has made to the user's profile since the session was opened take effect immediately. If a database in the candidate chain is missing, the next candidate is tried, so admin-dropped databases never cause `RESET SESSION` to fail.

`RESET SESSION` on an already-clean session is a no-op.

## Behavior with active transactions {#behavior-with-active-transactions}

`RESET SESSION` does **not** silently roll back an active transaction. If the session is inside a transaction (i.e. `BEGIN` was issued and the transaction has not yet been committed or rolled back), `RESET SESSION` is rejected with `INVALID_TRANSACTION` and the transaction is left untouched.

Connection-pool implementations that issue `RESET SESSION` before returning a connection to the pool must first `COMMIT` or `ROLLBACK` any open transaction. The intent is to avoid silently discarding writes a client believed it had open.

## Scope: protocol-local state is not reset {#scope-protocol-local-state}

`RESET SESSION` resets the ClickHouse session **context** — the state listed above, which the server tracks per session. It does **not** touch state that a wire-protocol handler keeps outside that context. In particular, **prepared statements created over the MySQL, PostgreSQL, and Arrow Flight protocols are not invalidated by `RESET SESSION`**:

- MySQL: statements prepared with `COM_STMT_PREPARE` remain registered. They can be closed individually with `COM_STMT_CLOSE`; ClickHouse's MySQL endpoint does not implement a bulk connection-reset command (`COM_RESET_CONNECTION`), so to fully discard protocol-local state, drop and reopen the connection.
- PostgreSQL: server-side prepared statements (`PREPARE name AS ...` / extended-query `Parse` messages) remain registered. Remove them individually with `DEALLOCATE name`, or drop and reopen the connection.
- Arrow Flight: prepared statement handles remain valid until closed via the Flight SQL `ClosePreparedStatement` action or the session is closed.

Do not rely on `RESET SESSION` to clear prepared statements on these protocols.

This is a deliberate scoping decision for the first version of the statement: `RESET SESSION` is a server-side, session-context reset. Extending it to drive per-protocol cleanup is tracked as future work.

### PostgreSQL protocol: no-op {#postgresql-protocol-no-op}

`RESET SESSION` is a **no-op over the PostgreSQL wire protocol** and leaves the session unchanged. The PostgreSQL emulation exposes the `pg_*` compatibility views (`pg_type`, `pg_namespace`, ...) as session-scoped temporary tables; a real reset would clear them without re-creating them, breaking later driver metadata queries. Rather than half-reset the session, ClickHouse skips the reset entirely on this interface. To reset a pooled connection reached over the PostgreSQL protocol, drop and reopen it.

### Other known limitations {#known-limitations}

- **Authentication-server `profile` settings.** Settings returned by an external authentication server are replayed on reset, but a `profile` entry in that response is applied as a plain setting rather than re-derived through the profile machinery. If your auth server returns a `profile`, prefer assigning the profile to the user in access control so it is restored as part of the normal profile re-derivation.
- **Client-side server-provided settings.** When `apply_settings_from_server` is enabled, `clickhouse-client` keeps the settings it received in the initial handshake until it reconnects. If an administrator changes the user's profile after the connection was opened, `RESET SESSION` picks up the new profile on the server, but the client continues to format and parse with the handshake-time settings until the next reconnect.

## Comparison with PostgreSQL {#comparison-with-postgresql}

The behaviour is closest to PostgreSQL's `DISCARD ALL`. PostgreSQL's `RESET ALL` only restores settings (`GUC` parameters) and does not drop temporary tables or other session state; `RESET SESSION` in ClickHouse is broader.

## Example {#example}

```sql
SET max_threads = 1;
SET param_x = '42';
CREATE TEMPORARY TABLE t (x Int) ENGINE = Memory;
USE system;

RESET SESSION;

SELECT getSetting('max_threads');     -- profile default + any auth-server setting
SELECT * FROM t;                       -- UNKNOWN_TABLE
SELECT currentDatabase();              -- the connection-start database (or user profile default)
```
