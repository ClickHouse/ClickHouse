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
- Restores the current roles to the user's default roles. Externally-granted roles (e.g. LDAP or interserver) installed at session start are re-applied, so the reset session keeps the same privileges a fresh authentication would have.
- Restores the current database. The candidate chain is: the database the connection was opened with (captured only by the native TCP handler at handshake; the other interfaces — HTTP, gRPC, Arrow Flight, and `clickhouse-local` — skip this step because they have no equivalent handshake hook), then the user's `DEFAULT DATABASE`, then the server's current database (matching what a fresh authentication leaves in place when the user has no profile default). The first existing candidate is selected, so an admin-dropped database does not break the reset. This does not apply to the PostgreSQL and MySQL protocols, where `RESET SESSION` is a no-op (see [below](#postgresql-and-mysql-protocols-no-op)).
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

`RESET SESSION` resets the ClickHouse session **context** — the state listed above, which the server tracks per session. On the interfaces where it runs, it does **not** touch state that a wire-protocol handler keeps outside that context. In particular, **prepared statements are not invalidated by `RESET SESSION`**:

- Arrow Flight: prepared statement handles remain valid until closed via the Flight SQL `ClosePreparedStatement` action or the session is closed.

Do not rely on `RESET SESSION` to clear prepared statements. (Over the MySQL and PostgreSQL protocols `RESET SESSION` does nothing at all — see [below](#postgresql-and-mysql-protocols-no-op) — so their prepared statements, like everything else, are left untouched.)

This is a deliberate scoping decision for the first version of the statement: `RESET SESSION` is a server-side, session-context reset. Extending it to drive per-protocol cleanup is tracked as future work.

### PostgreSQL and MySQL protocols: no-op {#postgresql-and-mysql-protocols-no-op}

`RESET SESSION` is a **no-op over the PostgreSQL and MySQL wire protocols** and leaves the session unchanged.

The PostgreSQL emulation exposes the `pg_*` compatibility views (`pg_type`, `pg_namespace`, ...) as session-scoped temporary tables; a real reset would clear them without re-creating them, breaking later driver metadata queries.

The MySQL handler keeps protocol-local state outside the session context — the handshake database it applies before the first query, and prepared statements registered with `COM_STMT_PREPARE` — that a session-context reset would not account for, leaving the connection off its post-handshake baseline.

Rather than half-reset the session, ClickHouse skips the reset entirely on these interfaces. To reset a pooled connection reached over the PostgreSQL or MySQL protocol, drop and reopen it.

### Other known limitations {#known-limitations}

- **Client-side server-provided settings.** When `apply_settings_from_server` is enabled, `clickhouse-client` keeps the settings it received in the initial handshake until it reconnects. If an administrator changes the user's profile after the connection was opened, `RESET SESSION` picks up the new profile on the server, but the client continues to format and parse with the handshake-time settings until the next reconnect.
- **`clickhouse-local` command-line settings overridden by the profile.** A setting passed on the `clickhouse-local` command line survives `RESET SESSION` unless the user's profile also sets that same setting. Because reset re-derives settings from the profile, a profile entry takes precedence on reset over the command-line value (at startup the command line wins instead). This affects only settings present both on the command line and in the profile.

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
