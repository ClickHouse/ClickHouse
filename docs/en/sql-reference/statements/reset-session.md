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
- Restores the current database. The candidate chain is: the database the connection was opened with (captured by the native, MySQL, and PostgreSQL handlers at handshake; HTTP, gRPC, and `clickhouse-local` skip this step), then the user's `DEFAULT DATABASE`, then the server's current database (matching what a fresh authentication leaves in place when the user has no profile default). The first existing candidate is selected, so an admin-dropped database does not break the reset.
- Drops every temporary table created in the session.
- Clears all query parameters set with `SET param_name = ...`.
- Clears all scalars sent over the protocol.

The authenticated user identity, the connection's client information, and the output format negotiated at handshake (e.g. `MySQLWire`, `PostgreSQLWire`) are preserved.

The user's profiles are re-read from access control on every `RESET SESSION` call, so any changes an administrator has made to the user's profile since the session was opened take effect immediately. If a database in the candidate chain is missing, the next candidate is tried, so admin-dropped databases never cause `RESET SESSION` to fail.

`RESET SESSION` on an already-clean session is a no-op.

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
