---
title: 'Overlay'
slug: /engines/database-engines/overlay
description: 'Creates a logical read-only database that exposes the union of tables from multiple underlying databases.'
sidebar_label: 'Overlay'
sidebar_position: 51
---

# `Overlay` — design & behavior (current implementation)

## What is `Overlay`? {#introduction}

`Overlay` is a logical "view" database that exposes **the union of tables from multiple underlying databases**. It does **not** own data itself.

`Overlay` is used in `clickhouse-local` for to load local files in temporary ephemeral databases.

---

## What operations does the overlay support? {#implementation}

### Table/Database discovery {#discovery}

* `SHOW TABLES FROM dboverlay` — returns the **union** of member database tables.
* `SELECT … FROM dboverlay.table` — **reads** transparently hit the underlying table.

### Mutating operations (facade mode) {#operations}

| Operation                  | Behavior                                                                                        |
| :------------------------- | :-----------------------------------------------------------------------------------------------|
| `CREATE TABLE dboverlay.*` | **Rejected** — throws `BAD_ARGUMENTS` with a clear message to create in an underlying database. |
| `ATTACH TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`.                                                                 |
| `ALTER TABLE dboverlay.*`  | **Pass-through** — forwards to the appropriate underlying database.                             |
| `RENAME TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`.                                                                 |
| `DROP TABLE dboverlay.*`   | **No-op** — ignored (allows `DROP DATABASE dboverlay` to succeed).                              |
| `INSERT INTO dboverlay.*`  | **Pass-through** — executes against the table in the corresponding underlying database.         |

> Rationale: the facade is a **view**. Data-definition & data-mutation happen in the member databases.

## Error codes and messages (facade mode) {#error-codes}

| Scenario                                   | Error                                                                                                                                              |
| :----------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| Overlay CREATE/ATTACH/ALTER/RENAME TABLE   | `BAD_ARGUMENTS` — “Database `<name>` is an Overlay facade (read-only view). Run this operation in an underlying database (e.g. `<first_member>`).” |
| Overlay references itself                  | `BAD_ARGUMENTS`                                                                                                                                    |
| Overlay references missing database        | `BAD_ARGUMENTS`                                                                                                                                    |
| DROP DATABASE overlay while tables “exist” | Succeeds (iterator/empty() semantics ensure no `DATABASE_NOT_EMPTY`)                                                                               |

---

## Notes {#notes}

* On `DROP TABLE` in facade mode, logs a trace and **ignores** the drop.

---

## Examples of use {#examples-of-use}

```sql
-- Create/prepare underlying DBs and tables
CREATE DATABASE db_a ENGINE = Atomic;
CREATE DATABASE db_b ENGINE = Atomic;

CREATE TABLE db_a.t_a (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db_b.t_b (id UInt32, s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO db_a.t_a VALUES (1,'a1'), (2,'a2');
INSERT INTO db_b.t_b VALUES (10,'b10'), (20,'b20');

-- Create the overlay facade
CREATE DATABASE dboverlay ENGINE = Overlay('db_a', 'db_b');

-- Discover and read through overlay
SHOW TABLES FROM dboverlay;                -- t_a, t_b
SELECT * FROM dboverlay.t_a ORDER BY id;   -- rows from db_a.t_a
SELECT * FROM dboverlay.t_b ORDER BY id;   -- rows from db_b.t_b

-- Add a new table in an underlying database (overlay is read-only for DDL)
CREATE TABLE db_a.t_new (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO db_a.t_new VALUES (100,'x'), (200,'y');

-- Read the new table via overlay
SHOW TABLES FROM dboverlay;                -- now includes t_new
SELECT * FROM dboverlay.t_new ORDER BY k;  -- rows from db_a.t_new

-- Rename/drop in the underlying DB; overlay reflects changes
RENAME TABLE db_a.t_new TO db_a.t_new_renamed;
SELECT * FROM dboverlay.t_new_renamed ORDER BY k;

DROP TABLE db_a.t_new_renamed;
SHOW TABLES FROM dboverlay;                -- t_new_renamed disappears

-- Remove the overlay (does not touch member DBs)
DROP DATABASE dboverlay SYNC;
```

---

## Compatibility notes {#compatibility}

* Overlay is intentionally **read-only** at the DDL/DML surface; it’s a **discovery & read** tool.
