---
title: 'Overlay'
slug: /engines/database-engines/overlay
description: 'Creates a logical read-only database that exposes the union of tables from multiple underlying databases.'
sidebar_label: 'Overlay'
sidebar_position: 51
doc_type: 'reference'
---

# `Overlay` — design & behavior (current implementation)

## What is `Overlay`? {#introduction}

`Overlay` is a logical "view" database that exposes **the union of tables from multiple underlying databases**. It does **not** own data itself.

`Overlay` is used in `clickhouse-local` to represent local files from the filesystem in the default database.

---

## What operations does the overlay support? {#implementation}

### Table/Database discovery {#discovery}

* `SHOW TABLES FROM dboverlay` — returns the **union** of member database tables.
* `SELECT … FROM dboverlay.table` — **reads** transparently hit the underlying table.

#### Lookup order {#lookup-order}

Sources are searched in the order they were listed in `CREATE DATABASE … ENGINE = Overlay(...)`. When two member databases contain tables with the same name, the table from the database listed **first** in the engine arguments wins. Duplicate names in the argument list are removed while preserving the first occurrence.

### Mutating operations (facade mode) {#operations}

| Operation                  | Behavior                                                                                        |
| :------------------------- | :-----------------------------------------------------------------------------------------------|
| `CREATE TABLE dboverlay.*` | **Rejected** — throws `BAD_ARGUMENTS` with a message, suggesting to create in an underlying database. |
| `ATTACH TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`.                                                                 |
| `ALTER TABLE dboverlay.*`  | **Rejected** — `TABLE_IS_READ_ONLY`.                                                            |
| `RENAME TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`.                                                                 |
| `DROP TABLE dboverlay.*`   | **Rejected** — `BAD_ARGUMENTS`. Drop the table in the underlying database that owns it.         |
| `DETACH TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`. Detach the table in the underlying database that owns it.       |
| `TRUNCATE TABLE dboverlay.*` | **Rejected** — `TABLE_IS_READ_ONLY`. Truncate the table in the underlying database that owns it. |
| `INSERT INTO dboverlay.*`  | **Pass-through** — executes against the table in the corresponding underlying database.         |

> Rationale: the facade is a **view**. Data-definition & data-mutation happen in the member databases.

`DROP DATABASE dboverlay` drops the facade only — the member databases and their tables are left untouched.

## Error codes and messages (facade mode) {#error-codes}

| Scenario                                   | Error                                                                                                                                              |
| :----------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| Overlay `CREATE`/`ATTACH`/`RENAME`/`DROP`/`DETACH TABLE` | `BAD_ARGUMENTS` — "Database `<name>` is an Overlay facade (read-only). Run this operation in an underlying database." |
| Overlay `ALTER`/`TRUNCATE` | `TABLE_IS_READ_ONLY` — "Database `<name>` is an Overlay facade (read-only). Run this operation in an underlying database." |
| Overlay references itself                  | `BAD_ARGUMENTS`                                                                                                                                    |
| Overlay reference cycle (e.g. `db_a` → `db_b` → `db_a`, formed by re-creating a source) | `BAD_ARGUMENTS` on any lookup through an affected Overlay; `DROP DATABASE` still works to break the cycle |
| Overlay references missing database        | `BAD_ARGUMENTS`                                                                                                                                    |
| DROP DATABASE overlay while tables “exist” | Succeeds (iterator/empty() semantics ensure no `DATABASE_NOT_EMPTY`)                                                                               |

---

## Notes {#notes}

* `DROP TABLE dboverlay.*` and `DETACH TABLE dboverlay.*` are rejected so that they cannot stop or detach the real table living in the underlying database. Drop or detach the table in the database that owns it.
* `DROP DATABASE dboverlay` drops the facade only; it does not call `shutdown`/`drop` on the member databases.
* `BACKUP DATABASE dboverlay` stores only the facade definition (`CREATE DATABASE … ENGINE = Overlay(...)`). The tables are backed up with the underlying databases that own them, so back up those databases (or `BACKUP ALL`) to capture the data.

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

-- DDL must target the underlying database; DROP TABLE dboverlay.t_new_renamed would throw BAD_ARGUMENTS.
DROP TABLE db_a.t_new_renamed;
SHOW TABLES FROM dboverlay;                -- t_new_renamed disappears

-- Remove the overlay (does not touch member DBs)
DROP DATABASE dboverlay SYNC;
```

---

## Access control {#access-control}

Access checks use the database name as written in the query. To read a table through an `Overlay` database, a user needs a `SELECT` grant on the `Overlay` database itself; grants on the underlying databases are neither required nor consulted for queries that go through the facade. Conversely, a grant on an underlying database does not allow reading the same table through the `Overlay`.

```sql
GRANT SELECT ON dboverlay.* TO some_user;  -- allows reading any table exposed by `dboverlay`
GRANT SELECT ON db_a.* TO some_user;       -- allows reading db_a tables directly, not via `dboverlay`
```

:::note
A grant on the `Overlay` database effectively grants read access to every table it exposes, including tables added to the underlying databases later. Grant access to an `Overlay` database only to users who may read all of its sources.
:::

## Compatibility notes {#compatibility}

* Overlay is intentionally **read-only** at the DDL surface (`CREATE`/`ATTACH`/`ALTER`/`RENAME`/`DROP`/`DETACH`/`TRUNCATE` are rejected); reads and `INSERT` pass through to the underlying tables.
