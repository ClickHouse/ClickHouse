---
title: 'Overlay'
slug: /engines/database-engines/overlay
description: 'Creates a logical read-only database that exposes the union of tables from multiple underlying databases.'
sidebar_label: 'Overlay'
sidebar_position: 51
doc_type: 'reference'
---

# `Overlay` — design & behavior (current implementation) {#overlay}

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
| `CREATE TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Create the table in an underlying database.     |
| `ATTACH TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Attach the table in an underlying database.     |
| `ALTER TABLE dboverlay.*`  | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`.                                                |
| `RENAME TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`.                                                |
| `DROP TABLE dboverlay.*`   | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Drop the table in the underlying database that owns it. |
| `DETACH TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Detach the table in the underlying database that owns it. |
| `TRUNCATE TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Truncate the table in the underlying database that owns it. |
| `OPTIMIZE TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Optimize the table in the underlying database that owns it. |
| `DELETE FROM dboverlay.*`  | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Delete in the underlying database that owns the table.     |
| `UPDATE dboverlay.*`       | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Update in the underlying database that owns the table.     |
| `SYSTEM ... dboverlay.*` / `SYSTEM ... FROM DATABASE dboverlay` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Run the `SYSTEM` command (e.g. `STOP MERGES`, `RESTART REPLICA`, `DROP REPLICA`) against the underlying database that owns the table. Whole-server `SYSTEM` commands (with no database named) skip the facade and reach its source tables through their owner databases instead. |
| `TRUNCATE DATABASE dboverlay` / `TRUNCATE TABLES FROM dboverlay` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Truncate in the underlying databases that own the tables. |
| `INSERT INTO dboverlay.*`  | **Pass-through** — executes against the table in the corresponding underlying database.         |

> Rationale: the facade is a **view**. Data-definition & data-mutation happen in the member databases.

`DROP DATABASE dboverlay` drops the facade only — the member databases and their tables are left untouched.

## Error codes and messages (facade mode) {#error-codes}

| Scenario                                   | Error                                                                                                                                              |
| :----------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| Any mutating/management operation through the facade — `CREATE`/`ATTACH`/`ALTER`/`RENAME`/`DROP`/`DETACH`/`TRUNCATE`/`OPTIMIZE TABLE`, `DELETE FROM`, `UPDATE`, `SYSTEM`, `TRUNCATE DATABASE` | `TABLE_IS_PERMANENTLY_READ_ONLY` — "Database `<name>` is an Overlay facade (read-only). Run this operation in an underlying database." |
| Overlay references itself                  | `BAD_ARGUMENTS`                                                                                                                                    |
| Overlay reference cycle (e.g. `db_a` → `db_b` → `db_a`, formed by re-creating a source) | `BAD_ARGUMENTS` on any lookup through an affected Overlay; `DROP DATABASE` still works to break the cycle |
| Overlay references missing database at `CREATE` | `BAD_ARGUMENTS` — a user-initiated `CREATE DATABASE ... ENGINE = Overlay(...)` validates that every source exists right now |
| Overlay references missing database after `ATTACH`/restore/startup | No error — sources are resolved lazily by name, and a currently-missing source is simply omitted from the union until it is (re)created |
| DROP DATABASE overlay while tables “exist” | Succeeds (iterator/empty() semantics ensure no `DATABASE_NOT_EMPTY`)                                                                               |

---

## Notes {#notes}

* `DROP TABLE dboverlay.*` and `DETACH TABLE dboverlay.*` are rejected so that they cannot stop or detach the real table living in the underlying database. Drop or detach the table in the database that owns it.
* `DROP DATABASE dboverlay` drops the facade only; it does not call `shutdown`/`drop` on the member databases.
* `BACKUP DATABASE dboverlay` stores only the facade definition (`CREATE DATABASE … ENGINE = Overlay(...)`). The tables are backed up with the underlying databases that own them, so back up those databases (or `BACKUP ALL`) to capture the data.
* `RESTORE DATABASE dboverlay AS ...` rewrites the facade's source database names through the restore renaming map. If a source database is restored under a new name in the same `RESTORE` (e.g. `RESTORE DATABASE db_a AS db_a2, DATABASE dboverlay AS dboverlay2`), the restored facade points at the restored source (`db_a2`); a source that is not renamed is referenced under its original name.

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

-- DDL must target the underlying database; DROP TABLE dboverlay.t_new_renamed would throw TABLE_IS_PERMANENTLY_READ_ONLY.
DROP TABLE db_a.t_new_renamed;
SHOW TABLES FROM dboverlay;                -- t_new_renamed disappears

-- Remove the overlay (does not touch member DBs)
DROP DATABASE dboverlay SYNC;
```

---

## Access control {#access-control}

Accessing a table through an `Overlay` database requires a grant on **both** the `Overlay`
database (the name written in the query) and the underlying source database that owns the
table. This applies to both reads and `INSERT`s:

* a `SELECT` grant on the `Overlay` alone is **not** enough to read through the facade — the
  user must also be granted `SELECT` on the underlying source database;
* a `SELECT` grant on an underlying database alone is **not** enough to read through the
  facade either, though it does allow reading that database directly (independently of the
  `Overlay`);
* `INSERT` through the facade likewise requires the `INSERT` privilege on both the `Overlay`
  and the underlying source database.

Row policies follow the same rule: reading a table through the facade applies the `SELECT` row
policies of **both** the `Overlay` and the underlying source table (a row is returned only if it
passes both). A row policy defined on the source table still applies to direct reads of that
table, independently of the `Overlay`.

Creating an `Overlay` database requires a `SELECT` privilege on each underlying database
it unions. A user who cannot read a source database therefore cannot expose it through a
new `Overlay`. Creating an `Overlay` confers no privileges on the overlay database itself;
as with any database engine, the creator must additionally be granted the relevant
privileges on the `Overlay` before they can use it.

```sql
-- Both grants are required to read db_a tables through `dboverlay`:
GRANT SELECT ON dboverlay.* TO some_user;
GRANT SELECT ON db_a.* TO some_user;
```

## Compatibility notes {#compatibility}

* Overlay is intentionally **read-only** at the DDL surface (`CREATE`/`ATTACH`/`ALTER`/`RENAME`/`DROP`/`DETACH`/`TRUNCATE` are rejected); reads and `INSERT` pass through to the underlying tables.
