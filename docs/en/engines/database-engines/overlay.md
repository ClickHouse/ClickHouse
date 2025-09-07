# DatabaseOverlay — design & behavior (current implementation)

This document describes the **Overlay** database engine you just implemented: what it is, how it’s constructed, how it behaves in each mode, what operations are supported/blocked, and how to test and troubleshoot it.

---

## What is `DatabaseOverlay`?

`DatabaseOverlay` is a logical “view” database that exposes **the union of tables from multiple underlying databases**. It does **not** own data itself. It supports two modes:

* `Mode::OwnedMembers` — used by **clickhouse-local** helper code; the overlay sits *on top of* member DBs it creates/owns.
* `Mode::FacadeOverCatalog` — used by the SQL factory (`ENGINE = Overlay(...)`); the overlay is a **read-only facade** over existing, separately managed databases.

---

## Construction

### Factory (SQL)

```sql
CREATE DATABASE dboverlay ENGINE = Overlay('db_overlay_a', 'db_overlay_b');
```

* The factory resolves every underlying DB name via `DatabaseCatalog::tryGetDatabase(name)`.
* **Null checks are enforced**: a missing underlying DB throws `BAD_ARGUMENTS` with a clear message.
* Self-reference is rejected.

### Programmatic (clickhouse-local helper)

```cpp
auto overlay = std::make_shared<DatabaseOverlay>(
    name_, context, DatabaseOverlay::Mode::OwnedMembers);

overlay->registerNextDatabase(std::make_shared<DatabaseAtomic>(...));
overlay->registerNextDatabase(std::make_shared<DatabaseFilesystem>(...));
```

---

## UUID semantics

* **FacadeOverCatalog**: `getUUID()` returns **`UUIDHelpers::Nil`**.
  This makes the overlay **skip DB-UUID registration** in the global catalog, eliminating collisions with DB/table UUIDs.
* **OwnedMembers**: `getUUID()` returns the **first member’s non-Nil UUID**, preserving the behavior expected by `clickhouse-local`.

---

## What operations does the overlay support?

### Table/DB discovery

* `SHOW TABLES FROM dboverlay` — returns the **union** of member DB tables.
* `SELECT … FROM dboverlay.table` — **reads** transparently hit the underlying table.

### Mutating operations (facade mode)

| Operation                  | Behavior                                                                                                    |
| :------------------------- | :---------------------------------------------------------------------------------------------------------- |
| `CREATE TABLE dboverlay.*` | **Rejected** — throws `BAD_ARGUMENTS` with a clear message to create in an underlying DB.                   |
| `ATTACH TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`.                                                                             |
| `ALTER TABLE dboverlay.*`  | **Rejected** — `BAD_ARGUMENTS`.                                                                             |
| `RENAME TABLE dboverlay.*` | **Rejected** — `BAD_ARGUMENTS`.                                                                             |
| `DROP TABLE dboverlay.*`   | **No-op** — ignored (allows `DROP DATABASE dboverlay` to succeed).                                          |
| `INSERT INTO dboverlay.*`  | **Not supported** — typically fails with `TABLE_UUID_MISMATCH` since storage is bound to the underlying DB. |

> Rationale: the facade is a **view**. Data-definition & data-mutation happen in the member databases.

### Database lifecycle (facade mode)

* `DROP DATABASE dboverlay` — **succeeds** and **does not** cascade to members.

  * Implementation details that make this safe:

    * `getUUID()` returns **Nil** (no DB UUID mapping).
    * `getTablesIterator(ctx, filter, /*skip_not_loaded=*/true)` returns **empty** (so the dropper doesn’t see tables).
    * `empty()` returns **true** (facade reports empty during destructive passes).
    * `dropTable()` is a **no-op** (dropper won’t fail mid-way).
    * `drop()` is a **no-op** (doesn’t touch member DBs).
* `isReadOnly()` returns **false** (so the server doesn’t block `DROP DATABASE` up front). Mutating table ops are still guarded individually.

---

## Resilience & safety

* **Null-safety**: all loops over `databases` skip `nullptr` entries; `registerNextDatabase()` rejects `nullptr`.
  This prevents crashes from bad metadata or attach order.
* **No-op on startup tasks** in facade mode: `loadStoredObjects`, `beforeLoadingMetadata`, `loadTablesMetadata`, `loadTableFromMetadata*`, `startup*`, `wait*`, `stopLoading` — all return immediately.
  This avoids double registrations and keeps the overlay a pure view.

---

## Error codes and messages (facade mode)

| Scenario                                   | Error                                                                                                                                              |
| :----------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| Overlay CREATE/ATTACH/ALTER/RENAME TABLE   | `BAD_ARGUMENTS` — “Database `<name>` is an Overlay facade (read-only view). Run this operation in an underlying database (e.g. `<first_member>`).” |
| INSERT via overlay                         | `TABLE_UUID_MISMATCH` (from lower layers)                                                                                                          |
| Overlay references itself                  | `BAD_ARGUMENTS`                                                                                                                                    |
| Overlay references missing DB              | `BAD_ARGUMENTS`                                                                                                                                    |
| DROP DATABASE overlay while tables “exist” | Succeeds (iterator/empty() semantics ensure no `DATABASE_NOT_EMPTY`)                                                                               |

---

## Logging

* Uses `LOG_TRACE` (include `#include <Common/logger_useful.h>` or `#include <base/logger_useful.h>` depending on your tree).
* On `DROP TABLE` in facade mode, logs a trace and **ignores** the drop.

---
Examples of usage
```sql
-- Create/prepare underlying DBs and tables
CREATE DATABASE db_overlay_a ENGINE = Atomic;
CREATE DATABASE db_overlay_b ENGINE = Atomic;

CREATE TABLE db_overlay_a.t_a (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db_overlay_b.t_b (id UInt32, s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO db_overlay_a.t_a VALUES (1,'a1'), (2,'a2');
INSERT INTO db_overlay_b.t_b VALUES (10,'b10'), (20,'b20');

-- Create the overlay facade
CREATE DATABASE dboverlay ENGINE = Overlay('db_overlay_a', 'db_overlay_b');

-- Discover and read through overlay
SHOW TABLES FROM dboverlay;                -- t_a, t_b
SELECT * FROM dboverlay.t_a ORDER BY id;   -- rows from db_overlay_a.t_a
SELECT * FROM dboverlay.t_b ORDER BY id;   -- rows from db_overlay_b.t_b

-- Add a new table in an underlying DB (overlay is read-only for DDL)
CREATE TABLE db_overlay_a.t_new (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO db_overlay_a.t_new VALUES (100,'x'), (200,'y');

-- Read the new table via overlay
SHOW TABLES FROM dboverlay;                -- now includes t_new
SELECT * FROM dboverlay.t_new ORDER BY k;  -- rows from db_overlay_a.t_new

-- Rename/drop in the underlying DB; overlay reflects changes
RENAME TABLE db_overlay_a.t_new TO db_overlay_a.t_new_renamed;
SELECT * FROM dboverlay.t_new_renamed ORDER BY k;

DROP TABLE db_overlay_a.t_new_renamed;
SHOW TABLES FROM dboverlay;                -- t_new_renamed disappears

-- Remove the overlay (does not touch member DBs)
DROP DATABASE dboverlay SYNC;
```
---

## Troubleshooting

| Symptom                                                                               | Cause                                                                     | Fix                                                                                                                                |
| :------------------------------------------------------------------------------------ | :------------------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------- |
| `Logical error: 'Mapping for table with UUID=… already exists'` when creating overlay | Overlay DB had a non-Nil UUID colliding with existing DB/table            | In facade mode, **return Nil** from `getUUID()` (you already do).                                                                  |
| `TABLE_ALREADY_EXISTS` under `store/000/00000000-…`                                   | Table was created **through** overlay; its storage bound to Nil-UUID path | Block `CREATE TABLE` via overlay; create in a member DB instead. Remove stale Nil dir if created.                                  |
| `TABLE_UUID_MISMATCH` on `INSERT INTO dboverlay.t_new`                                | DML via overlay not supported; storage UUIDs must match                   | Write to the underlying DB and read via overlay.                                                                                   |
| `DATABASE_NOT_EMPTY` while dropping overlay                                           | Dropper saw tables or `empty()` returned false                            | In facade mode: return **empty iterator** for `skip_not_loaded==true` and **true** from `empty()`. Make `dropTable()` a **no-op**. |
| Segfault in `canContainMergeTreeTables()` (or similar)                                | `databases` contained a **null** member                                   | Enforce non-null on registration; add `if (db)` guards in all loops (done).                                                        |

---

## Compatibility notes

* Facade overlay is intentionally **read-only** at the DDL/DML surface; it’s a **discovery & read** tool.
* `clickhouse-local` path (`OwnedMembers`) preserves the legacy expectation that overlay UUID equals the first member’s UUID.

---

## Future work (if desired)

* Add a **write-through** mode (retarget CREATE/INSERT ASTs to a chosen member).
* Optional setting to **error** (instead of no-op) on `DROP TABLE dboverlay.*`.
* Surface member list in `SHOW CREATE DATABASE` (already included) and possibly `system.databases` extras.

If you want this as a `docs/` markdown file, say the word and I’ll format it to your repository’s style guide.
