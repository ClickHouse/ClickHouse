# Overlay Database Engine {#database-engine-overlay}

Overlay is a **virtual database engine** that sits on top of one or more existing databases and provides a single logical namespace for their tables. It does **not store data** by itself; it forwards reads and writes to its member databases.

Typical use-cases:

* A “search path” across multiple databases (e.g., a *base* DB plus a *tenant* DB).
* A blue/green or A/B layout where new tables go to one DB but existing tables continue to be found in another.
* Gradual migrations: create the overlay first, then shift tables between member DBs behind the scenes.

---

## How it works {#overlay-how-it-works}

**Members & order.** An Overlay database is created with an ordered list of member databases. Order **matters**.

**Lookup (reads).**

* `SELECT ... FROM overlay_db.t` resolves to the **first** member that has table `t`.
* If no member has `t`, the table is considered missing.

**Writes (DDL/DML).**

* `CREATE TABLE overlay_db.t ...` is executed in the **first writable** member (i.e., not read-only).
* `ALTER TABLE overlay_db.t ...` / `DROP TABLE overlay_db.t` are executed in the **member that actually owns** `t`.
* `RENAME TABLE overlay_db.t TO overlay_db2.u`:

  * If the destination is another Overlay database, the destination is the **first member** of that overlay.
  * Otherwise, the rename goes directly into the destination database.

**No data/metadata duplication.** Overlay does not copy or own table data. Tables *belong* to their member databases; Overlay just routes operations.

**Persistence & SHOW CREATE.** The list of members is **persisted** in the database metadata.
`SHOW CREATE DATABASE overlay_db` returns:

```sql
CREATE DATABASE overlay_db
ENGINE = Overlay('db_a', 'db_b', ..., 'db_n')
```

---

## Syntax {#overlay-syntax}

```sql
CREATE DATABASE name
ENGINE = Overlay('db1'[, 'db2', ...]);  -- members must already exist
```

* Member names may be identifiers or string literals. They are stored and shown as strings.
* At creation time, the engine validates that:

  * Each member exists.
  * There are no duplicates.
  * The overlay does not reference itself.

---

## Examples {#overlay-examples}

```sql
-- Prepare two Atomic databases
CREATE DATABASE db_a ENGINE = Atomic;
CREATE DATABASE db_b ENGINE = Atomic;

-- Create the overlay
CREATE DATABASE db_all ENGINE = Overlay('db_a', 'db_b');

-- Create: goes to first writable member (db_a)
CREATE TABLE db_all.events (id UInt32, ts DateTime) ENGINE = MergeTree ORDER BY id;

-- Read: finds the table in db_a via the overlay
SELECT count() FROM db_all.events;

-- Add a table in db_b directly
CREATE TABLE db_b.users (id UInt32, name String) ENGINE = MergeTree ORDER BY id;

-- Read through overlay: resolves to db_b since 'users' is not in db_a
SELECT * FROM db_all.users LIMIT 10;

-- Rename from overlay to another overlay: lands in the first member of the destination
CREATE DATABASE db_shadow ENGINE = Overlay('db_b', 'db_a');
RENAME TABLE db_all.events TO db_shadow.events_new;
```

---

## Semantics & limitations {#overlay-limitations}

* **Members engine types.** Best used with **Atomic** databases. Ordinary databases are deprecated; Memory databases are not recommended for persistence. (Overlay does not change member semantics.)
* **Replication.** Overlay itself is not a replicated engine. If members are replicated, table replication remains fully owned by those members.
* **UUIDs.** Overlay does not introduce its own database UUID. Tables keep their original UUIDs from their member databases.
* **System tables.** Unchanged. Overlay affects only user databases listed as members.
* **Backups & restore.** Backups enumerate tables via members. To restore, restore member databases first, then recreate the overlay.
* **`ON CLUSTER`.** Overlay is a local metadata construct; distributed DDL may not be meaningful for it. Prefer creating Overlay on each node with the same member list.
* **Conflicts.**

  * If multiple members contain the same table name, the **first** one wins for reads.
  * Creating a table with a name that already exists in an earlier member will fail with “table already exists”.
* **Availability.** All member databases must exist when you create or load the overlay. If a member is missing at startup, the server will fail to load the overlay with a clear error.
* **DROP DATABASE.** **Important:** in the current implementation, `DROP DATABASE overlay_db` iterates over members and calls `DROP DATABASE` on each. This **will drop underlying databases**. If you want to remove only the overlay wrapper and keep members, use `DETACH DATABASE overlay_db` (if available in your build) or manually delete the overlay metadata and **do not** run `DROP`. Consider changing this behavior in your environment if you want a non-destructive drop.

---

## Privileges {#overlay-privileges}

Overlay does not add new privilege types. Access control checks are performed against the **actual target database** (the member chosen for the operation):

* To **create** through the overlay you need the privileges required to create in the chosen member (e.g., `CREATE TABLE ON db_a.*`).
* To **alter/drop** a table through the overlay you need privileges on the member that owns the table.
* `SHOW CREATE DATABASE overlay_db` requires metadata read on the overlay and the ability to see member names.

---

## Operational notes {#overlay-operational-notes}

* **Member changes.** To change the member list or order, re-create the overlay with the desired list. Order controls resolution precedence.
* **Migrations.** To move a table from `db_a` to `db_b`: create it in `db_b`, copy data, adjust references (or `RENAME` across DBs), then drop the original. The overlay can hide the transition.
* **Monitoring.** Because overlay is just routing, monitor utilization and health of member databases; Overlay has no independent storage metrics.

---

## Error messages (selected) {#overlay-errors}

* `Unknown database engine: Overlay`
  The engine is not registered in your build.
* `Overlay database failed to access underlying database 'X': Database X does not exist`
  A member did not exist during create/load.
* `Database was renamed to 'X', cannot create table in 'overlay_db'`
  Indicates the DDL path resolved to a different effective database; verify member order and existence.
* `Mapping for table with UUID=... already exists`
  Symptom of trying to duplicate UUID mappings at attach time. Ensure the overlay itself does not try to register table UUIDs; leave that to members.

---

## Best practices {#overlay-best-practices}

* Prefer **Atomic** databases as members.
* Keep member list **short and ordered**; first-match semantics are simple and predictable.
* Avoid using **Memory** databases as members unless the overlay is intentionally ephemeral.
* Treat Overlay as a **routing layer** only. All durability, replication, and performance characteristics come from member databases.
* In production, make `DROP DATABASE` for Overlay **non-destructive** (drop the overlay only) to avoid accidental member loss. If your build currently cascades drop to members, change that behavior before adoption.
