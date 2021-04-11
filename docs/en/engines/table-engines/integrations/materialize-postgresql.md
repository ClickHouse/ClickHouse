---
toc_priority: 12
toc_title: MateriaziePostgreSQL
---

# MaterializePostgreSQL {#materialize-postgresql}

## Creating a Table {#creating-a-table}

## Requirements {#requirements}

- A table with engine `MaterializePostgreSQL` must have a primary key - the same as a replica identity index of a postgres table (See [details on replica identity index](../../database-engines/materialize-postgresql.md#requirements)).

- Only database `Atomic` is allowed.

- Setting `wal_level`to `logical` and `max_replication_slots` to at least `2` in the postgresql config file.
