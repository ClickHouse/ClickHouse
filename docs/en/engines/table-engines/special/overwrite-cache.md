---
description: 'Stores one deterministic latest row per composite key in memory and serves indexed lookups.'
sidebar_label: 'OverwriteCache'
sidebar_position: 115
slug: /engines/table-engines/special/overwrite-cache
title: 'OverwriteCache table engine'
doc_type: 'reference'
---

The `OverwriteCache` engine stores exactly one winning row for each composite key in RAM. It is intended for bounded latest-state caches that are rebuilt from an upstream source.

Rows are held in RAM and are served from RAM, but they are also written to disk, so the cache survives a restart, a `DETACH`, and a `RENAME`. See [Persistence](#persistence). Setting `persist_mode = 'none'` gives a purely in-memory table whose content is lost on restart.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE latest_state
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version DateTime64(3),
    source_sequence UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag), (website_type), (website_type, tag)
SETTINGS
    max_memory_bytes = 1073741824,
    equal_version_tiebreak_columns = 'source_sequence',
    persist_mode = 'async';
```

`OverwriteCache` requires one engine argument identifying the version column and a nonempty storage-level `KEYS (...)` clause. The version column cannot also be a key column.

## Winner selection {#winner-selection}

For each composite key, an inserted row is handled as follows:

1. A greater version replaces the current row.
2. A lower version is ignored.
3. Equal versions compare `equal_version_tiebreak_columns` in declaration order.
4. A row that is equal on both the version and the tie-break columns is ignored, whatever its payload is.

Rule 4 makes a repeated insert of the same data a no-op, so re-delivering rows from the upstream source is harmless. It also means that rows which are genuinely different but indistinguishable to winner selection do not fail the insert: the row already stored wins. Which row that is depends on insertion order, and insertion order is not stable — a `INSERT ... SELECT` is divided into blocks by its pipeline, and concurrent inserts are ordered by arrival. Two caches rebuilt from the same upstream source can therefore hold different payloads for such a key. If the result has to be reproducible, `equal_version_tiebreak_columns` must fully determine the winner.

The `OverwriteCacheEqualVersionTies` profile event counts inserted rows ignored by rule 4. A nonzero value on a table whose tie-break columns are meant to be unique points at duplicate rows in the upstream query.

Memory-limit validation occurs before publishing mutations from an inserted block. `OverwriteCache` does not evict rows when its memory limit is reached.

## Concurrent publication {#concurrent-publication}

`OverwriteCache` addresses primary keys and lookup-index postings through fixed hash shards. Existing rows use a fixed set of striped row locks rather than one lock per row. This bounds lock memory independently of the number of stored keys.

An inserted block is prepared as a pending publication. Each affected row gets a new version tagged with the not-yet-published generation, so readers keep resolving the previous version while the writer works. One atomic generation change then makes the complete block visible. A query captures one generation, so it does not mix rows from before and after the same publication.

A writer never waits for readers. A query that captured an older generation keeps its own versions of the rows it can still reach, and those versions are released once no live query can observe them. A long-running `SELECT` therefore delays reclamation rather than blocking concurrent inserts, and `INSERT INTO ... SELECT` reading the same table completes normally. While such a query is open, replaced rows stay resident and are reported as reclaimed by `system.tables.total_bytes` before their memory is actually returned.

`TRUNCATE`, `DROP`, and `ALTER ... DROP INDEX` release storage outright rather than superseding it, so those do wait for in-flight readers to finish.

The atomicity boundary is one input block received by the table-engine sink. A SQL `INSERT` can be divided into multiple input blocks by its pipeline; earlier blocks remain committed if a later block is rejected. Buffering a complete statement would make publication size unbounded and is intentionally not part of this in-memory engine's contract.

## Memory representation {#memory-representation}

Committed payloads are stored in immutable native column segments. A segment preserves compact column representations such as `LowCardinality` dictionaries. Rows retain only a segment reference and row offset; serialized primary and lookup keys used while preparing an insert are not retained in the row payload.

Segments are uncompressed by default. `compress_segments` enables ClickHouse's in-memory column compression, which lowers residency at a large cost per accessed row: reading one row of a compressed column decompresses the whole column, so it suits caches that are scanned by large lookups rather than probed one key at a time. The version and tie-break columns are never compressed, so winner selection compares values in place and never materializes a stored payload.

Lookup postings store compact numeric entry identifiers instead of owning row pointers. They use `UInt32` identifiers while the ID range permits and upgrade to `UInt64` when required. Replacing a winner does not change postings because every indexed column must belong to the immutable `KEYS` tuple. When replacements leave at least half of a segment dead, its remaining live rows are compacted into a new immutable segment. Each segment carries one entry identifier per stored row, so selecting the survivors costs one pass over that segment rather than a scan of the whole table. Fully dead segments are reclaimed after readers of the previous publication epoch have drained.

Entries live in a chunked array that never relocates, and the primary index and lookup postings are open-addressed hash tables keyed by arena-owned bytes. A reader therefore reaches a row without taking any table-wide lock, and a writer hashes each key once and locks each affected shard once per published block.

Writers are serialized, but a large replacement publication does not take an engine-wide exclusive reader lock. New primary keys can briefly contend with readers of the same primary or posting shard. Large lookup-index results can still require substantial traversal work; sharding does not make an unbounded result inexpensive.

## Lookup indexes {#lookup-indexes}

The complete `KEYS` tuple always has a primary lookup and must not be repeated in `INDEX`. Each `INDEX (...)` tuple creates one exact lookup family. A composite family does not implicitly create indexes for its individual columns.

When no exact composite family matches a query, `OverwriteCache` can intersect multiple declared families. Within one family, `IN` values are combined as a union; families constrained by `AND` are intersected smallest-first. For example, `INDEX (website_type), (tag)` supports predicates on either column and their intersection, while `INDEX (website_type, tag)` avoids that intersection for the hot composite path.

An index can be added to an existing table without exposing a partial backfill:

```sql
ALTER TABLE latest_state ADD INDEX (user_id);
ALTER TABLE latest_state DROP INDEX (user_id);
```

An added index is privately populated and becomes available to new readers only after atomic publication. If population or metadata persistence fails, the previous index catalog remains active. Dropping an index atomically publishes a catalog without that family; readers that already planned against the previous catalog retain its immutable index generation until they finish.

## Persistence {#persistence}

The unit of persistence is the immutable row segment. A publication builds exactly one segment, which is written to its own file, is never rewritten, and whose file is removed when the segment is released. Nothing else is written: the primary index, the lookup postings, the entry table and the version chains are all rebuilt when the table is loaded, because every indexed column must belong to the `KEYS` tuple and is therefore stored inside the segment itself. Adding or dropping a lookup index does not touch the data files.

A `manifest` file holds one record per publication, in publication order: the segments it added, the segments it retired, and the keys it deleted. Deletions have to be recorded because a segment that a `DELETE` leaves partially dead is not superseded by any later segment, so replaying that segment alone would bring the deleted keys back. `manifest` records are collapsed in the background once the bookkeeping grows well past the segments it describes.

Loading replays the log: it skips the added segments that a later record retired, then applies the surviving segments in publication order through ordinary winner selection. Every stored row was a winner when it was published, so the replayed state is the exact state that was lost, including the equal-version tie-break rule, which resolves in favour of the row already stored and therefore depends on insertion order. Because the segments in memory mirror the files one to one, the loaded table occupies exactly the memory it occupied before, and segment compaction is left alone during replay.

Loading happens while the table is being attached, so a table whose log cannot be replayed fails to attach rather than coming up as a silently empty cache. With `async_load_databases` enabled, which is the default, the table loads in the background after startup and a query that reaches it before it is ready waits for that load, exactly as it does for a `MergeTree` table.

The stored log records the columns, the `KEYS` tuple, the version column and the tie-break columns it was written for. Attaching a table whose definition no longer matches fails instead of reinterpreting the stored rows.

`persist_mode` selects when a publication reaches the disk:

- `async`, the default, queues the publication and makes it visible immediately. A crash can lose the tail of the log; the manifest records are framed and checksummed, so an incomplete tail is recognized and dropped rather than read as a record. This suits a cache that can be topped up from its upstream source, and re-delivering rows is harmless because winner selection ignores a row that ties on the version and the tie-break columns.
- `sync` returns from the `INSERT` only once its publication is durable. The wait happens after the writer lock is released, so a durable write never holds up another publication, but the throughput of the table is then bounded by the disk.

A clean shutdown drains the queue, so restarting the server loses nothing even in `async` mode. Publication does not outrun the disk without bound: once the queue holds enough payload, the next publication waits for the writer rather than letting the queue become a second copy of the table.

If persisting fails, the error is not swallowed - the table stops accepting publications and reports the failure, instead of continuing as an in-memory table whose disk copy has silently stopped advancing.

`BACKUP` copies the immutable segment files together with a self-contained `manifest`, and holds back segment retirement while it does, so a concurrent publication cannot delete a file the backup was told to copy. `RESTORE` replaces the table's files and replays them; it refuses a table that already holds rows unless `allow_non_empty_tables` is set. A table created with `persist_mode = 'none'` cannot be backed up, because it keeps nothing on disk.

## Settings {#settings}

- `max_memory_bytes` — Optional hard admission limit for engine-accounted memory. If omitted, allocations remain subject to the normal ClickHouse memory tracker and query limits.
- `equal_version_tiebreak_columns` — Optional comma-separated column names used to select a deterministic winner when versions are equal. These columns cannot be key or version columns.
- `compress_segments` — Optional flag, `0` by default. When set to `1`, payload columns are compressed in memory. This trades a large per-row read cost for lower residency; see [Memory representation](#memory-representation). Segment files are always compressed, so this setting does not change them, and a table can be recreated with a different value for it over the same data.
- `persist_mode` — `'async'` by default. One of `'async'`, `'sync'` or `'none'`; see [Persistence](#persistence).
- `disk` — Disk holding the segment files, `'default'` by default. Ignored when `persist_mode = 'none'`.

Lookup indexes are declared only through `INDEX (...)`. The legacy settings `max_index_probe_rows`, `max_index_result_rows`, `index_each_key_column`, `secondary_index_columns`, `secondary_index_segment_column`, and `max_secondary_index_rows` are not supported. Every lookup-index column must occur in `KEYS`, and duplicate tuples are rejected.

## Reading rows {#reading-rows}

Normal `SELECT` queries must contain either:

- equality or `IN` predicates that fully specify all columns in `KEYS (...)`; or
- equality or `IN` predicates covered by one or more declared `INDEX (...)` tuples.

Other predicates can be applied after an indexed lookup, but they cannot be the only access path. Queries that require an unrestricted or partial-key scan are rejected.

```sql
SELECT value
FROM latest_state
WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';

SELECT user_id, value
FROM latest_state
WHERE website_type = 1 AND tag = 'risk';
```

## Deleting rows {#deleting-rows}

`DELETE FROM` and `ALTER TABLE ... DELETE WHERE` remove rows by key. The rows to remove are resolved through the read path, so a delete accepts exactly the predicates a `SELECT` accepts — a complete `KEYS` tuple, or one or more declared `INDEX (...)` tuples — and a predicate that would need a scan is rejected with the same error. Further predicates can narrow an indexed delete, exactly as they narrow an indexed `SELECT`.

```sql
DELETE FROM latest_state WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';
DELETE FROM latest_state WHERE tag = 'risk';
DELETE FROM latest_state WHERE tag = 'risk' AND value = 'stale';
```

A delete is applied synchronously and is not a mutation left in the background: when the statement returns, the rows are no longer visible to new queries. It is published as one generation change, so a query never observes a partially applied delete, and, like an insert, it does not wait for in-flight readers. A query that captured an earlier generation keeps resolving the rows it already found.

A deleted key can be inserted again. Winner selection has nothing to compare against after a delete, so the next inserted row wins whatever its version is — a delete is not a version floor.

A delete publishes a tombstone rather than removing the key from the primary index and the lookup postings, because reclaiming those would require waiting for readers. The row payload is reclaimed: when every row of an immutable segment is gone the segment is released, and a segment that a delete leaves at least half dead is compacted just as a replacement compacts it. What a delete does not return is the key bytes in the primary index and the entry identifier in each lookup posting, so deleting and reinserting the same keys repeatedly leaves those growing. Only `TRUNCATE` and `DROP` release them.

Deleting a key that is absent, or already deleted, is a no-op. `UPDATE` is not supported: a stored row is replaced by inserting a row with a greater version.

## Scalar lookup functions {#scalar-lookup-functions}

`overwriteCacheGet` and `overwriteCacheGetOrNull` perform full composite-key lookups. The table name and returned column name must be constant strings. Key arguments follow the order in `KEYS (...)`.

```sql
SELECT overwriteCacheGetOrNull(
    'default.latest_state',
    'value',
    toUInt8(1),
    toUInt64(42),
    'risk'
);
```

`overwriteCacheGet` returns the column default for a missing key. `overwriteCacheGetOrNull` returns `NULL`.
