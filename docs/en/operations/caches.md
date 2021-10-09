---
toc_priority: 65
toc_title: Caches
---

# Cache Types {#cache-types}

When performing queries, ClichHouse uses different caches.

Main cache types:

- `mark_cache` — Cache of marks used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.
- `uncompressed_cache` — Cache of uncompressed data used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.

Additional cache types:

- DNS cache.
- [Regexp](../interfaces/formats.md#data-format-regexp) cache.
- Compiled expressions cache.
- [Avro format](../interfaces/formats.md#data-format-avro) schemas cache.
- [Dictionaries](../sql-reference/dictionaries/index.md) data cache.

Indirectly used:

- OS page cache.

To drop cache, use [SYSTEM DROP ... CACHE](../sql-reference/statements/system.md) statements.

[Original article](https://clickhouse.com/docs/en/operations/caches/) <!--hide-->
