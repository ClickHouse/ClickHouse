---
description: 'When performing queries, ClickHouse uses different caches.'
sidebar_label: 'Caches'
sidebar_position: 65
slug: /operations/caches
title: 'Cache Types'
---

# Cache Types

When performing queries, ClickHouse uses different caches.

Main cache types:

- `mark_cache` — Cache of marks used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.
- `uncompressed_cache` — Cache of uncompressed data used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.
- Operating system page cache (used indirectly, for files with actual data).

Additional cache types:

- DNS cache.
- [Regexp](../interfaces/formats.md#data-format-regexp) cache.
- Compiled expressions cache.
- [Vector Similarity Index](../engines/table-engines/mergetree-family/annindexes.md) cache.
- [Avro format](../interfaces/formats.md#data-format-avro) schemas cache.
- [Dictionaries](../sql-reference/dictionaries/index.md) data cache.
- Schema inference cache.
- [Filesystem cache](storing-data.md) over S3, Azure, Local and other disks.
- [Userspace page cache](/operations/userspace-page-cache)
- [Query cache](query-cache.md).
- [Query condition cache](query-condition-cache.md).
- Format schema cache.

To drop one of the caches, use [SYSTEM DROP ... CACHE](../sql-reference/statements/system.md) statements.
