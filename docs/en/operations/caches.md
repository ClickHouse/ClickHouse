---
slug: /en/operations/caches
sidebar_position: 65
sidebar_label: Caches
title: "Cache Types"
description: When performing queries, ClickHouse uses different caches.
---

When performing queries, ClickHouse uses different caches.

Main cache types:

- `mark_cache` — Cache of marks used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.
- `uncompressed_cache` — Cache of uncompressed data used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.
- Operating system page cache (used indirectly, for files with actual data).

Additional cache types:

- DNS cache.
- [Regexp](../interfaces/formats.md#data-format-regexp) cache.
- Compiled expressions cache.
- [Avro format](../interfaces/formats.md#data-format-avro) schemas cache.
- [Dictionaries](../sql-reference/dictionaries/index.md) data cache.
- Schema inference cache.
- [Filesystem cache](storing-data.md) over S3, Azure, Local and other disks.
- [Query cache](query-cache.md).

To drop one of the caches, use [SYSTEM DROP ... CACHE](../sql-reference/statements/system.md#drop-mark-cache) statements.
