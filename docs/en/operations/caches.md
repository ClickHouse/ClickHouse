---
description: 'When performing queries, ClickHouse uses different caches.'
sidebar_label: 'Caches'
sidebar_position: 65
slug: /operations/caches
title: 'Cache types'
keywords: ['cache']
doc_type: 'reference'
---

# Cache types

When performing queries, ClickHouse uses different caches to speed up queries
and reduce the need to read from or write to disk.

The main cache types are:

- `mark_cache` — Cache of [marks](/development/architecture#merge-tree) used by table engines of the [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) family.
- `uncompressed_cache` — Cache of uncompressed data used by table engines of the [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) family.
- Operating system page cache (used indirectly, for files with actual data).

There are also a host of additional cache types:

- DNS cache.
- [Regexp](/interfaces/formats/Regexp) cache.
- Compiled expressions cache.
- [Vector similarity index](../engines/table-engines/mergetree-family/annindexes.md) cache.
- [Text index](../engines/table-engines/mergetree-family/textindexes.md#caching) cache.
- [Avro format](/interfaces/formats/Avro) schemas cache.
- [Dictionaries](../sql-reference/statements/create/dictionary/index.md) data cache.
- Schema inference cache.
- [Filesystem cache](storing-data.md) over S3, Azure, Local and other disks.
- [Userspace page cache](/operations/userspace-page-cache)
- [Query cache](query-cache.md).
- [Query condition cache](query-condition-cache.md).
- Format schema cache.

Should you wish to clear one of the caches, for performance tuning, troubleshooting, or data consistency reasons,
you can use the [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md) statement.
