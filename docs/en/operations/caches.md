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
  
  Sample query using `mark_cache`:
  
  ```
  SELECT COUNT(*)
  FROM my_table
  WHERE date >= toDate('2022-01-01') AND date < toDate('2023-01-01')
  MARK CACHE 'my_cache';
  ```
   In this query, we are selecting the count of all rows in the `my_table` table where the date falls within the year 2022. The `MARK CACHE` keyword is used to mark the result of this query to be cached in ClickHouse's memory cache, with the name 'my_cache'. This can improve query performance if the same query is run multiple      times, as the results can be retrieved from the cache instead of re-executing the query on the underlying data.

- `uncompressed_cache` — Cache of uncompressed data used by table engines of the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family.
  
  Sample query using `uncompressed_cache`:
  
  ```
  SELECT COUNT(*)
  FROM my_table
  WHERE date >= '2022-01-01' AND date < '2022-02-01'
  SETTINGS storage_compression_codec = 'NONE',
          uncompressed_cache = 1
  ```
  This query counts the number of rows in `my_table` that fall within the specified date range, with the `UNCOMPRESSED_CACHE` setting enabled to cache the uncompressed    data in memory. The storage_compression_codec setting is also set to NONE to ensure that the data is not compressed before being cached. This can be useful in cases where the uncompressed data is frequently accessed and performance is critical.

- Operating system page cache (used indirectly, for files with actual data).

  The operating system page cache is a caching mechanism used by the operating system to temporarily store frequently accessed data in memory to reduce disk I/O operations. ClickHouse takes advantage of page cache to improve performance.

Additional cache types:

- DNS cache.
- [Regexp](../interfaces/formats.md#data-format-regexp) cache.
- Compiled expressions cache.
- [Avro format](../interfaces/formats.md#data-format-avro) schemas cache.
- [Dictionaries](../sql-reference/dictionaries/index.md) data cache.
- Schema inference cache.
- [Filesystem cache](storing-data.md) over S3, Azure, Local and other disks.
- [(Experimental) Query cache](query-cache.md).

To drop one of the caches, use [SYSTEM DROP ... CACHE](../sql-reference/statements/system.md#drop-mark-cache) statements.
