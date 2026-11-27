---
title: 'Storage efficiency - time-series'
sidebarTitle: 'Storage efficiency'
description: 'Improving time-series storage efficiency'
slug: /use-cases/time-series/storage-efficiency
keywords: ['time-series', 'storage efficiency', 'compression', 'data retention', 'TTL', 'storage optimization', 'disk usage']
show_related_blogs: true
doc_type: 'guide'
---

After exploring how to query our Wikipedia statistics dataset, let's focus on optimizing its storage efficiency in ClickHouse.
This section demonstrates practical techniques to reduce storage requirements while maintaining query performance.

## Type optimization {#time-series-type-optimization}

The general approach to optimizing storage efficiency is using optimal data types.
Let's take the `project` and `subproject` columns. These columns are of type String, but have a relatively small amount of unique values:

```sql
SELECT
    uniq(project),
    uniq(subproject)
FROM wikistat;
```

```text
┌─uniq(project)─┬─uniq(subproject)─┐
│          1332 │              130 │
└───────────────┴──────────────────┘
```

This means we can use the LowCardinality() data type, which uses dictionary-based encoding. This causes ClickHouse to store the internal value ID instead of the original string value, which in turn saves a lot of space:

```sql
ALTER TABLE wikistat
MODIFY COLUMN `project` LowCardinality(String),
MODIFY COLUMN `subproject` LowCardinality(String)
```

We've also used a UInt64 type for the `hits` column, which takes 8 bytes, but has a relatively small max value:

```sql
SELECT max(hits)
FROM wikistat;
```

```text
┌─max(hits)─┐
│    449017 │
└───────────┘
```

Given this value, we can use UInt32 instead, which takes only 4 bytes, and allows us to store up to ~4b as a max value:

```sql
ALTER TABLE wikistat
MODIFY COLUMN `hits` UInt32;
```

This will reduce the size of this column in memory by at least a factor of two. Note that the size on disk will remain unchanged due to compression. But be careful, pick data types that aren't too small!

## Specialized codecs {#time-series-specialized-codecs}

When we deal with sequential data, like time-series, we can further improve storage efficiency by using special codecs.
The general idea is to store changes between values instead of absolute values themselves, which results in much less space needed when dealing with slowly changing data:

```sql
ALTER TABLE wikistat
MODIFY COLUMN `time` CODEC(Delta, ZSTD);
```

We've used the Delta codec for the `time` column, which is a good fit for time-series data.

The right ordering key can also save disk space.
Since we usually want to filter by a path, we will add `path` to the sorting key.
This requires recreation of the table.

Below we can see the `CREATE` command for our initial table and the optimized table:

```sql
CREATE TABLE wikistat
(
    `time` DateTime,
    `project` String,
    `subproject` String,
    `path` String,
    `hits` UInt64
)
ENGINE = MergeTree
ORDER BY (time);
```

```sql
CREATE TABLE optimized_wikistat
(
    `time` DateTime CODEC(Delta(4), ZSTD(1)),
    `project` LowCardinality(String),
    `subproject` LowCardinality(String),
    `path` String,
    `hits` UInt32
)
ENGINE = MergeTree
ORDER BY (path, time);
```

And let's have a look at the amount of space taken up by the data in each table:

```sql
SELECT
    table,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    count() AS parts
FROM system.parts
WHERE table LIKE '%wikistat%'
GROUP BY ALL;
```

```text
┌─table──────────────┬─uncompressed─┬─compressed─┬─parts─┐
│ wikistat           │ 35.28 GiB    │ 12.03 GiB  │     1 │
│ optimized_wikistat │ 30.31 GiB    │ 2.84 GiB   │     1 │
└────────────────────┴──────────────┴────────────┴───────┘
```

The optimized table takes up just over 4 times less space in its compressed form.
