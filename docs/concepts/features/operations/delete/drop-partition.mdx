---
slug: /managing-data/drop_partition
sidebarTitle: 'Drop partition'
title: 'Dropping partitions'
hide_title: false
description: 'Page describing drop partitions'
keywords: ['DROP PARTITION', 'drop partition', 'delete partition', 'remove partition', 'partition management']
doc_type: 'reference'
---

## Background {#background}
Partitioning is specified on a table when it is initially defined via the `PARTITION BY` clause. This clause can contain a SQL expression on any columns, the results of which will define which partition a row is sent to.

The data parts are logically associated with each partition on disk and can be queried in isolation. For the example below, we partition the `posts` table by year using the expression `toYear(CreationDate)`. As rows are inserted into ClickHouse, this expression will be evaluated against each row and routed to the resulting partition if it exists (if the row is the first for a year, the partition will be created).

```sql
 CREATE TABLE posts
(
        `Id` Int32 CODEC(Delta(4), ZSTD(1)),
        `PostTypeId` Enum8('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8),
        `AcceptedAnswerId` UInt32,
        `CreationDate` DateTime64(3, 'UTC'),
...
        `ClosedDate` DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (PostTypeId, toDate(CreationDate), CreationDate)
PARTITION BY toYear(CreationDate)
```

Read about setting the partition expression in a section [How to set the partition expression](/reference/statements/alter/partition#how-to-set-partition-expression).

In ClickHouse, users should principally consider partitioning to be a data management feature, not a query optimization technique. By separating data logically based on a key, each partition can be operated on independently e.g. deleted. This allows users to move partitions, and thus subsets, between [storage tiers](/integrations/connectors/data-ingestion/AWS/integrating-s3-with-clickhouse#storage-tiers) efficiently on time or [expire data/efficiently delete from the cluster](/reference/statements/alter/partition).

## Drop partitions {#drop-partitions}
`ALTER TABLE ... DROP PARTITION` provides a cost-efficient way to drop a whole partition.

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

This query tags the partition as inactive and deletes data completely, approximately in 10 minutes. The query is replicated – it deletes data on all replicas.

In example, below we remove posts from 2008 for the earlier table by dropping the associated partition.

```sql
SELECT DISTINCT partition
FROM system.parts
WHERE `table` = 'posts'
```

```response
┌─partition─┐
│ 2008      │
│ 2009      │
│ 2010      │
│ 2011      │
│ 2012      │
│ 2013      │
│ 2014      │
│ 2015      │
│ 2016      │
│ 2017      │
│ 2018      │
│ 2019      │
│ 2020      │
│ 2021      │
│ 2022      │
│ 2023      │
│ 2024      │
└───────────┘

17 rows in set. Elapsed: 0.002 sec.
```

```sql
ALTER TABLE posts
(DROP PARTITION '2008')
```

```response
0 rows in set. Elapsed: 0.103 sec.
```
