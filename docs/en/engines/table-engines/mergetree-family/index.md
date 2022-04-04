---
toc_folder_title: MergeTree Family
toc_priority: 28
toc_title: Introduction
---

# MergeTree Engine Family {#mergetree-engine-family}

Table engines from the MergeTree family are the core of ClickHouse data storage capabilities. They provide most features for resilience and high-performance data retrieval: columnar storage, custom partitioning, sparse primary index, secondary data-skipping indexes, etc.

Base [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) table engine can be considered the default table engine for single-node ClickHouse instances because it is versatile and practical for a wide range of use cases.

For production usage [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) is the way to go, because it adds high-availability to all features of regular MergeTree engine. A bonus is automatic data deduplication on data ingestion, so the software can safely retry if there was some network issue during insert.

All other engines of MergeTree family add extra functionality for some specific use cases. Usually, it’s implemented as additional data manipulation in background.

The main downside of MergeTree engines is that they are rather heavy-weight. So the typical pattern is to have not so many of them. If you need many small tables, for example for temporary data, consider [Log engine family](../../../engines/table-engines/log-family/index.md).
