---
title: Does ClickHouse support multi-region replication?
toc_hidden: true
toc_priority: 30
---

# Does ClickHouse support multi-region replication? {#does-clickhouse-support-multi-region-replication}

The short answer is "yes". However, we recommend keeping latency between all regions/datacenters in two-digit range, otherwise write performance will suffer as it goes through distributed consensus protocol. For example, replication between US coasts will likely work fine, but between the US and Europe won't.

Configuration-wise there's no difference compared to single-region replication, simply use hosts that are located in different locations for replicas.

For more information, see [full article on data replication](../../engines/table-engines/mergetree-family/replication.md).
