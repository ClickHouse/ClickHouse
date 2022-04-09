---
sidebar_label: Rebalancing Shards
sidebar_position: 20
description: ClickHouse does not support automatic shard rebalancing, so we provide some best practices for how to rebalance shards.
---

# Rebalancing Data

ClickHouse does not support automatic shard rebalancing. However, there are ways to rebalance shards in order of preference:

1. Adjust the shard for the [distributed table](../../en/engines/table-engines/special/distributed/), allowing writes to be biased to the new shard. This potentially will cause load imbalances and hot spots on the cluster but can be viable in most scenarios where write throughput is not extremely high. It does not require the user to change their write target i.e. It can remain as the distributed table. This does not assist with rebalancing existing data.

2. As an alternative to (1), modify the existing cluster and write exclusively to the new shard until the cluster is balanced - manually weighting writes. This has the same limitations as (1).

3. If you need to rebalance existing data and you have partitioned your data, consider detaching partitions and manually relocating them to another node before reattaching to the new shard. This is more manual than subsequent techniques but may be faster and less resource-intensive. This is a manual operation and thus needs to consider the rebalancing of the data.

4. Create a new cluster with the new topology and copy the data using [ClickHouse Copier](../../en/operations/utilities/clickhouse-copier.md).  Alternatively, create a new database within the existing cluster and migrate the data using ClickHouse Copier. This can be potentially computationally expensive and may impact your production environment. Building a new cluster on separate hardware, and applying this technique, is an option to mitigate this at the expense of cost.

5. Export the data from the source cluster to the new cluster via an[ INSERT FROM SELECT](../../en/sql-reference/statements/insert-into/#insert_query_insert-select). This will not be performant on very large datasets and will potentially incur significant IO on the source cluster and use considerable network resources. This represents a last resort.

There is an internal effort to reconsider how rebalancing could be implemented. There is some relevant discussion [here](https://github.com/ClickHouse/ClickHouse/issues/13574).
