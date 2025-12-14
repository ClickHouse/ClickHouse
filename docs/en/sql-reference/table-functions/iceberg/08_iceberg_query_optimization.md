---
description: 'Iceberg query optimization topics such as partition pruning and processing of tables with deleted rows'
sidebar_label: 'Query optimization'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg-query-optimization
title: 'Iceberg query optimization topics'
doc_type: 'reference'
---

## Partition pruning {#partition-pruning}

ClickHouse supports partition pruning during SELECT queries for Iceberg tables, which helps optimize query performance by skipping irrelevant data files.
To enable partition pruning, set `use_iceberg_partition_pruning = 1`.
For more information about iceberg partition pruning address https://iceberg.apache.org/spec/#partitioning
