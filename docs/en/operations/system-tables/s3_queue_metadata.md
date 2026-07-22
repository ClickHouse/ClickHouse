---
description: 'Contains the current number of processed, processing and failed nodes in keeper for each S3Queue metadata object.'
keywords: ['system table', 's3_queue_metadata']
slug: /operations/system-tables/s3_queue_metadata
title: 'system.s3_queue_metadata'
doc_type: 'reference'
---

## Description {#description}

Contains the current number of `processed`, `processing` and `failed` nodes in keeper for each `S3Queue` metadata object, and, on demand, their contents. Unlike `system.s3queue_metadata_cache`, which shows the in-memory cache, this table reads the state directly from keeper.

The `processed`, `processing`, `failed` and `processed_path` map columns issue keeper requests to list the corresponding folders and are fetched only when the respective column is selected, so selecting only the `*_nodes` counters avoids the extra keeper traffic.

## Columns {#columns}

- `zookeeper_path` ([String](/sql-reference/data-types/string)) — Path in zookeeper to metadata
- `processed_nodes` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Number of nodes in the `processed` folder in keeper. Only set for `unordered` mode: in `ordered` mode there are no per-file processed nodes (see `processed_path` instead), so the value is NULL.
- `processing_nodes` ([UInt64](/sql-reference/data-types/int-uint)) — Number of nodes in the `processing` folder in keeper
- `failed_nodes` ([UInt64](/sql-reference/data-types/int-uint)) — Number of nodes in the `failed` folder in keeper
- `processed` ([Map(String, String)](/sql-reference/data-types/map)) — Contents (node name -> node data) of the `processed` folder in keeper. Only filled for `unordered` mode. Fetched only when this column is selected.
- `processing` ([Map(String, String)](/sql-reference/data-types/map)) — Contents (node name -> node data) of the `processing` folder in keeper. Fetched only when this column is selected.
- `failed` ([Map(String, String)](/sql-reference/data-types/map)) — Contents (node name -> node data) of the `failed` folder in keeper. Fetched only when this column is selected.
- `processed_path` ([Map(String, String)](/sql-reference/data-types/map)) — Last processed path per processed pointer in keeper (relative pointer path -> last processed file path). Only filled for `ordered` mode, where it covers the single, per-bucket and per-partition pointers. Fetched only when this column is selected.

**See also**

- [S3Queue table engine](/engines/table-engines/integrations/s3queue)
- [system.s3queue_metadata_cache](/operations/system-tables/s3queue_metadata_cache)
- [system.s3queue_log](/operations/system-tables/s3queue_log)
