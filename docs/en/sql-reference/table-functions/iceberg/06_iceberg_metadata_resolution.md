---
description: 'Provides a read-only table-like interface to Apache Iceberg tables in Amazon S3, Azure, HDFS or local storage.'
sidebar_label: 'Metadata resolution'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg-metadata-resolution
title: 'iceberg'
doc_type: 'reference'
---

## Metadata File Resolution {#metadata-file-resolution}

When using the `iceberg` table function in ClickHouse, the system needs to locate the correct metadata.json file that describes the Iceberg table structure. Here's how this resolution process works:

### Candidate Search (in Priority Order) {#candidate-search}

1. **Direct Path Specification**:
   *If you set `iceberg_metadata_file_path`, the system will use this exact path by combining it with the Iceberg table directory path.
* When this setting is provided, all other resolution settings are ignored.

2. **Table UUID Matching**:
   *If `iceberg_metadata_table_uuid` is specified, the system will:
   *Look only at `.metadata.json` files in the `metadata` directory
   *Filter for files containing a `table-uuid` field matching your specified UUID (case-insensitive)

3. **Default Search**:
   *If neither of the above settings are provided, all `.metadata.json` files in the `metadata` directory become candidates

### Selecting the Most Recent File {#most-recent-file}

After identifying candidate files using the above rules, the system determines which one is the most recent:

* If `iceberg_recent_metadata_file_by_last_updated_ms_field` is enabled:
* The file with the largest `last-updated-ms` value is selected

* Otherwise:
* The file with the highest version number is selected
* (Version appears as `V` in filenames formatted as `V.metadata.json` or `V-uuid.metadata.json`)

**Note**: All mentioned settings are table function settings (not global or query-level settings) and must be specified as shown below:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

:::note
While Iceberg Catalogs typically handle metadata resolution, the `iceberg` table function in ClickHouse directly interprets files stored in S3 as Iceberg tables, which is why understanding these resolution rules is important.
:::

## Metadata cache {#metadata-cache}

`Iceberg` table engine and table function support metadata cache storing the information of manifest files, manifest list and metadata json. The cache is stored in memory. This feature is controlled by setting `use_iceberg_metadata_files_cache`, which is enabled by default.