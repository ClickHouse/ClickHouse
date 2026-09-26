---
title: 'Handling TOAST columns'
description: 'Learn how to handle TOAST columns when replicating data from PostgreSQL to ClickHouse.'
slug: /integrations/clickpipes/postgres/toast
doc_type: 'guide'
keywords: ['clickpipes', 'postgresql', 'cdc', 'data ingestion', 'real-time sync']
integration:
   - support_level: 'core'
   - category: 'clickpipes'
---

When replicating data from PostgreSQL to ClickHouse, it's important to understand the limitations and special considerations for TOAST (The Oversized-Attribute Storage Technique) columns. This guide will help you identify and properly handle TOAST columns in your replication process.

## What are TOAST columns in PostgreSQL? {#what-are-toast-columns-in-postgresql}

TOAST (The Oversized-Attribute Storage Technique) is PostgreSQL's mechanism for handling large field values. When a row exceeds the maximum row size (typically 2KB, but this can vary depending on the PostgreSQL version and exact settings), PostgreSQL automatically moves large field values into a separate TOAST table, storing only a pointer in the main table.

It's important to note that during Change Data Capture (CDC), unchanged TOAST columns aren't included in the replication stream. This can lead to incomplete data replication if not handled properly.

During the initial load (snapshot), all column values, including TOAST columns, will be replicated correctly regardless of their size. The limitations described in this guide primarily affect the ongoing CDC process after the initial load.

You can read more about TOAST and its implementation in PostgreSQL here: https://www.postgresql.org/docs/current/storage-toast.html

## Identifying TOAST columns in a table {#identifying-toast-columns-in-a-table}

To identify if a table has TOAST columns, you can use the following SQL query:

```sql
SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
WHERE c.relname = 'your_table_name'
  AND a.attlen = -1
  AND a.attstorage != 'p'
  AND a.attnum > 0;
```

This query will return the names and data types of columns that could potentially be TOASTed. However, it's important to note that this query only identifies columns that are eligible for TOAST storage based on their data type and storage attributes. To determine if these columns actually contain TOASTed data, you'll need to consider whether the values in these columns exceed the size. The actual TOASTing of data depends on the specific content stored in these columns.

## Ensuring proper handling of TOAST columns {#ensuring-proper-handling-of-toast-columns}

To ensure that TOAST columns are handled correctly during replication, you should set the `REPLICA IDENTITY` of the table to `FULL`. This tells PostgreSQL to include the full old row in the WAL for UPDATE and DELETE operations, ensuring that all column values (including TOAST columns) are available for replication.

You can set the `REPLICA IDENTITY` to `FULL` using the following SQL command:

```sql
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
```

Refer to [this blog post](https://xata.io/blog/replica-identity-full-performance) for performance considerations when setting `REPLICA IDENTITY FULL`.

## Replication behavior when REPLICA IDENTITY FULL isn't set {#replication-behavior-when-replica-identity-full-is-not-set}

If `REPLICA IDENTITY FULL` isn't set for a table with TOAST columns, you may encounter the following issues when replicating to ClickHouse:

1. For INSERT operations, all columns (including TOAST columns) will be replicated correctly.

2. For UPDATE operations:
   - If a TOAST column isn't modified, its value will appear as NULL or empty in ClickHouse.
   - If a TOAST column is modified, it will be replicated correctly.

3. For DELETE operations, TOAST column values will appear as NULL or empty in ClickHouse.

These behaviors can lead to data inconsistencies between your PostgreSQL source and ClickHouse destination. Therefore, it's crucial to set `REPLICA IDENTITY FULL` for tables with TOAST columns to ensure accurate and complete data replication.

## Conclusion {#conclusion}

Properly handling TOAST columns is essential for maintaining data integrity when replicating from PostgreSQL to ClickHouse. By identifying TOAST columns and setting the appropriate `REPLICA IDENTITY`, you can ensure that your data is replicated accurately and completely.
