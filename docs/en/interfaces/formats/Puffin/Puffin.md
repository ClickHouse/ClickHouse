---
description: 'Documentation for the Puffin format'
keywords: ['Puffin']
slug: /interfaces/formats/Puffin
title: 'Puffin'
doc_type: 'reference'
---

## Description {#description}

Input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) files.

The format exposes deleted row positions from `deletion-vector-v1` blobs. Other blob types (for example `apache-datasketches-theta-v1`) are skipped.
If a puffin file contains multiple `deletion-vector-v1` blobs, the format outputs one row per such blob.

Fixed output column:
- `deleted_rows` (`Array(UInt64)`) - 64-bit row positions deleted according to the deletion vector roaring bitmap

Only a subset of output columns can be requested. A user-provided structure with unexpected column names or types is rejected when the format is created.

## Example usage {#example-usage}

Read deleted row positions:

```sql
SELECT deleted_rows
FROM file(deletes.puffin, Puffin);
```

Expand deleted positions into individual rows:

```sql
SELECT row_number
FROM file(deletes.puffin, Puffin)
ARRAY JOIN deleted_rows AS row_number
ORDER BY row_number;
```

Use `PuffinMetadata` to inspect footer blob descriptors before reading deletion vectors.
