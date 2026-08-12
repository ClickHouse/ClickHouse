---
description: 'Documentation for the Puffin format'
input_format: true
output_format: false
keywords: ['Puffin']
sidebar_label: 'Puffin'
sidebar_position: 1
slug: /interfaces/formats/Puffin
title: 'Puffin'
doc_type: 'reference'
---

## Description {#description}

Input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) files.

The format exposes deleted row positions from `deletion-vector-v1` blobs. Other blob types (for example `apache-datasketches-theta-v1`) are skipped.
If a puffin file contains multiple `deletion-vector-v1` blobs, the format outputs one row per such blob.

Fixed output columns:
- `referenced_data_file` (`String`) - location of the data file the deletion vector applies to (`referenced-data-file` blob property)
- `deleted_rows` (`Array(UInt64)`) - 64-bit row positions deleted according to the deletion vector roaring bitmap

Deletion vectors whose declared `cardinality` exceeds an absolute materialization ceiling are rejected when `deleted_rows` is requested. Footer `deletion-vector-v1` properties (including that `cardinality` parses as an unsigned integer) are always validated. Selecting only `referenced_data_file` skips on-disk payload I/O and therefore also skips envelope, CRC, roaring deserialize, and the materialization ceiling — intentionally, so a path-only projection does not read up to the blob-size cap.

On-disk `deletion-vector-v1` blob length is bounded by an absolute ceiling (aligned with Iceberg's 2 GiB content-size check). When `deleted_rows` is requested, the reader peeks the envelope header (combined length and magic) before allocating the full payload; CRC is verified after the bounded read.

LZ4-compressed and uncompressed puffin footers are supported. Footer payload size (and declared LZ4 content size) is bounded by a compression ratio where applicable and an absolute ceiling; oversized footers are rejected before allocation.

Only a subset of output columns can be requested. A user-provided structure with unexpected column names or types is rejected when the format is created.

## Example usage {#example-usage}

Read deleted row positions with the referenced data file:

```sql
SELECT referenced_data_file, deleted_rows
FROM file(deletes.puffin, Puffin);
```

Expand deleted positions into individual rows:

```sql
SELECT referenced_data_file, row_number
FROM file(deletes.puffin, Puffin)
ARRAY JOIN deleted_rows AS row_number
ORDER BY referenced_data_file, row_number;
```

Use `PuffinMetadata` to inspect footer blob descriptors before reading deletion vectors.
