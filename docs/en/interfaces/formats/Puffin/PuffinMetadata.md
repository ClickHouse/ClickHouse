---
description: 'Documentation for the PuffinMetadata format'
keywords: ['PuffinMetadata']
slug: /interfaces/formats/PuffinMetadata
title: 'PuffinMetadata'
doc_type: 'reference'
---

## Description {#description}

Special input format for reading [Apache Iceberg Puffin](https://iceberg.apache.org/puffin-spec/) file footer metadata.
It outputs one row per blob entry from the footer `BlobMetadata` list.

Fixed output columns:
- `blob_type` (`String`) - blob type, for example `deletion-vector-v1`
- `snapshot_id` (`Int64`) - snapshot id of the blob
- `sequence_number` (`Int64`) - sequence number of the blob
- `fields` (`Array(Int32)`) - sorted list of field ids the blob applies to
- `offset` (`Int64`) - offset of the blob payload in the file
- `length` (`Int64`) - length of the blob payload in bytes
- `compression_codec` (`String`) - compression codec of the blob payload, if present
- `properties` (`Map(String, String)`) - blob-specific properties

Optional top-level `FileMetadata.properties` in the footer (for example `created-by`) are type-checked when present but are not returned as columns.

LZ4-compressed puffin footers are supported. Declared decompressed footer size is bounded by a compression ratio and an absolute ceiling; oversized frames are rejected before allocation.

## Example usage {#example-usage}

Inspect footer blobs:

```sql
SELECT blob_type, snapshot_id, sequence_number, offset, length, compression_codec,
       mapKeys(properties), mapValues(properties)
FROM file(deletes.puffin, PuffinMetadata);
```

Pair with the `Puffin` format to read `deletion-vector-v1` blob payloads.
