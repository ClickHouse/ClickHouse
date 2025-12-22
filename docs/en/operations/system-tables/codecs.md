---
description: 'System table containing information about codecs
  in queue.'
keywords: ['system table', 'codecs', 'compression']
slug: /operations/system-tables/codecs
title: 'system.codecs'
---

Contains information about compression and encryption codecs.

You can use this table to get information about the available compression and encryption codecs

The `system.codecs` table contains the following columns (the column type is shown in brackets):

- `name` ([String](../../sql-reference/data-types/string.md)) — Codec name.
- `method_byte` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — Byte which indicates codec in compressed file.
- `is_compression` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — True if this codec compresses something. Otherwise it can be just a transformation that helps compression.
- `is_generic_compression` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is a generic compression algorithm like lz4, zstd.
- `is_encryption` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec encrypts the data.
- `is_timeseries_codec`([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is for floating-point time series data.
- `is_experimental` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is experimental.
- `description` ([String](../../sql-reference/data-types/string.md)) — A high-level description of the codec.

**Example**

Query:

```sql
SELECT * FROM system.codecs WHERE name='LZ4'
```

Result:

```text
Row 1:
──────
name:                   LZ4
method_byte:            130
is_compression:         1
is_generic_compression: 1
is_encryption:          0
is_timeseries_codec:    0
is_experimental:        0
description:            Extremely fast; good compression; balanced speed and efficiency.
```
