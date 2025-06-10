---
description: 'System table containing information about codecs
  in queue.'
keywords: ['system table', 'codecs', 'compression']
slug: /operations/system-tables/codecs
title: 'system.codecs'
---

Contains information about compression and encryption codecs.

You can use this sable to get information about the available compression and encryption codecs

The `system.codecs` table contains the following columns (the column type is shown in brackets):

- `codec` ([String](../../sql-reference/data-types/string.md)) — Codec name.
- `method_byte` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — Byte which indicates codec in compressed file.
- `compression` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is a compression.
- `generic_compression` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is a generic compression.
- `encription` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec encrypts.
- `floating_point_timeseries`([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is for floating point timeseries codec.
- `delta_compression` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is for delta compression.
- `experimental` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — The codec is experimental.
- `notes` ([String](../../sql-reference/data-types/string.md)) — Performance notes.

**Example**

Query:

```sql
SELECT * FROM system.codecs WHERE codec='LZ4'
```

Result:

```text
Row 1:
──────
codec:                     LZ4
method_byte:               130
compression:               1
generic_compression:       1
encription:                0
floating_point_timeseries: 0
delta_compression:         0
experimental:              0
notes:                     Extremely fast; good compression; balanced speed and efficiency.
```
