---
description: 'Merges multiple serialized Quantiles DoubleSketches for distributed percentile estimation'
slug: /sql-reference/aggregate-functions/reference/mergeSerializedDoubleSketch
title: 'mergeSerializedDoubleSketch'
doc_type: 'reference'
---

# mergeSerializedDoubleSketch

Merges multiple serialized Apache DataSketches Quantiles DoubleSketches that were created by [serializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeddoublesketch) into a single unified sketch. This enables distributed percentile estimation where sketches are computed on different nodes or time periods and then merged together.

## Syntax

```sql
mergeSerializedDoubleSketch([assume_raw_binary])(sketch_column)
```

## Arguments

- `sketch_column` — Column containing serialized Quantiles DoubleSketches. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Parameters (optional)

- `assume_raw_binary` — Performance optimization flag. Type: [Bool](/docs/en/sql-reference/data-types/boolean.md). Default: 1 (true).
  - `1` (true): Assumes input is raw binary data, skips base64 detection (~95% faster)
  - `0` (false): Checks for base64 encoding and decodes if detected

## Returned Value

- Merged serialized Quantiles DoubleSketch. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Examples

```sql
-- Merge hourly latency sketches to get daily percentiles
SELECT 
    toStartOfDay(hour) AS day,
    percentileFromDoubleSketch(
        mergeSerializedDoubleSketch(latency_sketch), 
        0.95
    ) AS daily_p95_latency
FROM hourly_latency_sketches
GROUP BY day;
```

## See Also

- [serializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeddoublesketch) — Create quantiles sketches
- [percentileFromDoubleSketch](/docs/en/sql-reference/functions/percentilefromdoublesketch) — Extract percentile from merged sketch
