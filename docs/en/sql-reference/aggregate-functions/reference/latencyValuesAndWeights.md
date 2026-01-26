---
description: 'Creates a Quantiles DoubleSketch and returns values/weights pairs for latency analysis'
slug: /sql-reference/aggregate-functions/reference/latencyValuesAndWeights
title: 'latencyValuesAndWeights'
doc_type: 'reference'
---

# latencyValuesAndWeights

Creates an Apache DataSketches Quantiles DoubleSketch and returns the internal values and weights as a JSON-formatted string. This format is optimized for latency metrics and downstream consumption by systems that need the raw sketch data.

## Syntax

```sql
latencyValuesAndWeights(expression)
```

## Arguments

- `expression` — Numeric expression representing latency values. Supported types: [Int](/docs/en/sql-reference/data-types/int-uint.md), [UInt](/docs/en/sql-reference/data-types/int-uint.md), [Float](/docs/en/sql-reference/data-types/float.md).

## Returned Value

- JSON-formatted string containing value-weight pairs from the sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Examples

```sql
SELECT latencyValuesAndWeights(response_time_ms) AS latency_data
FROM requests
WHERE service = 'api';
```

## See Also

- [serializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeddoublesketch) — Standard quantiles sketch format
