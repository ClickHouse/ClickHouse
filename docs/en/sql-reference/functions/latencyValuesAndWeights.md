---
description: 'Extracts values and weights from a serialized Quantiles sketch as JSON'
slug: /sql-reference/functions/latencyvaluesandweights
title: 'latencyValuesAndWeights'
---

# latencyValuesAndWeights

Extracts the internal values and weights from a serialized Apache DataSketches Quantiles sketch and returns them as a JSON-formatted string. This format is optimized for downstream consumption by systems that need raw sketch data.

## Syntax

```sql
latencyValuesAndWeights(sketch)
```

## Arguments

- `sketch` — Serialized Quantiles sketch. Type: [String](../../sql-reference/data-types/string).

## Returned Value

- JSON-formatted string containing value-weight pairs from the sketch. Type: [String](../../sql-reference/data-types/string).
- Returns empty JSON object if the sketch is empty.

## Examples

### Example 1: Extract Sketch Data

```sql
WITH sketch AS (
    SELECT serializedQuantiles(response_time_ms) AS quantiles
    FROM requests
    WHERE service = 'api'
)
SELECT latencyValuesAndWeights(quantiles) AS sketch_data
FROM sketch;
```

```response
┌─sketch_data─────────────────────────────────────┐
│ {"values":[10.5,20.3,...],"weights":[1,2,...]} │
└─────────────────────────────────────────────────┘
```

### Example 2: Export for External Analysis

```sql
SELECT 
    service,
    date,
    latencyValuesAndWeights(daily_sketch) AS sketch_json
FROM daily_latency_sketches
WHERE date >= today() - 7
ORDER BY service, date;
```

### Example 3: Parse and Analyze

```sql
WITH 
    sketch AS (
        SELECT serializedQuantiles(number) AS s
        FROM numbers(100)
    ),
    json_data AS (
        SELECT latencyValuesAndWeights(s) AS data
        FROM sketch
    )
SELECT 
    JSONExtractArrayRaw(data, 'values') AS values_array,
    JSONExtractArrayRaw(data, 'weights') AS weights_array
FROM json_data;
```

## Implementation Details

The JSON format contains:
- `values`: Array of sample values from the sketch
- `weights`: Corresponding weights for each value

This function is useful for:
- Exporting sketch data to external systems
- Custom analysis requiring raw sketch values
- Integration with non-ClickHouse tools
- Debugging and visualization

For standard percentile queries, use `percentileFromQuantiles` instead.

## See Also

- [serializedQuantiles](../../sql-reference/aggregate-functions/reference/serializedquantiles) — Create Quantiles sketch
- [percentileFromQuantiles](../../sql-reference/functions/percentilefromquantiles) — Extract percentile from sketch
- [mergeSerializedQuantiles](../../sql-reference/aggregate-functions/reference/mergeserializedquantiles) — Merge Quantiles sketches
