---
description: 'Extracts centroids (mean and weight pairs) from a serialized TDigest sketch'
slug: /sql-reference/functions/centroidsfromtdigest
title: 'centroidsFromTDigest'
---

# centroidsFromTDigest

Extracts the internal centroids from a serialized Apache DataSketches TDigest sketch. Each centroid represents a cluster of values with a mean and weight.

## Syntax

```sql
centroidsFromTDigest(sketch)
```

## Arguments

- `sketch` — Serialized TDigest sketch. Type: [String](../../sql-reference/data-types/string).

## Returned Value

- JSON-formatted String containing centroid mean-to-weight pairs. Type: [String](../../sql-reference/data-types/string).
- Returns `{}` if the sketch is empty or invalid.

## Examples

### Example 1: Inspect TDigest Structure

```sql
WITH sketch AS (
    SELECT serializedTDigest(number) AS tdigest
    FROM numbers(1000)
)
SELECT 
    centroidsFromTDigest(tdigest) AS centroids_json
FROM sketch;
```

### Example 2: Analyze Centroid Distribution

```sql
WITH sketch AS (
    SELECT serializedTDigest(response_time_ms) AS tdigest
    FROM requests
    WHERE service = 'api'
)
SELECT 
    centroidsFromTDigest(tdigest) AS centroids_json
FROM sketch
LIMIT 10;
```

### Example 3: Custom Analysis

```sql
WITH 
    sketch AS (
        SELECT serializedTDigest(latency_ms) AS tdigest
        FROM requests
    ),
SELECT 
    centroidsFromTDigest(tdigest) AS centroids_json
FROM sketch;
```

## Implementation Details

Centroids are the internal representation of TDigest:
- **Mean**: Representative value for a cluster
- **Weight**: Number of values in the cluster

TDigest adaptively clusters values to maintain accuracy:
- More centroids near distribution extremes (tails)
- Fewer centroids in the middle (bulk)
- Total centroids bounded regardless of input size

Use cases:
- Debugging TDigest behavior
- Custom percentile algorithms
- Visualization of value distribution
- Understanding sketch compression

For most use cases, use `percentileFromTDigest` instead of extracting centroids directly.

## See Also

- [serializedTDigest](../../sql-reference/aggregate-functions/reference/serializedtdigest) — Create TDigest sketch
- [percentileFromTDigest](../../sql-reference/functions/percentilefromtdigest) — Extract percentile from TDigest
- [mergeSerializedTDigest](../../sql-reference/aggregate-functions/reference/mergeserializedtdigest) — Merge TDigest sketches
