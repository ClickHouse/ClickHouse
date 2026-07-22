---
description: 'Extracts centroids (mean and weight pairs) from a serialized TDigest sketch'
doc_type: 'reference'
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

- JSON-formatted string of the form `{"means":[...],"weights":[...]}` with parallel arrays of centroid means and weights. Parallel arrays are used because a TDigest can contain multiple centroids with the same mean. Type: [String](../../sql-reference/data-types/string).
- Returns `{"means":[],"weights":[]}` if the sketch is empty.
- Throws an exception if the input is not a valid serialized TDigest sketch.

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

- [serializedTDigest](../../sql-reference/aggregate-functions/reference/serializedTDigest) — Create TDigest sketch
- [percentileFromTDigest](../../sql-reference/functions/percentilefromtdigest) — Extract percentile from TDigest
- [mergeSerializedTDigest](../../sql-reference/aggregate-functions/reference/mergeSerializedTDigest) — Merge TDigest sketches
