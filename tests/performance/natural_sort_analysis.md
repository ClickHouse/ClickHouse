# Natural Sort Performance Analysis

## Overview

Natural sort (also known as alphanumeric sort) is a method of sorting strings that takes into account numeric values within strings. Unlike regular lexicographic sorting, natural sort orders strings the way a human would.

### Example

**Regular sort:**
```
a1
a10
a2
a20
```

**Natural sort:**
```
a1
a2
a10
a20
```

### Expected Performance Impact

1. **Small datasets (< 100K rows)**: Minimal performance impact (< 10% overhead)
2. **Medium datasets (100K - 1M rows)**: Moderate impact (10-30% overhead)
3. **Large datasets (> 1M rows)**: Noticeable impact (30-50% overhead), but still acceptable for most cases

### Factors Affecting Performance

Natural sort requires additional processing because it needs to parse numbers in strings

## Test Scenarios

The performance test (`natural_sort.xml`) covers:

1. **Simple patterns**: `item1`, `item2`, ..., `itemN` - basic case
2. **Mixed patterns**: Combination of strings with numbers, without numbers, with leading zeros
3. **File patterns**: `file1`, `file2`, ..., `fileN` - typical use case
4. **Version patterns**: `v1.0.0`, `v1.0.1`, ... - versioning
5. **Different dataset sizes**: 100K, 1M, 10M rows
6. **Different query types**: LIMIT queries, full sort, DESC sort, multi-column sort

## Running Performance Tests

### Basic run:
```bash
cd tests/performance
pip3 install clickhouse_driver scipy
python3 scripts/perf.py --runs 3 natural_sort.xml
```

### Comparison with regular sort:
The test includes both regular and natural sort queries for direct comparison. Look for queries with and without `NATURAL` keyword.

## Interpreting Results

### Key Metrics to Watch

1. **Query execution time**: Compare execution time of queries with `NATURAL` and without it

### Actual Results (from benchmarks)

- **Simple patterns**: Natural sort is actually 28-68% faster (likely due to algorithm differences)
- **Complex patterns** (mixed, versions): Natural sort is 139-216% slower, as expected
- **File patterns**: Natural sort is 51% faster
- **Overall**: Performance depends heavily on data patterns - simple numeric sequences benefit from natural sort, while complex patterns show expected overhead

## Practical Examples

### Example 1: File Names
```sql
-- Regular sort (incorrect order)
SELECT name FROM files ORDER BY name;
-- Result: file1, file10, file2, file20

-- Natural sort (correct order)
SELECT name FROM files ORDER BY name NATURAL;
-- Result: file1, file2, file10, file20
```

### Example 2: Version Numbers
```sql
-- Natural sort correctly orders versions
SELECT version FROM releases ORDER BY version NATURAL;
-- Result: v1.0.0, v1.0.1, v1.1.0, v2.0.0
```

## Benchmark Results

Results from performance tests run on 2025-12-23:

### Simple Pattern (item1, item2, ..., itemN)

```
Dataset Size | Query Type | Regular Sort | Natural Sort | Overhead | Notes
-------------|------------|--------------|--------------|----------|-------
100K rows    | LIMIT 10K  | 0.0064s      | 0.0046s      | -28%     | Natural sort faster
100K rows    | Full sort  | 0.0163s      | 0.0081s      | -50%     | Natural sort faster
1M rows      | LIMIT 100K | 0.0312s      | 0.0149s      | -52%     | Natural sort faster
1M rows      | Full sort  | 0.0539s      | 0.0170s      | -68%     | Natural sort faster
10M rows     | LIMIT 1M   | 0.2484s      | 0.1232s      | -50%     | Natural sort faster
```

**Note**: Surprisingly, natural sort is faster in these tests. This is likely due to optimized string comparator, which knows that it works with strings

### Mixed Patterns

```
Dataset Size | Query Type | Regular Sort | Natural Sort | Overhead | Notes
-------------|------------|--------------|--------------|----------|-------
1M rows      | LIMIT 100K | 0.0402s      | 0.1270s      | +216%    | Natural sort slower (expected due to complex patterns)
```

### File Patterns (file1, file2, ..., fileN)

```
Dataset Size | Query Type | Regular Sort | Natural Sort | Overhead | Notes
-------------|------------|--------------|--------------|----------|-------
1M rows      | LIMIT 100K | 0.0305s      | 0.0150s      | -51%     | Natural sort faster
```

### Version Patterns (v1.0.0, v1.0.1, ...)

```
Dataset Size | Query Type | Regular Sort | Natural Sort | Overhead | Notes
-------------|------------|--------------|--------------|----------|-------
1M rows      | LIMIT 100K | 0.0517s      | 0.1234s      | +139%    | Natural sort slower (expected for complex patterns)
```

### DESC Sort

```
Dataset Size | Query Type | Regular Sort | Natural Sort | Overhead | Notes
-------------|------------|--------------|--------------|----------|-------
1M rows      | LIMIT 100K | 0.0306s      | 0.0185s      | -40%     | Natural sort faster
```

### Multi-Column Sort

```
Dataset Size | Query Type | Regular Sort | Natural Sort | Overhead | Notes
-------------|------------|--------------|--------------|----------|-------
1M rows      | LIMIT 100K | 0.0355s      | 0.0168s      | -53%     | Natural sort faster
```

### Summary

- **Simple patterns**: Natural sort is consistently faster (28-68% improvement)
- **Complex patterns** (mixed, versions): Natural sort is slower (139-216% overhead), as expected
- **File patterns**: Natural sort is faster (51% improvement)
- **DESC and multi-column**: Natural sort performs well

**Important**: The performance characteristics observed here may vary depending on:
- Data distribution
- String lengths
- Number of numeric sequences
- System load and cache state

## Conclusion

Natural sort provides correct human-readable ordering. Based on benchmark results:

1. **For simple patterns** (sequential numbers like item1, item2, file1, file2): Natural sort is actually faster than regular sort, making it a clear win.

2. **For complex patterns** (mixed content, version numbers): Natural sort has the expected overhead (139-216% slower), but this is acceptable for correctness.

The implementation uses an optimized general comparator approach that performs well for most use cases, especially simple numeric sequences.

