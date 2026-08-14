---
title: 'DataStore profiling'
sidebarTitle: 'Profiling'
slug: /chdb/debugging/profiling
description: 'Measure DataStore performance with the built-in profiler'
keywords: ['chdb', 'datastore', 'profiling', 'performance', 'timing', 'benchmark']
doc_type: 'guide'
---

The DataStore profiler helps you measure execution time and identify performance bottlenecks.

## Quick Start {#quick-start}

```python
from chdb import datastore as pd
from chdb.datastore.config import config, get_profiler

# Enable profiling
config.enable_profiling()

# Run your operations
ds = pd.read_csv("large_data.csv")
result = (ds
    .filter(ds['amount'] > 100)
    .groupby('category')
    .agg({'amount': 'sum'})
    .sort('sum', ascending=False)
    .head(10)
    .to_df()
)

# View report
profiler = get_profiler()
print(profiler.report())
```

## Enabling Profiling {#enabling}

```python
from chdb.datastore.config import config

# Enable profiling
config.enable_profiling()

# Disable profiling
config.disable_profiling()

# Check if profiling is enabled
print(config.profiling_enabled)  # True or False
```

---

## Profiler API {#api}

### Getting the Profiler {#get-profiler}

```python
from chdb.datastore.config import get_profiler

profiler = get_profiler()
```

### report() {#report}

Display a performance report.

```python
profiler.report(min_duration_ms=0.1)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_duration_ms` | float | `0.1` | Only show steps >= this duration |

**Example output:**

```text
======================================================================
EXECUTION PROFILE
======================================================================
   45.79ms (100.0%) Total Execution
     23.25ms ( 50.8%) Query Planning [ops_count=2]
     22.29ms ( 48.7%) SQL Segment 1 [ops=2]
       20.48ms ( 91.9%) SQL Execution
        1.74ms (  7.8%) Result to DataFrame
----------------------------------------------------------------------
      TOTAL:    45.79ms
======================================================================
```

The report shows:
- Duration in milliseconds for each step
- Percentage of parent/total time
- Hierarchical nesting of operations
- Metadata for each step (e.g., `ops_count`, `ops`)

### step() {#step}

Manually time a code block.

```python
with profiler.step("custom_operation"):
    # Your code here
    expensive_operation()
```

### clear() {#clear}

Clear all profiling data.

```python
profiler.clear()
```

### summary() {#summary}

Get a dictionary of step names to durations (ms).

```python
summary = profiler.summary()
for name, duration in summary.items():
    print(f"{name}: {duration:.2f}ms")
```

Example output:

```text
Total Execution: 45.79ms
Total Execution.Cache Check: 0.00ms
Total Execution.Query Planning: 23.25ms
Total Execution.SQL Segment 1: 22.29ms
Total Execution.SQL Segment 1.SQL Execution: 20.48ms
Total Execution.SQL Segment 1.Result to DataFrame: 1.74ms
```

---

## Understanding the Report {#understanding}

### Step Names {#step-names}

| Step Name | Description |
|-----------|-------------|
| `Total Execution` | Overall execution time |
| `Query Planning` | Time spent planning the query |
| `SQL Segment N` | Execution of SQL segment N |
| `SQL Execution` | Actual SQL query execution |
| `Result to DataFrame` | Converting results to pandas |
| `Cache Check` | Checking query cache |
| `Cache Write` | Writing results to cache |

### Duration {#duration}

- **Planning steps** (Query Planning): Usually fast
- **Execution steps** (SQL Execution): Where actual work happens
- **Transfer steps** (Result to DataFrame): Converting data to pandas

### Identifying Bottlenecks {#bottlenecks}

```text
======================================================================
EXECUTION PROFILE
======================================================================
  200.50ms (100.0%) Total Execution
    10.25ms (  5.1%) Query Planning [ops_count=4]
   190.00ms ( 94.8%) SQL Segment 1 [ops=4]
     185.00ms ( 97.4%) SQL Execution    <- Main bottleneck
       5.00ms (  2.6%) Result to DataFrame
----------------------------------------------------------------------
      TOTAL:   200.50ms
======================================================================
```

---

## Profiling Patterns {#patterns}

### Profile a Single Query {#single-query}

```python
config.enable_profiling()
profiler = get_profiler()
profiler.clear()  # Clear previous data

# Run query
result = ds.filter(...).groupby(...).agg(...).to_df()

# View this query's profile
print(profiler.report())
```

### Profile Multiple Queries {#multiple-queries}

```python
config.enable_profiling()
profiler = get_profiler()
profiler.clear()

# Query 1
with profiler.step("Query 1"):
    result1 = query1.to_df()

# Query 2
with profiler.step("Query 2"):
    result2 = query2.to_df()

print(profiler.report())
```

### Compare Approaches {#compare}

```python
profiler = get_profiler()

# Approach 1: Filter then groupby
profiler.clear()
with profiler.step("filter_then_groupby"):
    result1 = ds.filter(ds['x'] > 10).groupby('y').sum().to_df()
summary1 = profiler.summary()
time1 = summary1.get('filter_then_groupby', 0)

# Approach 2: Groupby then filter
profiler.clear()
with profiler.step("groupby_then_filter"):
    result2 = ds.groupby('y').sum().filter(ds['x'] > 10).to_df()
summary2 = profiler.summary()
time2 = summary2.get('groupby_then_filter', 0)

print(f"Approach 1: {time1:.2f}ms")
print(f"Approach 2: {time2:.2f}ms")
print(f"Winner: {'Approach 1' if time1 < time2 else 'Approach 2'}")
```

---

## Optimization Tips {#optimization}

### 1. Check SQL Execution Time {#check-sql}

If `SQL execution` is the bottleneck:
- Add more filters to reduce data
- Use Parquet instead of CSV
- Check for proper indexes (for database sources)

### 2. Check I/O Time {#check-io}

If `read_csv` or `read_parquet` is the bottleneck:
- Use Parquet (columnar, compressed)
- Read only needed columns
- Filter at source if possible

### 3. Check Data Transfer {#check-transfer}

If `to_df` is slow:
- Result set may be too large
- Add more filters or limit
- Use `head()` for previewing

### 4. Compare Engines {#compare-engines}

```python
from chdb.datastore.config import config

# Profile with chdb
config.use_chdb()
profiler.clear()
result_chdb = query.to_df()
time_chdb = profiler.total_duration_ms

# Profile with pandas
config.use_pandas()
profiler.clear()
result_pandas = query.to_df()
time_pandas = profiler.total_duration_ms

print(f"chdb: {time_chdb:.2f}ms")
print(f"pandas: {time_pandas:.2f}ms")
```

---

## Best Practices {#best-practices}

### 1. Profile Before Optimizing {#best-practice-1}

```python
# Don't guess - measure!
config.enable_profiling()
result = your_query.to_df()
print(get_profiler().report())
```

### 2. Clear Between Tests {#best-practice-2}

```python
profiler.clear()  # Clear previous data
# Run test
print(profiler.report())
```

### 3. Use min_duration_ms for Focus {#best-practice-3}

```python
# Only show operations >= 100ms
profiler.report(min_duration_ms=100)
```

### 4. Profile Representative Data {#best-practice-4}

```python
# Profile with real-world data sizes
# Small test data may not show real bottlenecks
```

### 5. Disable in Production {#best-practice-5}

```python
# Development
config.enable_profiling()

# Production
config.set_profiling_enabled(False)  # Avoid overhead
```

---

## Example: Full Profiling Session {#example}

```python
from chdb import datastore as pd
from chdb.datastore.config import config, get_profiler

# Setup
config.enable_profiling()
config.enable_debug()  # Also see what's happening
profiler = get_profiler()

# Load data
profiler.clear()
print("=== Loading Data ===")
ds = pd.read_csv("sales_2024.csv")  # 10M rows
print(profiler.report())

# Query 1: Simple filter
profiler.clear()
print("\n=== Query 1: Simple Filter ===")
result1 = ds.filter(ds['amount'] > 1000).to_df()
print(profiler.report())

# Query 2: Complex aggregation
profiler.clear()
print("\n=== Query 2: Complex Aggregation ===")
result2 = (ds
    .filter(ds['amount'] > 100)
    .groupby('region', 'category')
    .agg({
        'amount': ['sum', 'mean', 'count'],
        'quantity': 'sum'
    })
    .sort('sum', ascending=False)
    .head(20)
    .to_df()
)
print(profiler.report())

# Summary
print("\n=== Summary ===")
print(f"Query 1: {len(result1)} rows")
print(f"Query 2: {len(result2)} rows")
```
