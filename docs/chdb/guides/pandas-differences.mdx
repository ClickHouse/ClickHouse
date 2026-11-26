---
title: 'Key differences from pandas'
sidebarTitle: 'Key differences'
slug: /chdb/guides/pandas-differences
description: 'Important differences between DataStore and pandas'
keywords: ['chdb', 'datastore', 'pandas', 'differences', 'behavior']
doc_type: 'guide'
---

While DataStore is highly compatible with pandas, there are important differences to understand.

## Summary Table {#summary}

| Aspect | pandas | DataStore |
|--------|--------|-----------|
| **Execution** | Eager (immediate) | Lazy (deferred) |
| **Return types** | DataFrame/Series | DataStore/ColumnExpr |
| **Row order** | Preserved | Preserved (automatic); not guaranteed in [performance mode](/chdb/configuration/performance-mode) |
| **inplace** | Supported | Not supported |
| **Index** | Full support | Simplified |
| **Memory** | All data in memory | Data at source |

---

## 1. Lazy vs Eager Execution {#lazy-execution}

### pandas (Eager) {#pandas-eager}

Operations execute immediately:

```python
import pandas as pd

df = pd.read_csv("data.csv")  # Loads entire file NOW
result = df[df['age'] > 25]   # Filters NOW
grouped = result.groupby('city')['salary'].mean()  # Aggregates NOW
```

### DataStore (Lazy) {#datastore-lazy}

Operations are deferred until results are needed:

```python
from chdb import datastore as pd

ds = pd.read_csv("data.csv")  # Just records the source
result = ds[ds['age'] > 25]   # Just records the filter
grouped = result.groupby('city')['salary'].mean()  # Just records

# Execution happens here:
print(grouped)        # Executes when displaying
df = grouped.to_df()  # Or when converting to pandas
```

### Why It Matters {#why-lazy}

Lazy execution enables:
- **Query optimization**: Multiple operations compile to one SQL query
- **Column pruning**: Only needed columns are read
- **Filter pushdown**: Filters apply at the source
- **Memory efficiency**: Don't load data you don't need

---

## 2. Return Types {#return-types}

### pandas {#pandas-return-types}

```python
df['col']           # Returns pd.Series
df[['a', 'b']]      # Returns pd.DataFrame
df[df['x'] > 10]    # Returns pd.DataFrame
df.groupby('x')     # Returns DataFrameGroupBy
```

### DataStore {#datastore-return-types}

```python
ds['col']           # Returns ColumnExpr (lazy)
ds[['a', 'b']]      # Returns DataStore (lazy)
ds[ds['x'] > 10]    # Returns DataStore (lazy)
ds.groupby('x')     # Returns LazyGroupBy
```

### Converting to pandas Types {#converting-to-pandas-types}

```python
# Get pandas DataFrame
df = ds.to_df()
df = ds.to_pandas()

# Get pandas Series from column
series = ds['col'].to_pandas()

# Or trigger execution
print(ds)  # Automatically converts for display
```

---

## 3. Execution Triggers {#triggers}

DataStore executes when you need actual values:

| Trigger | Example | Notes |
|---------|---------|-------|
| `print()` / `repr()` | `print(ds)` | Display needs data |
| `len()` | `len(ds)` | Need row count |
| `.columns` | `ds.columns` | Need column names |
| `.dtypes` | `ds.dtypes` | Need type info |
| `.shape` | `ds.shape` | Need dimensions |
| `.values` | `ds.values` | Need actual data |
| `.index` | `ds.index` | Need index |
| `to_df()` | `ds.to_df()` | Explicit conversion |
| Iteration | `for row in ds` | Need to iterate |
| `equals()` | `ds.equals(other)` | Need comparison |

### Operations That Stay Lazy {#stay-lazy}

| Operation | Returns |
|-----------|---------|
| `filter()` | DataStore |
| `select()` | DataStore |
| `sort()` | DataStore |
| `groupby()` | LazyGroupBy |
| `join()` | DataStore |
| `ds['col']` | ColumnExpr |
| `ds[['a', 'b']]` | DataStore |
| `ds[condition]` | DataStore |

---

## 4. Row Order {#row-order}

### pandas {#pandas-row-order}

Row order is always preserved:

```python
df = pd.read_csv("data.csv")
print(df.head())  # Always same order as file
```

### DataStore {#datastore-row-order}

Row order is **automatically preserved** for most operations:

```python
ds = pd.read_csv("data.csv")
print(ds.head())  # Matches file order

# Filter preserves order
ds_filtered = ds[ds['age'] > 25]  # Same order as pandas
```

DataStore automatically tracks original row positions internally (using `rowNumberInAllBlocks()`) to ensure order consistency with pandas.

### When Order Is Preserved {#order-preserved}

- File sources (CSV, Parquet, JSON, etc.)
- pandas DataFrame sources
- Filter operations
- Column selection
- After explicit `sort()` or `sort_values()`
- Operations that define order (`nlargest()`, `nsmallest()`, `head()`, `tail()`)

### When Order May Differ {#order-may-differ}

- After `groupby()` aggregations (use `sort_values()` to ensure consistent order)
- After `merge()` / `join()` with certain join types
- In **performance mode** (`config.use_performance_mode()`): row order is not guaranteed for any operation. See [Performance Mode](/chdb/configuration/performance-mode).

---

## 5. No inplace Parameter {#no-inplace}

### pandas {#pandas-inplace}

```python
df.drop(columns=['col'], inplace=True)  # Modifies df
df.fillna(0, inplace=True)              # Modifies df
df.rename(columns={'old': 'new'}, inplace=True)
```

### DataStore {#datastore-inplace}

`inplace=True` is not supported. Always assign the result:

```python
ds = ds.drop(columns=['col'])           # Returns new DataStore
ds = ds.fillna(0)                       # Returns new DataStore
ds = ds.rename(columns={'old': 'new'})  # Returns new DataStore
```

### Why No inplace? {#why-no-inplace}

DataStore uses immutable operations to enable:
- Query building (lazy evaluation)
- Thread safety
- Easier debugging
- Cleaner code

---

## 6. Index Support {#index}

### pandas {#pandas-index}

Full index support:

```python
df = df.set_index('id')
df.loc['user123']           # Label-based access
df.loc['a':'z']             # Label-based slicing
df.reset_index()
df.index.name = 'user_id'
```

### DataStore {#datastore-index}

Simplified index support:

```python
# Basic operations work
ds.loc[0:10]               # Integer position
ds.iloc[0:10]              # Same as loc for DataStore

# For pandas-style index operations, convert first
df = ds.to_df()
df = df.set_index('id')
df.loc['user123']
```

### DataStore Source Matters {#datastore-source-matters}

- **DataFrame source**: Preserves pandas index
- **File source**: Uses simple integer index

---

## 7. Comparison Behavior {#comparison}

### Comparing with pandas {#comparing-with-pandas}

pandas doesn't recognize DataStore objects:

```python
import pandas as pd
from chdb import datastore as ds

pdf = pd.DataFrame({'a': [1, 2, 3]})
dsf = ds.DataFrame({'a': [1, 2, 3]})

# This doesn't work as expected
pdf == dsf  # pandas doesn't know DataStore

# Solution: convert DataStore to pandas
pdf.equals(dsf.to_pandas())  # True
```

### Using equals() {#using-equals}

```python
# DataStore.equals() also works
dsf.equals(pdf)  # Compares with pandas DataFrame
```

---

## 8. Type Inference {#types}

### pandas {#pandas-types}

Uses numpy/pandas types:

```python
df['col'].dtype  # int64, float64, object, datetime64, etc.
```

### DataStore {#datastore-types}

May use ClickHouse types:

```python
ds['col'].dtype  # Int64, Float64, String, DateTime, etc.

# Types are converted when going to pandas
df = ds.to_df()
df['col'].dtype  # Now pandas type
```

### Explicit Casting {#explicit-casting}

```python
# Force specific type
ds['col'] = ds['col'].astype('int64')
```

---

## 9. Memory Model {#memory}

### pandas {#pandas-memory}

All data lives in memory:

```python
df = pd.read_csv("huge.csv")  # 10GB in memory!
```

### DataStore {#datastore-memory}

Data stays at source until needed:

```python
ds = pd.read_csv("huge.csv")  # Just metadata
ds = ds.filter(ds['year'] == 2024)  # Still just metadata

# Only filtered result is loaded
df = ds.to_df()  # Maybe only 1GB now
```

---

## 10. Error Messages {#errors}

### Different Error Sources {#different-error-sources}

- **pandas errors**: From pandas library
- **DataStore errors**: From chDB or ClickHouse

```python
# May see ClickHouse-style errors
# "Code: 62. DB::Exception: Syntax error..."
```

### Debugging Tips {#debugging-tips}

```python
# View the SQL to debug
print(ds.to_sql())

# See execution plan
ds.explain()

# Enable debug logging
from chdb.datastore.config import config
config.enable_debug()
```

---

## Migration Checklist {#checklist}

When migrating from pandas:

- [ ] Change import statement
- [ ] Remove `inplace=True` parameters
- [ ] Add explicit `to_df()` where pandas DataFrame is required
- [ ] Add sorting if row order matters
- [ ] Use `to_pandas()` for comparison tests
- [ ] Test with representative data sizes

---

## Quick Reference {#quick-ref}

| pandas | DataStore |
|--------|-----------|
| `df[condition]` | Same (returns DataStore) |
| `df.groupby()` | Same (returns LazyGroupBy) |
| `df.drop(inplace=True)` | `ds = ds.drop()` |
| `df.equals(other)` | `ds.to_pandas().equals(other)` |
| `df.loc['label']` | `ds.to_df().loc['label']` |
| `print(df)` | Same (triggers execution) |
| `len(df)` | Same (triggers execution) |
