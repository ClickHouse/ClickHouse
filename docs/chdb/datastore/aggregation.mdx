---
title: 'DataStore aggregation functions'
sidebarTitle: 'Aggregation'
slug: /chdb/datastore/aggregation
description: 'Aggregate functions, window functions, and the F namespace in DataStore'
keywords: ['chdb', 'datastore', 'aggregation', 'window', 'groupby', 'sum', 'mean', 'avg']
doc_type: 'reference'
---

DataStore provides comprehensive aggregation and window function support, leveraging ClickHouse's powerful SQL aggregation capabilities.

## Basic Aggregations {#basic}

### Built-in Methods {#builtin}

| Method | SQL Equivalent | Description |
|--------|---------------|-------------|
| `sum()` | `SUM()` | Sum of values |
| `mean()` | `AVG()` | Average/mean |
| `count()` | `COUNT()` | Count non-null values |
| `min()` | `MIN()` | Minimum value |
| `max()` | `MAX()` | Maximum value |
| `median()` | `MEDIAN()` | Median value |
| `std()` | `stddevPop()` | Standard deviation |
| `var()` | `varPop()` | Variance |
| `nunique()` | `COUNT(DISTINCT)` | Count unique values |

**Examples:**

```python
from pathlib import Path
Path("sales.csv").write_text("""\
region,product,category,amount,quantity,price,date,order_id
East,Widget,Electronics,5200,10,120,2024-01-15,1001
West,Gadget,Electronics,800,5,160,2024-02-20,1002
East,Gizmo,Home,6500,3,100,2024-03-10,1003
North,Widget,Electronics,4500,6,150,2024-06-18,1004
West,Gadget,Electronics,2000,8,250,2024-09-14,1005
""")

from chdb import datastore as pd

ds = pd.read_csv("sales.csv")

# Single column aggregation
total = ds['amount'].sum()
average = ds['amount'].mean()
count = ds['amount'].count()

# All aggregations
print(ds['amount'].sum())    # Total
print(ds['amount'].mean())   # Average
print(ds['amount'].std())    # Standard deviation
print(ds['amount'].median()) # Median
print(ds['amount'].nunique()) # Unique count
```

---

## GroupBy Aggregations {#groupby}

### Single Aggregation {#single-agg}

```python
# Group by and aggregate
result = ds.groupby('category')['amount'].sum()
result = ds.groupby('region')['sales'].mean()
```

### Multiple Aggregations {#multi-agg}

```python
# Dictionary syntax
result = ds.groupby('category').agg({
    'amount': 'sum',
    'quantity': 'mean',
    'order_id': 'count'
})

# List of aggregations per column
result = ds.groupby('category').agg({
    'amount': ['sum', 'mean', 'max'],
    'quantity': ['sum', 'count']
})
```

### Named Aggregations {#named-agg}

```python
# Named aggregation (pandas style)
result = ds.groupby('region').agg(
    total_amount=('amount', 'sum'),
    avg_quantity=('quantity', 'mean'),
    order_count=('order_id', 'count'),
    max_price=('price', 'max')
)
```

### Multiple GroupBy Keys {#multi-groupby}

```python
# Group by multiple columns
result = ds.groupby(['region', 'category']).agg({
    'amount': 'sum',
    'quantity': 'sum'
})
```

---

## Statistical Aggregations {#statistical}

| Method | SQL Equivalent | Description |
|--------|---------------|-------------|
| `quantile(q)` | `quantile(q)` | q-th quantile (0-1) |
| `skew()` | `skewPop()` | Skewness |
| `kurt()` | `kurtPop()` | Kurtosis |
| `corr()` | `corr()` | Correlation |
| `cov()` | `covar()` | Covariance |
| `sem()` | - | Standard error of mean |

**Examples:**

```python
# Quantiles
q50 = ds['amount'].quantile(0.5)  # Median
q95 = ds['amount'].quantile(0.95) # 95th percentile

# Multiple quantiles
quantiles = ds['amount'].quantile([0.25, 0.5, 0.75])

# Correlation between columns
correlation = ds[['sales', 'marketing_spend']].corr()
```

---

## Conditional Aggregations {#conditional}

ClickHouse-specific conditional aggregation functions.

| Function | ClickHouse | Description |
|----------|------------|-------------|
| `sum_if(cond)` | `sumIf()` | Sum where condition |
| `count_if(cond)` | `countIf()` | Count where condition |
| `avg_if(cond)` | `avgIf()` | Average where condition |
| `min_if(cond)` | `minIf()` | Min where condition |
| `max_if(cond)` | `maxIf()` | Max where condition |

**Examples:**

```python
from chdb.datastore import F, Field

# Sum only high value orders
high_value_sum = F.sum_if(Field('amount'), Field('amount') > 1000)

# Count active users
active_count = F.count_if(Field('status') == 'active')

# In groupby context
result = ds.groupby('region').agg({
    'total': ('amount', 'sum'),
    'high_value': ('amount', F.sum_if(Field('amount') > 1000)),
})
```

---

## Collection Aggregations {#collection}

ClickHouse-specific functions that collect values.

| Function | ClickHouse | Description |
|----------|------------|-------------|
| `group_array()` | `groupArray()` | Collect into array |
| `group_uniq_array()` | `groupUniqArray()` | Collect unique into array |
| `group_concat(sep)` | `groupConcat()` | Concatenate strings |
| `top_k(n)` | `topK(n)` | Top K frequent values |
| `any()` | `any()` | Any value |
| `any_last()` | `anyLast()` | Last value |
| `first_value()` | `first_value()` | First value in order |
| `last_value()` | `last_value()` | Last value in order |

**Examples:**

```python
from chdb.datastore import F, Field

# Collect all tags per category
result = ds.groupby('category').agg({
    'all_tags': ('tag', F.group_array()),
    'unique_tags': ('tag', F.group_uniq_array())
})

# Get top 5 products per region
result = ds.groupby('region').agg({
    'top_products': ('product_id', F.top_k(5))
})
```

---

## Window Functions {#window}

### Ranking Functions {#ranking}

| Function | SQL | Description |
|----------|-----|-------------|
| `row_number()` | `ROW_NUMBER()` | Sequential row number |
| `rank()` | `RANK()` | Rank with gaps |
| `dense_rank()` | `DENSE_RANK()` | Rank without gaps |
| `ntile(n)` | `NTILE(n)` | Divide into n buckets |
| `percent_rank()` | `PERCENT_RANK()` | Percentile rank (0-1) |
| `cume_dist()` | `CUME_DIST()` | Cumulative distribution |

**Examples:**

```python
from chdb.datastore import F, Field

# Add row number
ds['row_num'] = F.row_number().over(order_by='date')

# Rank within groups
ds['rank'] = F.rank().over(
    partition_by='category',
    order_by='sales'
)

# Dense rank (no gaps)
ds['dense_rank'] = F.dense_rank().over(
    partition_by='region',
    order_by=('revenue', 'desc')
)

# Divide into quartiles
ds['quartile'] = F.ntile(4).over(order_by='score')
```

### Value Functions {#value-functions}

| Function | SQL | Description |
|----------|-----|-------------|
| `lag(n)` | `LAG(col, n)` | Previous row value |
| `lead(n)` | `LEAD(col, n)` | Next row value |
| `first_value()` | `FIRST_VALUE()` | First value in window |
| `last_value()` | `LAST_VALUE()` | Last value in window |
| `nth_value(n)` | `NTH_VALUE(col, n)` | Nth value in window |

**Examples:**

```python
# Previous and next value
ds['prev_price'] = F.lag('price', 1).over(order_by='date')
ds['next_price'] = F.lead('price', 1).over(order_by='date')

# First and last in partition
ds['first_order'] = F.first_value('amount').over(
    partition_by='customer_id',
    order_by='date'
)
```

### Cumulative Functions {#cumulative}

| Method | Description |
|--------|-------------|
| `cumsum()` | Cumulative sum |
| `cummax()` | Cumulative maximum |
| `cummin()` | Cumulative minimum |
| `cumprod()` | Cumulative product |
| `diff(n)` | Difference from n rows back |
| `pct_change(n)` | Percent change from n rows back |

**Examples:**

```python
# Cumulative calculations
ds['running_total'] = ds['amount'].cumsum()
ds['running_max'] = ds['amount'].cummax()

# With grouping
ds['group_cumsum'] = ds.groupby('category')['amount'].cumsum()

# Period over period
ds['daily_diff'] = ds['sales'].diff(1)
ds['pct_change'] = ds['sales'].pct_change(1)
```

### Rolling Windows {#rolling}

```python
# Rolling window aggregations
ds['rolling_avg'] = ds['price'].rolling(window=7).mean()
ds['rolling_sum'] = ds['amount'].rolling(window=30).sum()
ds['rolling_std'] = ds['value'].rolling(window=10).std()

# Expanding windows
ds['expanding_max'] = ds['price'].expanding().max()
ds['expanding_sum'] = ds['amount'].expanding().sum()
```

---

## F Namespace {#f-namespace}

The `F` namespace provides access to ClickHouse functions.

### Import {#f-import}

```python
from chdb.datastore import F, Field
```

### Using F Functions {#f-usage}

```python
# Aggregations
F.sum(Field('amount'))
F.avg(Field('price'))
F.count(Field('id'))

# Statistical
F.quantile(Field('value'), 0.95)
F.stddev_pop(Field('score'))
F.corr(Field('x'), Field('y'))

# Conditional
F.sum_if(Field('amount'), Field('status') == 'completed')
F.count_if(Field('is_active'))

# String
F.length(Field('name'))
F.upper(Field('text'))

# Date/Time
F.to_year(Field('date'))
F.date_diff('day', Field('start'), Field('end'))

# Array
F.array_sum(Field('values'))
F.array_avg(Field('scores'))

# Math
F.abs(Field('delta'))
F.round(Field('price'), 2)
F.floor(Field('value'))
F.ceil(Field('value'))
```

### F with Window Functions {#f-window}

```python
# Define window frame
window = F.window(
    partition_by='category',
    order_by='date',
    rows_between=(-7, 0)  # Current row and 7 preceding
)

ds['rolling_avg'] = F.avg(Field('price')).over(window)
```

---

## Common Aggregation Patterns {#patterns}

### Top N per Group {#top-n}

```python
# Top 3 products per category by sales
result = (ds
    .assign(rank=F.row_number().over(
        partition_by='category',
        order_by=('sales', 'desc')
    ))
    .filter(ds['rank'] <= 3)
)
```

### Running Total {#running-total}

```python
# Running total of sales
ds['running_total'] = F.sum('amount').over(
    order_by='date',
    rows_between=(None, 0)  # All rows up to current
)
```

### Moving Average {#moving-avg}

```python
# 7-day moving average
ds['ma_7'] = F.avg('price').over(
    order_by='date',
    rows_between=(-6, 0)
)
```

### Year-over-Year Comparison {#yoy}

```python
# YoY comparison
ds['prev_year_sales'] = F.lag('sales', 12).over(
    partition_by='product_id',
    order_by='month'
)
ds['yoy_growth'] = (ds['sales'] - ds['prev_year_sales']) / ds['prev_year_sales']
```

### Percentile Ranking {#percentile}

```python
# Rank customers by total spend
ds['spend_percentile'] = F.percent_rank().over(order_by='total_spend')
```

---

## Aggregation Methods Summary {#summary}

| Category | Methods |
|----------|---------|
| **Basic** | `sum`, `mean`, `count`, `min`, `max`, `median` |
| **Statistical** | `std`, `var`, `quantile`, `skew`, `kurt`, `corr`, `cov` |
| **Conditional** | `sum_if`, `count_if`, `avg_if`, `min_if`, `max_if` |
| **Collection** | `group_array`, `group_uniq_array`, `group_concat`, `top_k` |
| **Ranking** | `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank` |
| **Value** | `lag`, `lead`, `first_value`, `last_value`, `nth_value` |
| **Cumulative** | `cumsum`, `cummax`, `cummin`, `cumprod`, `diff`, `pct_change` |
| **Rolling** | `rolling().mean/sum/std/...`, `expanding().mean/sum/...` |
