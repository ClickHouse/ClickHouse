---
slug: /en/engines/table-engines/mergetree-family/hybrid-storage
sidebar_label: Hybrid Row-Column Storage
sidebar_position: 95
keywords: [hybrid storage, row storage, column storage, point query, wide query]
---

# Hybrid Row-Column Storage

## Overview

Hybrid row-column storage is an experimental feature in ClickHouse that stores data in both columnar format (traditional MergeTree storage) and row-oriented format (in a special `__row` column). This enables automatic query optimization to choose the most efficient access pattern based on the query's column selection.

:::warning Experimental Feature
This feature is experimental and may change in future versions. Use with caution in production environments.
:::

## When to Use Hybrid Storage

Hybrid storage is beneficial for:

| Use Case | Benefit |
|----------|---------|
| **Point queries** (`SELECT * WHERE id = 123`) | 2-5x faster |
| **Wide queries** (selecting many columns from tables with 50+ columns) | 3-10x faster |
| **OLTP-like workloads** (frequent row-level access patterns) | Significant improvement |
| **Hybrid workloads** (mix of analytical and transactional queries) | Automatic optimization |

Traditional columnar storage remains better for:
- Analytical queries selecting few columns from many rows
- Aggregations on specific columns
- Narrow tables (< 10 columns)
- Column-oriented scans

## Configuration

### Table-Level Settings

Enable hybrid storage when creating a table:

```sql
CREATE TABLE my_table
(
    id UInt64,
    col1 String,
    col2 UInt32,
    col3 Float64,
    ...
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    enable_hybrid_storage = 1,
    hybrid_storage_max_row_size = 1048576,
    hybrid_storage_column_threshold = 0.5,
    hybrid_storage_compression_codec = 'ZSTD(3)';
```

### Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `enable_hybrid_storage` | `0` (disabled) | Enable hybrid row-column storage. When enabled, data is stored in both columnar format and row format in the `__row` column. |
| `hybrid_storage_max_row_size` | `1048576` (1 MB) | Maximum size of serialized row data in bytes. Rows exceeding this limit will not be stored in the `__row` column and will fall back to column-based reading. |
| `hybrid_storage_column_threshold` | `0.5` (50%) | Threshold for switching to row-based reading. If a query requests more than this fraction of non-key columns, use the `__row` column instead of reading individual columns. Value should be between 0.0 and 1.0. |
| `hybrid_storage_compression_codec` | `'ZSTD(3)'` | Compression codec for `__row` column data. |

### Query-Level Settings

Control hybrid storage optimization at query level:

```sql
-- Enable hybrid storage optimization (default)
SET query_plan_optimize_hybrid_storage = 1;

-- Disable hybrid storage optimization for a specific query
SELECT * FROM my_table
SETTINGS query_plan_optimize_hybrid_storage = 0;
```

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Query Execution                          │
│                                                             │
│  ┌──────────────┐      ┌──────────────────────────────┐    │
│  │ Query Parser │─────▶│  Query Plan Optimizer        │    │
│  └──────────────┘      │  - Analyze column selection  │    │
│                        │  - Calculate cost            │    │
│                        │  - Choose read strategy      │    │
│                        └──────────┬───────────────────┘    │
│                                   │                         │
│                        ┌──────────▼───────────────────┐    │
│                        │  Decision: Row vs Column     │    │
│                        └──────────┬───────────────────┘    │
│                                   │                         │
│              ┌────────────────────┴────────────────────┐   │
│              │                                          │   │
│    ┌─────────▼──────────┐              ┌───────────────▼──────────┐
│    │ Row-Based Reading  │              │ Column-Based Reading     │
│    │ (from __row)       │              │ (traditional)            │
│    └─────────┬──────────┘              └───────────────┬──────────┘
│              │                                          │   │
└──────────────┼──────────────────────────────────────────┼───┘
               │                                          │
┌──────────────▼──────────────────────────────────────────▼───┐
│                    Storage Layer                            │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Column 1     │  │ Column 2     │  │ __row        │      │
│  │ (col1.bin)   │  │ (col2.bin)   │  │ (binary)     │      │
│  │              │  │              │  │              │      │
│  │ Traditional  │  │ Traditional  │  │ Serialized   │      │
│  │ columnar     │  │ columnar     │  │ row data     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                             │
│  Data is stored in BOTH formats simultaneously             │
└─────────────────────────────────────────────────────────────┘
```

### Decision Logic

The query optimizer uses a simple cost model:

1. **Calculate selection ratio**: `requested_non_key_columns / total_non_key_columns`
2. **Compare with threshold**: If ratio >= `hybrid_storage_column_threshold`, use row-based reading
3. **Fall back gracefully**: If `__row` column doesn't exist (older parts), use column-based reading

## Monitoring

### ProfileEvents

The following ProfileEvents track hybrid storage usage:

| Event | Description |
|-------|-------------|
| `HybridStorageRowBasedReads` | Number of times row-based reading was used |
| `HybridStorageColumnBasedReads` | Number of times column-based reading was used (when hybrid storage is enabled) |
| `HybridStorageBytesReadFromRow` | Bytes read from `__row` column |
| `HybridStorageBytesWrittenToRow` | Bytes written to `__row` column |
| `HybridStorageRowsReadFromRow` | Number of rows read from `__row` column |
| `HybridStorageRowsWrittenToRow` | Number of rows written to `__row` column |
| `HybridStorageRowSerializationMicroseconds` | Time spent serializing row data |
| `HybridStorageRowDeserializationMicroseconds` | Time spent deserializing row data |
| `HybridStorageRowsTooLarge` | Number of rows that exceeded size limit |
| `HybridStorageChecksumMismatches` | Number of checksum verification failures |

### Query Log

The `system.query_log` table includes:

| Field | Type | Description |
|-------|------|-------------|
| `used_hybrid_storage` | `UInt8` | Whether hybrid storage was used during query execution |
| `hybrid_storage_mode` | `String` | Mode of reading: 'row', 'column', or empty |

### Example Queries

```sql
-- Check hybrid storage ProfileEvents
SELECT name, value, description
FROM system.events
WHERE name LIKE 'HybridStorage%';

-- Monitor hybrid storage usage in query log
SELECT
    query,
    used_hybrid_storage,
    hybrid_storage_mode,
    ProfileEvents['HybridStorageRowBasedReads'] as row_reads,
    ProfileEvents['HybridStorageColumnBasedReads'] as col_reads
FROM system.query_log
WHERE event_date = today()
  AND type = 'QueryFinish'
  AND used_hybrid_storage = 1;

-- Check hybrid storage settings for tables
SELECT
    database,
    table,
    name,
    value
FROM system.merge_tree_settings
WHERE name LIKE 'hybrid_storage%'
  AND database = 'default';
```

## Storage Overhead

Hybrid storage duplicates data in two formats, resulting in additional storage overhead:

| Scenario | Typical Overhead |
|----------|------------------|
| Best case (good compression) | 10-15% |
| Typical | 15-25% |
| Worst case (poor compression) | 30-40% |

To minimize overhead:
- Use strong compression (`ZSTD(3)` is the default)
- Only enable for tables that benefit (many columns, frequent wide queries)
- Configure appropriate size limits

## Merges and Mutations

### Merge Behavior

During merges:
- Existing `__row` columns are merged together
- For parts without `__row` columns, the optimizer generates them during merge
- Data integrity is maintained between columnar and row formats

### Mutation Behavior

Mutations (UPDATE, DELETE) that affect data columns will:
- Update both columnar data and `__row` column
- Regenerate `__row` column if necessary
- Maintain consistency between formats

## Best Practices

1. **Enable for wide tables**: Tables with 50+ columns benefit most
2. **Set appropriate threshold**: Default 0.5 (50%) works well for most cases
3. **Monitor storage overhead**: Use `system.parts` to track storage usage
4. **Use with point queries**: Excellent for `SELECT * WHERE id = X` patterns
5. **Test with your workload**: Benchmark before production deployment

## Limitations

- **Experimental feature**: API and behavior may change
- **Storage overhead**: 10-40% additional storage required
- **Complex types**: Some complex nested types may have limitations
- **Not for all workloads**: Analytical queries on few columns don't benefit

## Examples

### Basic Usage

```sql
-- Create table with hybrid storage
CREATE TABLE products
(
    id UInt64,
    name String,
    description String,
    price Float64,
    category String,
    attributes Map(String, String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1;

-- Insert data
INSERT INTO products VALUES
    (1, 'Widget A', 'A great widget', 9.99, 'Electronics', {'color': 'blue'}, now(), now());

-- Point query (benefits from hybrid storage)
SELECT * FROM products WHERE id = 1;

-- Analytical query (uses columnar storage)
SELECT category, avg(price) FROM products GROUP BY category;
```

### Tuning for Wide Tables

```sql
CREATE TABLE wide_table
(
    id UInt64,
    -- 100 columns
    c1 String, c2 String, c3 String, -- ...
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    enable_hybrid_storage = 1,
    hybrid_storage_column_threshold = 0.3,  -- Use row-based for 30%+ columns
    hybrid_storage_max_row_size = 2097152;  -- 2 MB limit
```

## Related

- [MergeTree Engine](/docs/en/engines/table-engines/mergetree-family/mergetree.md)
- [Projections](/docs/en/engines/table-engines/mergetree-family/mergetree.md#projections)
