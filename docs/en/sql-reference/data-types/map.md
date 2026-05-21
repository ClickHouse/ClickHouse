---
description: 'Documentation for the Map data type in ClickHouse'
sidebar_label: 'Map(K, V)'
sidebar_position: 36
slug: /sql-reference/data-types/map
title: 'Map(K, V)'
doc_type: 'reference'
---

Data type `Map(K, V)` stores key-value pairs.

Unlike other databases, maps are not unique in ClickHouse, i.e. a map can contain two elements with the same key.
(The reason for that is that maps are internally implemented as `Array(Tuple(K, V))`.)

You can use use syntax `m[k]` to obtain the value for key `k` in map `m`.
Also, `m[k]` scans the map, i.e. the runtime of the operation is linear in the size of the map.

**Parameters**

- `K` — The type of the Map keys. Arbitrary type except [Nullable](../../sql-reference/data-types/nullable.md) and [LowCardinality](../../sql-reference/data-types/lowcardinality.md) nested with [Nullable](../../sql-reference/data-types/nullable.md) types.
- `V` — The type of the Map values. Arbitrary type.

**Examples**

Create a table with a column of type map:

```sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

To select `key2` values:

```sql
SELECT m['key2'] FROM tab;
```

Result:

```text
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

If the requested key `k` is not contained in the map, `m[k]` returns the value type's default value, e.g. `0` for integer types and `''` for string types.
To check whether a key exists in a map, you can use function [mapContains](/sql-reference/functions/tuple-map-functions#mapContainsKey).

```sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

Result:

```text
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

## Converting Tuple to Map {#converting-tuple-to-map}

Values of type `Tuple()` can be cast to values of type `Map()` using function [CAST](/sql-reference/functions/type-conversion-functions#CAST):

**Example**

Query:

```sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

Result:

```text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## Reading subcolumns of Map {#reading-subcolumns-of-map}

To avoid reading the entire map, you can use subcolumns `keys` and `values` in some cases.

**Example**

Query:

```sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

Result:

```text
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

## Bucketed Map Serialization in MergeTree {#bucketed-map-serialization}

By default, a `Map` column in MergeTree is stored as a single `Array(Tuple(K, V))` stream.
Reading a single key with `m['key']` requires scanning the entire column — every key-value pair for every row — even if only one key is needed.
For maps with many distinct keys this becomes a bottleneck.

Bucketed serialization (`with_buckets`) splits the key-value pairs into multiple independent substreams (buckets) by hashing the key.
When a query accesses `m['key']`, only the bucket that contains that key is read from disk, skipping all other buckets.

### Enabling Bucketed Serialization {#enabling-bucketed-serialization}

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

To avoid slowing down inserts, you can keep `basic` serialization for zero-level parts (created during `INSERT`) and only use `with_buckets` for merged parts:

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

### How It Works {#how-it-works}

When a data part is written with `with_buckets` serialization:

1. The average number of keys per row is computed from the block statistics.
2. The number of buckets is determined by the configured strategy (see [Settings](#bucketed-map-settings)).
3. Each key-value pair is assigned to a bucket by hashing the key: `bucket = hash(key) % num_buckets`.
4. Each bucket is stored as an independent substream with its own keys, values, and offsets.
5. A `buckets_info` metadata stream records the bucket count and statistics.

When a query reads a specific key (`m['key']`), the optimizer rewrites the expression to a key subcolumn (`m.key_<serialized_key>`).
The serialization layer computes which bucket the requested key belongs to and reads only that single bucket from disk.

When the full map is read (e.g., `SELECT m`), all buckets are read and reassembled into the original map. This is slower than `basic` serialization due to the overhead of reading and merging multiple substreams.

:::note
The order of keys within a map value may differ from the original insertion order when using `with_buckets` serialization. Keys are distributed across buckets by hash and are reassembled in bucket order, not insertion order. With `basic` serialization, the key order from inserted maps is preserved.
:::

The bucket count can vary between parts. When parts with different bucket counts are merged, the new part's bucket count is recalculated from the merged statistics. Parts with `basic` and `with_buckets` serialization can coexist in the same table and are merged transparently.

### Settings {#bucketed-map-settings}

| Setting | Default | Description |
|---------|---------|-------------|
| `map_serialization_version` | `basic` | Serialization format for `Map` columns. `basic` stores as a single array stream. `with_buckets` splits keys into buckets for faster single-key reads. |
| `map_serialization_version_for_zero_level_parts` | `basic` | Serialization format for zero-level parts (created by `INSERT`). Allows keeping `basic` for inserts to avoid write overhead, while merged parts use `with_buckets`. |
| `max_buckets_in_map` | `32` | Upper bound on the number of buckets. The actual count depends on `map_buckets_strategy`. The maximum allowed value is 256. |
| `map_buckets_strategy` | `sqrt` | Strategy for computing bucket count from average map size: `constant` — always use `max_buckets_in_map`; `sqrt` — use `round(coefficient * sqrt(avg_size))`; `linear` — use `round(coefficient * avg_size)`. Result is clamped to `[1, max_buckets_in_map]`. |
| `map_buckets_coefficient` | `1.0` | Multiplier for `sqrt` and `linear` strategies. Ignored when strategy is `constant`. |
| `map_buckets_min_avg_size` | `32` | Minimum average keys per row to enable bucketing. If the average is below this threshold, a single bucket is used regardless of other settings. Set to `0` to disable the threshold. |

### Performance Trade-offs {#performance-trade-offs}

The following table summarizes the performance impact of `with_buckets` compared to `basic` serialization at various map sizes (10 to 10,000 keys per row). The bucket count was determined by the `sqrt` strategy capped at 32. The exact numbers depend on key/value types, data distribution, and hardware.

| Operation | 10 keys | 100 keys | 1,000 keys | 10,000 keys | Notes |
|-----------|---------|----------|------------|-------------|-------|
| **Single key lookup** (`m['key']`) | 1.6–3.2x faster | 4.5–7.7x faster | 16–39x faster | 21–49x faster | Reads only one bucket instead of the entire column. |
| **5 key lookups** | ~1x | 1.5–3.1x faster | 2.9–8.3x faster | 4.5–6.7x faster | Each key reads its own bucket; some buckets may overlap. |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 1.5–3.0x faster | 2.9–7.3x faster | 5.3–31x faster | 20–45x faster | PREWHERE filter reads only one bucket; full map read only for matching rows. Speedup depends on selectivity — fewer matching granules means less full-map I/O. |
| **Full map scan** (`SELECT m`) | ~2x slower | ~2x slower | ~2x slower | ~2x slower | Must read and reassemble all buckets. |
| **INSERT** | 1.5–2.5x slower | 1.5–2.5x slower | 1.5–2.5x slower | 1.5–2.5x slower | Overhead of hashing keys and writing to multiple substreams. |

### Recommendations {#recommendations}

- **Small maps (< 32 keys on average):** Keep `basic` serialization. The overhead of bucketing is not justified for small maps. The default `map_buckets_min_avg_size = 32` enforces this automatically.
- **Medium maps (32–100 keys):** Use `with_buckets` with `sqrt` strategy if queries frequently access individual keys. The speedup is 4–8x for single-key lookups.
- **Large maps (100+ keys):** Use `with_buckets`. Single-key lookups are 16–49x faster. Consider `map_serialization_version_for_zero_level_parts = 'basic'` to keep insert speed close to the baseline.
- **Full map scans dominate the workload:** Keep `basic`. Bucketed serialization adds ~2x overhead for full scans.
- **Mixed workload (some key lookups, some full scans):** Use `with_buckets` with zero-level parts set to `basic`. The `PREWHERE` optimization reads only the relevant bucket for the filter, then reads the full map only for matching rows, giving a significant net speedup.

### Alternative Approaches {#map-alternatives}

If bucketed `Map` serialization does not fit your use case, there are two alternative approaches for improving key-level access performance:

#### Using the JSON Data Type {#using-the-json-data-type}

The [JSON](/sql-reference/data-types/newjson) data type stores each frequent path as a separate dynamic subcolumn. Paths that exceed the `max_dynamic_paths` limit go into a [shared data structure](/sql-reference/data-types/newjson#shared-data-structure), which can use `advanced` serialization for optimized single-path reads. See the [blog post](https://clickhouse.com/blog/json-data-type-gets-even-better) for a detailed overview of the `advanced` serialization.

| Aspect             | `Map` with buckets                                                                             | `JSON`                                                                                                                                                           |
|--------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Single key read    | Reads one bucket (may contain other keys). All key-value pairs in the bucket are deserialized. | Frequent paths are read directly from dynamic subcolumns. Infrequent paths go to shared data; with `advanced` serialization, only the exact path's data is read. |
| Value types        | All values share the same type `V`                                                             | Each path can have its own type. Paths without a type hint use `Dynamic`.                                                                                        |
| Skip index support | Works with some index types created on `mapKeys`/`mapValues`                                   | Skip indexes can only be created on specific path subcolumns, not on all paths/values at once.                                                                   |
| Full column read   | ~2x slower than `basic` due to bucket reassembly                                               | Overhead from `Dynamic` type encoding and path reconstruction.                                                                                                   |
| Storage overhead   | Minimal additional metadata                                                                    | Higher due to `Dynamic` type encoding, path name storage, and additional metadata in `advanced` serialization.                                                   |
| Schema flexibility | Fixed key and value types at table creation                                                    | Fully dynamic — keys and value types can vary per row. Typed path hints can be declared for known paths.                                                         |

Use `JSON` when different keys need different value types, when the set of keys varies significantly across rows, or when frequently accessed keys are known in advance and can be declared as typed paths for direct subcolumn access.

#### Manual Sharding into Multiple Map Columns {#manual-sharding-into-multiple-map-columns}

You can manually split a single `Map` into multiple columns by key hash at the application level:

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

During insertion, route each key-value pair to the column `m{hash(key) % 4}`. During queries, read from the specific column: `m{hash('target_key') % 4}['target_key']`.

| Aspect | `Map` with buckets | Manual sharding |
|--------|-------------------|-----------------|
| Ease of use | Transparent — handled by the storage engine | Requires application-level routing logic for inserts and selects |
| Vertical merge | Not supported — all buckets belong to one column | Supported — each `Map` column is an independent column and can be merged vertically |
| Schema changes | Bucket count adapts automatically per part | Changing the number of shards requires rewriting data or adding new columns |
| Query syntax | `m['key']` works directly | Must compute the correct column: `m0['key']`, `m1['key']`, etc. |
| Bucket granularity | Per-part, adapts to data statistics | Fixed at table creation |

Manual sharding is beneficial when vertical merges are important for reducing memory usage during merges of tables with many columns, or when the number of shards must be fixed and controlled explicitly. For most use cases, automatic bucketed serialization is simpler and sufficient.

**See Also**

- [map()](/sql-reference/functions/tuple-map-functions#map) function
- [CAST()](/sql-reference/functions/type-conversion-functions#CAST) function
- [-Map combinator for Map datatype](../aggregate-functions/combinators.md#-map)

## Related content {#related-content}

- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
