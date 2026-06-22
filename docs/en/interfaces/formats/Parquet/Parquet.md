---
alias: []
description: 'Documentation for the Parquet format'
input_format: true
keywords: ['Parquet']
output_format: true
slug: /interfaces/formats/Parquet
title: 'Parquet'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

## Data types matching {#data-types-matching-parquet}

The table below shows how Parquet data types match ClickHouse [data types](/sql-reference/data-types/index.md).

| Parquet type (logical, converted, or physical) | ClickHouse data type |
|------------------------------------------------|----------------------|
| `BOOLEAN` | [Bool](/sql-reference/data-types/boolean.md) |
| `UINT_8` | [UInt8](/sql-reference/data-types/int-uint.md) |
| `INT_8` | [Int8](/sql-reference/data-types/int-uint.md) |
| `UINT_16` | [UInt16](/sql-reference/data-types/int-uint.md) |
| `INT_16` | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) |
| `UINT_32` | [UInt32](/sql-reference/data-types/int-uint.md) |
| `INT_32` | [Int32](/sql-reference/data-types/int-uint.md) |
| `UINT_64` | [UInt64](/sql-reference/data-types/int-uint.md) |
| `INT_64` | [Int64](/sql-reference/data-types/int-uint.md) |
| `DATE` | [Date32](/sql-reference/data-types/date.md) |
| `TIMESTAMP`, `TIME` | [DateTime64](/sql-reference/data-types/datetime64.md) |
| `FLOAT` | [Float32](/sql-reference/data-types/float.md) |
| `DOUBLE` | [Float64](/sql-reference/data-types/float.md) |
| `INT96` | [DateTime64(9, 'UTC')](/sql-reference/data-types/datetime64.md) |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON` | [String](/sql-reference/data-types/string.md) |
| `JSON` | [JSON](/sql-reference/data-types/newjson.md) |
| `VARIANT` | [JSON](/sql-reference/data-types/newjson.md) by default when `input_format_parquet_enable_json_parsing = 1`; ClickHouse type-hint metadata can preserve [Dynamic](/sql-reference/data-types/dynamic.md) for files written from `Dynamic`; can also be read into [String](/sql-reference/data-types/string.md) |
| `FIXED_LEN_BYTE_ARRAY` | [FixedString](/sql-reference/data-types/fixedstring.md) |
| `DECIMAL` | [Decimal](/sql-reference/data-types/decimal.md) |
| `LIST` | [Array](/sql-reference/data-types/array.md) |
| `MAP` | [Map](/sql-reference/data-types/map.md) |
| struct | [Tuple](/sql-reference/data-types/tuple.md) |
| `FLOAT16` | [Float32](/sql-reference/data-types/float.md) |
| `UUID` | [FixedString(16)](/sql-reference/data-types/fixedstring.md) |
| `INTERVAL` | [FixedString(12)](/sql-reference/data-types/fixedstring.md) |
| `Point` (GeoParquet) | [Point](/sql-reference/data-types/geo.md#point) |
| `LineString` (GeoParquet) | [LineString](/sql-reference/data-types/geo.md#linestring) |
| `Polygon` (GeoParquet) | [Polygon](/sql-reference/data-types/geo.md#polygon) |
| `MultiLineString` (GeoParquet) | [MultiLineString](/sql-reference/data-types/geo.md#multilinestring) |
| `MultiPolygon` (GeoParquet) | [MultiPolygon](/sql-reference/data-types/geo.md#multipolygon) |
| mixed/unknown geometry (GeoParquet) | [Geometry](/sql-reference/data-types/geo.md#geometry) |

When writing Parquet file, data types that don't have a matching Parquet type are converted to the nearest available type:

| ClickHouse data type | Parquet type |
|----------------------|--------------|
| [IPv4](/sql-reference/data-types/ipv4.md) | `UINT_32` |
| [IPv6](/sql-reference/data-types/ipv6.md) | `FIXED_LEN_BYTE_ARRAY` (16 bytes) |
| [Date](/sql-reference/data-types/date.md) (16 bits) | `DATE` (32 bits) |
| [DateTime](/sql-reference/data-types/datetime.md) (32 bits, seconds) | `TIMESTAMP` (64 bits, milliseconds) |
| [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md) | `FIXED_LEN_BYTE_ARRAY` (16/32 bytes, little-endian) |
| [Point](/sql-reference/data-types/geo.md#point) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [LineString](/sql-reference/data-types/geo.md#linestring) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [Polygon](/sql-reference/data-types/geo.md#polygon) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [MultiLineString](/sql-reference/data-types/geo.md#multilinestring) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [MultiPolygon](/sql-reference/data-types/geo.md#multipolygon) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |

Arrays can be nested and can have a value of `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to that data type which is set for the ClickHouse table column. E.g. a `UINT_32` Parquet column can be read into an [IPv4](/sql-reference/data-types/ipv4.md) ClickHouse column.

For some Parquet types there's no closely matching ClickHouse type. We read them as follows:
* `TIME` (time of day) is read as a timestamp. E.g. `10:23:13.000` becomes `1970-01-01 10:23:13.000`.
* `TIMESTAMP`/`TIME` with `isAdjustedToUTC=false` is a local wall-clock time (year, month, day, hour, minute, second and subsecond fields in a local timezone, regardless of what specific time zone is considered local), same as SQL `TIMESTAMP WITHOUT TIME ZONE`. ClickHouse reads it as if it were a UTC timestamp instead. E.g. `2025-09-29 18:42:13.000` (representing a reading of a local wall clock) becomes `2025-09-29 18:42:13.000` (`DateTime64(3, 'UTC')` representing a point in time). If converted to String, it shows the correct year, month, day, hour, minute, second and subsecond, which can then be interpreted as being in some local timezone instead of UTC. Counterintuitively, changing the type from `DateTime64(3, 'UTC')` to `DateTime64(3)` would not help as both types represent a point in time rather than a clock reading, but `DateTime64(3)` would incorrectly be formatted using local timezone.
* `INTERVAL` is currently read as `FixedString(12)` with raw binary representation of the time interval, as encoded in Parquet file.

## Parquet `VARIANT` {#parquet-variant}

ClickHouse can read and write Parquet columns annotated with the `VARIANT` logical type.

Parquet `VARIANT` is not the same thing as the ClickHouse [`Variant(T1, T2, ...)`](/sql-reference/data-types/variant.md) data type. Parquet `VARIANT` is meant for semi-structured JSON-like data, while ClickHouse `Variant(T1, T2, ...)` is for values that can have one of several pre-defined types.

### Schema inference {#parquet-variant-schema-inference}

When schema inference is used and the file has no ClickHouse type-hint metadata, a Parquet `VARIANT` column is inferred as:

* [`Dynamic`](/sql-reference/data-types/dynamic.md) if `input_format_parquet_enable_json_parsing = 0`.
* [`JSON`](/sql-reference/data-types/newjson.md) if `input_format_parquet_enable_json_parsing = 1`.

Files written by ClickHouse can store `ClickHouse.variant_type_hints` metadata. This metadata preserves whether a Parquet `VARIANT` column came from `Dynamic`, `JSON`, or a full `JSON(...)` type with declared paths and settings.

You can also read the same column into:

* [`JSON`](/sql-reference/data-types/newjson.md) or `Nullable(JSON)` if you want structured JSON output.
* [`String`](/sql-reference/data-types/string.md) or `Nullable(String)` if you want the JSON representation as text.

Accessing `Dynamic` or `JSON` subcolumns read from Parquet `VARIANT` through subqueries currently requires `enable_analyzer = 1`.

### Writing `Dynamic` and `JSON` {#parquet-variant-writing-dynamic-and-json}

ClickHouse writes:

* [`Dynamic`](/sql-reference/data-types/dynamic.md) columns as Parquet `VARIANT`.
* [`JSON`](/sql-reference/data-types/newjson.md) columns as Parquet `JSON` by default.
* [`JSON`](/sql-reference/data-types/newjson.md) columns as Parquet `VARIANT` if `output_format_parquet_json_as_variant = 1`.

When writing Parquet `VARIANT`, ClickHouse uses the Parquet `VARIANT` wrapper with `metadata` and `value` fields, and adds a `typed_value` field when it can shred stable structure out of the payload. This keeps the file compatible with readers that understand Parquet `VARIANT` while still preserving mixed or ambiguous values in the residual `value` field.

### `max_dynamic_paths` interaction {#parquet-variant-max-dynamic-paths}

If `output_format_parquet_json_as_variant = 1`, the `JSON(max_dynamic_paths=N, ...)` type parameter affects how much of the `JSON` structure is shredded into `typed_value`:

* Explicit typed paths declared in the `JSON(...)` type are always preserved in the shredded schema.
* Up to `N` additional inferred dynamic paths are selected for shredding.
* Remaining paths are kept in the residual `value` payload, so no data is lost.

The inferred dynamic paths are chosen by frequency in the written data, with path name used as a deterministic tie-breaker.

This is especially useful for semi-structured data:

* `JSON` objects can be written as Parquet `VARIANT` when `output_format_parquet_json_as_variant = 1`, and later read back as `JSON`, `Dynamic`, or `String`.
* `Dynamic` values with mixed scalars, arrays, tuples, and nested objects are preserved.
* Empty objects and arrays are preserved when reading through the native Parquet reader.

Example:

```sql
SET output_format_parquet_json_as_variant = 1;

INSERT INTO FUNCTION file('data.parquet', Parquet)
SELECT
    1 AS id,
    CAST('{"a":1,"c":["x",2],"extra":"keep"}' AS JSON) AS var;

SELECT id, toJSONString(var)
FROM file('data.parquet', Parquet, 'id UInt64, var JSON')
ORDER BY id;
```

## Geo types (GeoParquet) {#geo-types}

ClickHouse supports reading and writing geometry columns according to the [GeoParquet](https://geoparquet.org/) specification. Geometry columns are stored as `BYTE_ARRAY` payloads encoded in [WKB](https://libgeos.org/specifications/wkb/) (or WKT on read), with a JSON `geo` key in the file-level Parquet metadata describing each geometry column's encoding, geometry type and CRS.

### Read behavior {#read}

On read, geometry columns are mapped to the corresponding ClickHouse [geo data types](/sql-reference/data-types/geo.md):
* A column declared as `Point`, `LineString`, `Polygon`, `MultiLineString` or `MultiPolygon` is read into the matching ClickHouse geo type.
* A column with multiple or unknown geometry types is read into the [`Geometry`](/sql-reference/data-types/geo.md#geometry) type, which is a `Variant` over all supported geo types.
* If the requested column type is `String`, the GeoParquet metadata is ignored and the raw encoded geometry payload is returned as-is — WKB or WKT bytes, matching whichever encoding the GeoParquet column declares. This is also true if the setting [`input_format_parquet_allow_geoparquet_parser`](/operations/settings/settings-formats.md#input_format_parquet_allow_geoparquet_parser) is set to `0`.

### Write behavior {#write}

On write, top-level columns of type `Point`, `LineString`, `Polygon`, `MultiLineString` or `MultiPolygon` are encoded as `BYTE_ARRAY` (WKB) and the appropriate `geo` JSON metadata is appended to the Parquet file footer. A top-level [`Geometry`](/sql-reference/data-types/geo.md#geometry) `Variant` is also encoded as a WKB `BYTE_ARRAY` payload (its sub-values are converted to WKB and stored as a `Nullable(String)` column), but no `geo` metadata is emitted for it, so the result is not recognized as a GeoParquet geometry column on read. Other geo-related types, such as [`Ring`](/sql-reference/data-types/geo.md#ring), are written using their native underlying representation with no GeoParquet metadata. This behavior can be disabled entirely by setting [`output_format_parquet_geometadata`](/operations/settings/settings-formats.md#output_format_parquet_geometadata) to `0`, in which case even the supported geo types are written using their native underlying representation (`Point` as `Tuple(Float64, Float64)`, `LineString` as `Array(Point)`, `Polygon` as `Array(Array(Point))`, etc.) and no GeoParquet metadata is emitted.

Geometry columns must appear at the root of the schema or nested inside `Tuple` (`struct`); nesting them inside `Array` or `Map` is not supported. `Nullable` is not supported for geo columns either.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a Parquet file with the following data, named as `football.parquet`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.parquet' FORMAT Parquet;
```

### Reading data {#reading-data}

Read data using the `Parquet` format:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.parquet'
FORMAT Parquet
```

:::tip
Parquet is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output Parquet files.
:::

To exchange data with Hadoop, you can use the [`HDFS table engine`](/engines/table-engines/integrations/hdfs.md).

## Format settings {#format-settings}

| Setting                                                                        | Description                                                                                                                                                                                                                       | Default     |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_parquet_case_insensitive_column_matching`                        | Ignore case when matching Parquet columns with CH columns.                                                                                                                                                                          | `0`         |
| `input_format_parquet_preserve_order`                                          | Avoid reordering rows when reading from Parquet files. Usually makes it much slower.                                                                                                                                              | `0`         |
| `input_format_parquet_filter_push_down`                                        | When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.                                                                                          | `1`         |
| `input_format_parquet_bloom_filter_push_down`                                  | When reading Parquet files, skip whole row groups based on the WHERE expressions and bloom filter in the Parquet metadata.                                                                                                          | `0`         |
| `input_format_parquet_allow_missing_columns`                                   | Allow missing columns while reading Parquet input formats                                                                                                                                                                          | `1`         |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format                                                                                                                          | `8192`      |
| `input_format_parquet_enable_row_group_prefetch`                               | Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch.                                                                                                                          | `1`         |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Parquet                                                                                                                                                      | `0`         |
| `input_format_parquet_max_block_size`                                          | Max block size for parquet reader.                                                                                                                                                                                                | `65409`     |
| `input_format_parquet_prefer_block_bytes`                                      | Average block bytes output by parquet reader                                                                                                                                                                                      | `16744704`  |
| `input_format_parquet_enable_json_parsing`                                     | When reading Parquet files, parse Parquet `JSON` columns as ClickHouse `JSON`. Also makes schema inference read Parquet `VARIANT` columns as ClickHouse `JSON` instead of `Dynamic`.                                         | `1`         |
| `input_format_parquet_allow_geoparquet_parser`                                  | When reading Parquet files, recognize the GeoParquet `geo` metadata and decode geometry columns (WKB or WKT, per the column's declared encoding) as ClickHouse geo data types. If `0`, geometry columns are exposed as their raw physical (`String`) representation.                                                                                                                                              | `1`         |
| `output_format_parquet_row_group_size`                                         | Target row group size in rows.                                                                                                                                                                                                      | `1000000`   |
| `output_format_parquet_row_group_size_bytes`                                   | Target row group size in bytes, before compression.                                                                                                                                                                                  | `536870912` |
| `output_format_parquet_string_as_string`                                       | Use Parquet String type instead of Binary for String columns.                                                                                                                                                                      | `1`         |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | Use Parquet FIXED_LEN_BYTE_ARRAY type instead of Binary for FixedString columns.                                                                                                                                                  | `1`         |
| `output_format_parquet_compression_method`                                     | Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)                                                                                                              | `zstd`      |
| `output_format_parquet_json_as_variant`                                        | Write ClickHouse `JSON` columns as Parquet `VARIANT` instead of Parquet `JSON`. `Dynamic` columns are always written as Parquet `VARIANT`.                                                                                       | `0`         |
| `output_format_parquet_parallel_encoding`                                      | Do Parquet encoding in multiple threads.                                                                                                                                          | `1`         |
| `output_format_parquet_data_page_size`                                         | Target page size in bytes, before compression.                                                                                                                                                                                      | `1048576`   |
| `output_format_parquet_batch_size`                                             | Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.                                                                                                              | `1024`      |
| `output_format_parquet_write_page_index`                                       | Add a possibility to write page index into parquet files.                                                                                                                                                                          | `1`         |
| `output_format_parquet_geometadata`                                            | Write GeoParquet `geo` metadata into the Parquet file footer and encode top-level ClickHouse geo columns ([`Point`](/sql-reference/data-types/geo.md#point), [`LineString`](/sql-reference/data-types/geo.md#linestring), [`Polygon`](/sql-reference/data-types/geo.md#polygon), [`MultiLineString`](/sql-reference/data-types/geo.md#multilinestring), [`MultiPolygon`](/sql-reference/data-types/geo.md#multipolygon)) as WKB. If `0`, those columns are written using their native underlying representation (e.g. `Point` as `Tuple(Float64, Float64)`) and no GeoParquet metadata is emitted.                                                                                                                                                                          | `1`         |
| `input_format_parquet_import_nested`                                           | Obsolete setting, does nothing.                                                                                                                                                                                                   | `0`         |
| `input_format_parquet_local_time_as_utc` | true | Determines the data type used by schema inference for Parquet timestamps with isAdjustedToUTC=false. If true: DateTime64(..., 'UTC'), if false: DateTime64(...). Neither behavior is fully correct as ClickHouse doesn't have a data type for local wall-clock time. Counterintuitively, 'true' is probably the less incorrect option, because formatting the 'UTC' timestamp as String will produce representation of the correct local time. |
