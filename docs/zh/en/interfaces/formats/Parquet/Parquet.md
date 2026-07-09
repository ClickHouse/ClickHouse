---
alias: []
description: 'Parquet 格式文档'
input_format: true
keywords: ['Parquet']
output_format: true
slug: /interfaces/formats/Parquet
title: 'Parquet'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

[Apache Parquet](https://parquet.apache.org/) 是一种在 Hadoop 生态系统中广泛使用的列式存储格式。ClickHouse 支持读写这种格式。

<div id="data-types-matching-parquet">
  ## 数据类型匹配
</div>

下表显示了 Parquet 数据类型与 ClickHouse [数据类型](/zh/sql-reference/data-types/index.md) 的对应关系。

| Parquet 类型 (logical、converted 或 physical)  | ClickHouse 数据类型                                                                            |
| ------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `BOOLEAN`                                  | [Bool](/zh/sql-reference/data-types/boolean.md)                                               |
| `UINT_8`                                   | [UInt8](/zh/sql-reference/data-types/int-uint.md)                                             |
| `INT_8`                                    | [Int8](/zh/sql-reference/data-types/int-uint.md)                                              |
| `UINT_16`                                  | [UInt16](/zh/sql-reference/data-types/int-uint.md)                                            |
| `INT_16`                                   | [Int16](/zh/sql-reference/data-types/int-uint.md)/[Enum16](/zh/sql-reference/data-types/enum.md) |
| `UINT_32`                                  | [UInt32](/zh/sql-reference/data-types/int-uint.md)                                            |
| `INT_32`                                   | [Int32](/zh/sql-reference/data-types/int-uint.md)                                             |
| `UINT_64`                                  | [UInt64](/zh/sql-reference/data-types/int-uint.md)                                            |
| `INT_64`                                   | [Int64](/zh/sql-reference/data-types/int-uint.md)                                             |
| `DATE`                                     | [Date32](/zh/sql-reference/data-types/date.md)                                                |
| `TIMESTAMP`, `TIME`                        | [DateTime64](/zh/sql-reference/data-types/datetime64.md)                                      |
| `FLOAT`                                    | [Float32](/zh/sql-reference/data-types/float.md)                                              |
| `DOUBLE`                                   | [Float64](/zh/sql-reference/data-types/float.md)                                              |
| `INT96`                                    | [DateTime64(9, &#39;UTC&#39;)](/zh/sql-reference/data-types/datetime64.md)                    |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON`       | [String](/zh/sql-reference/data-types/string.md)                                              |
| `JSON`                                     | [JSON](/zh/sql-reference/data-types/newjson.md)                                               |
| `FIXED_LEN_BYTE_ARRAY`                     | [FixedString](/zh/sql-reference/data-types/fixedstring.md)                                    |
| `DECIMAL`                                  | [Decimal](/zh/sql-reference/data-types/decimal.md)                                            |
| `LIST`                                     | [Array](/zh/sql-reference/data-types/array.md)                                                |
| `MAP`                                      | [Map](/zh/sql-reference/data-types/map.md)                                                    |
| struct                                     | [Tuple](/zh/sql-reference/data-types/tuple.md)                                                |
| `FLOAT16`                                  | [Float32](/zh/sql-reference/data-types/float.md)                                              |
| `UUID`                                     | [FixedString(16)](/zh/sql-reference/data-types/fixedstring.md)                                |
| `INTERVAL`                                 | [FixedString(12)](/zh/sql-reference/data-types/fixedstring.md)                                |
| `Point` (GeoParquet)                       | [Point](/zh/sql-reference/data-types/geo.md#point)                                            |
| `LineString` (GeoParquet)                  | [LineString](/zh/sql-reference/data-types/geo.md#linestring)                                  |
| `Polygon` (GeoParquet)                     | [Polygon](/zh/sql-reference/data-types/geo.md#polygon)                                        |
| `MultiLineString` (GeoParquet)             | [MultiLineString](/zh/sql-reference/data-types/geo.md#multilinestring)                        |
| `MultiPolygon` (GeoParquet)                | [MultiPolygon](/zh/sql-reference/data-types/geo.md#multipolygon)                              |
| 混合/未知几何类型 (GeoParquet)                     | [Geometry](/zh/sql-reference/data-types/geo.md#geometry)                                      |

写入 Parquet 文件时，没有对应 Parquet 类型的数据类型会被转换为最接近的可用类型：

| ClickHouse 数据类型                                                        | Parquet 类型                             |
| ---------------------------------------------------------------------- | -------------------------------------- |
| [IPv4](/zh/sql-reference/data-types/ipv4.md)                              | `UINT_32`                              |
| [IPv6](/zh/sql-reference/data-types/ipv6.md)                              | `FIXED_LEN_BYTE_ARRAY` (16 字节)         |
| [Date](/zh/sql-reference/data-types/date.md) (16 位)                       | `DATE` (32 位)                          |
| [DateTime](/zh/sql-reference/data-types/datetime.md) (32 位，秒)             | `TIMESTAMP` (64 位，毫秒)                  |
| [Int128/UInt128/Int256/UInt256](/zh/sql-reference/data-types/int-uint.md) | `FIXED_LEN_BYTE_ARRAY` (16/32 字节，小端序)  |
| [Point](/zh/sql-reference/data-types/geo.md#point)                        | `BYTE_ARRAY` (WKB)  + GeoParquet 元数据   |
| [LineString](/zh/sql-reference/data-types/geo.md#linestring)              | `BYTE_ARRAY` (WKB)  + GeoParquet 元数据   |
| [Polygon](/zh/sql-reference/data-types/geo.md#polygon)                    | `BYTE_ARRAY` (WKB)  + GeoParquet 元数据   |
| [MultiLineString](/zh/sql-reference/data-types/geo.md#multilinestring)    | `BYTE_ARRAY` (WKB)  + GeoParquet 元数据   |
| [MultiPolygon](/zh/sql-reference/data-types/geo.md#multipolygon)          | `BYTE_ARRAY` (WKB)  + GeoParquet 元数据   |

Array 类型可以嵌套，也可以接受 `Nullable` 类型的值作为参数。`Tuple` 和 `Map` 类型同样可以嵌套。

ClickHouse 表中列的数据类型可能与插入的 Parquet 数据中对应字段的类型不同。插入数据时，ClickHouse 会根据上表解析数据类型，然后将数据[转换](/zh/sql-reference/functions/type-conversion-functions#CAST)为该 ClickHouse 表列所设置的数据类型。例如，`UINT_32` Parquet 列可以读入 [IPv4](/zh/sql-reference/data-types/ipv4.md) ClickHouse 列中。

对于某些 Parquet 类型，没有与之紧密对应的 ClickHouse 类型。我们会按如下方式读取：

* `TIME` (一天中的时间) 会被读取为时间戳。例如，`10:23:13.000` 会变成 `1970-01-01 10:23:13.000`。
* 带有 `isAdjustedToUTC=false` 的 `TIMESTAMP`/`TIME` 表示本地挂钟时间 (即本地时区中的年、月、日、时、分、秒和亚秒字段，而不考虑具体哪个时区被视为本地) ，与 SQL `TIMESTAMP WITHOUT TIME ZONE` 相同。ClickHouse 会改为将其视作 UTC 时间戳读取。例如，`2025-09-29 18:42:13.000` (表示本地挂钟的读数) 会变成 `2025-09-29 18:42:13.000` (`DateTime64(3, 'UTC')`，表示一个时间点) 。如果将其转换为 `String`，会显示正确的年、月、日、时、分、秒和亚秒，之后即可将其解释为某个本地时区中的时间，而不是 UTC。比较反直觉的是，将类型从 `DateTime64(3, 'UTC')` 改为 `DateTime64(3)` 也无济于事，因为这两种类型表示的都是时间点，而不是时钟读数；但 `DateTime64(3)` 还会错误地按本地时区进行格式化。
* `INTERVAL` 当前会读取为 `FixedString(12)`，其中包含时间间隔的原始二进制表示，编码方式与 Parquet 文件中的存储方式一致。

<div id="geo-types">
  ## Geo 类型 (GeoParquet)
</div>

ClickHouse 支持按照 [GeoParquet](https://geoparquet.org/) 规范读写几何列。几何列存储为使用 [WKB](https://libgeos.org/specifications/wkb/) 编码的 `BYTE_ARRAY` 载荷 (读取时也可使用 WKT) ；文件级 Parquet 元数据中的 JSON `geo` 键用于描述每个几何列的编码、几何类型和 CRS。

<div id="read">
  ### 读取行为
</div>

读取时，几何列会映射为对应的 ClickHouse [Geo 数据类型](/zh/sql-reference/data-types/geo.md)：

* 声明为 `Point`、`LineString`、`Polygon`、`MultiLineString` 或 `MultiPolygon` 的列，会被读取为对应的 ClickHouse Geo 类型。
* 包含多种或未知 geometry types 的列，会被读取为 [`Geometry`](/zh/sql-reference/data-types/geo.md#geometry) 类型，它是涵盖所有受支持 Geo 类型的 `Variant`。
* 如果请求的列类型是 `String`，则会忽略 GeoParquet 元数据，并按原样返回原始编码的几何载荷——即 WKB 或 WKT 字节，具体取决于 GeoParquet 列声明的编码方式。若将设置 [`input_format_parquet_allow_geoparquet_parser`](/zh/operations/settings/settings-formats.md#input_format_parquet_allow_geoparquet_parser) 设为 `0`，也是如此。

<div id="write">
  ### 写入行为
</div>

写入时，顶层中类型为 `Point`、`LineString`、`Polygon`、`MultiLineString` 或 `MultiPolygon` 的列会被编码为 `BYTE_ARRAY` (WKB) ，并将相应的 `geo` JSON 元数据追加到 Parquet 文件的 footer 中。顶层的 [`Geometry`](/zh/sql-reference/data-types/geo.md#geometry) `Variant` 也会被编码为 WKB `BYTE_ARRAY` 载荷 (其子值会转换为 WKB，并存储为 `Nullable(String)` 列) ，但不会输出其 `geo` 元数据，因此读取时，结果不会被识别为 GeoParquet 几何列。其他与 geo 相关的类型，例如 [`Ring`](/zh/sql-reference/data-types/geo.md#ring)，会使用其原生底层表示进行写入，不附带任何 GeoParquet 元数据。将 [`output_format_parquet_geometadata`](/zh/operations/settings/settings-formats.md#output_format_parquet_geometadata) 设置为 `0` 即可完全禁用此行为；在这种情况下，即使是受支持的 geo 类型，也会使用其原生底层表示写入 (`Point` 写为 `Tuple(Float64, Float64)`，`LineString` 写为 `Array(Point)`，`Polygon` 写为 `Array(Array(Point))`，等等) ，并且不会输出任何 GeoParquet 元数据。

Geometry columns 必须位于 schema 的根层级，或嵌套在 `Tuple` (`struct`) 内部；不支持将其嵌套在 `Array` 或 `Map` 中。geo 列同样不支持 `Nullable`。

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用一个名为 `football.parquet`、包含以下数据的 Parquet 文件：

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

插入数据：

```sql
INSERT INTO football FROM INFILE 'football.parquet' FORMAT Parquet;
```

<div id="reading-data">
  ### 读取数据
</div>

使用 `Parquet` 格式读取数据：

```sql
SELECT *
FROM football
INTO OUTFILE 'football.parquet'
FORMAT Parquet
```

:::tip
Parquet 是一种二进制格式，无法以人类可读的形式在终端中显示。请使用 `INTO OUTFILE` 输出 Parquet 文件。
:::

如需与 Hadoop 交换数据，可以使用 [`HDFS 表引擎`](/zh/engines/table-engines/integrations/hdfs.md)。

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                             | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | 默认值                                                                                                                                                                                                                                                                       |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `input_format_parquet_case_insensitive_column_matching`                        | 匹配 Parquet 列与 CH columns 时忽略大小写。                                                                                                                                                                                                                                                                                                                                                                                                                                              | `0`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_preserve_order`                                          | 从 Parquet 文件读取时避免重排行顺序。通常会显著降低速度。                                                                                                                                                                                                                                                                                                                                                                                                                                             | `0`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_filter_push_down`                                        | 读取 Parquet 文件时，根据 Parquet 元数据中的 WHERE/PREWHERE 表达式以及最小/最大统计信息跳过整个行组。                                                                                                                                                                                                                                                                                                                                                                                                          | `1`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_bloom_filter_push_down`                                  | 读取 Parquet 文件时，根据 WHERE 表达式和 Parquet 元数据中的 bloom filter 跳过整个行组。                                                                                                                                                                                                                                                                                                                                                                                                               | `0`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_allow_missing_columns`                                   | 读取 Parquet 输入格式时允许列缺失                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `1`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | 在 Parquet 输入格式中，本地读取 (文件) 执行寻道而非读取并忽略时所需的最小字节数                                                                                                                                                                                                                                                                                                                                                                                                                                | `8192`                                                                                                                                                                                                                                                                    |
| `input_format_parquet_enable_row_group_prefetch`                               | 在 Parquet 解析期间启用行组预取。目前只有单线程解析支持预取。                                                                                                                                                                                                                                                                                                                                                                                                                                           | `1`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | 对 Parquet 格式进行 schema inference 时，跳过类型不受支持的列                                                                                                                                                                                                                                                                                                                                                                                                                                  | `0`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_max_block_size`                                          | Parquet reader 的最大块大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `65409`                                                                                                                                                                                                                                                                   |
| `input_format_parquet_prefer_block_bytes`                                      | Parquet reader 输出的平均块字节数                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `16744704`                                                                                                                                                                                                                                                                |
| `input_format_parquet_enable_json_parsing`                                     | 读取 Parquet files 时，将 JSON columns 解析为 ClickHouse JSON Column。                                                                                                                                                                                                                                                                                                                                                                                                                 | `1`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_allow_geoparquet_parser`                                 | 读取 Parquet 文件时，识别 GeoParquet 的 `geo` 元数据，并将几何列 (WKB 或 WKT，具体取决于该列声明的编码) 解码为 ClickHouse Geo 数据类型。若为 `0`，则几何列将以其原始物理表示 (`String`) 形式呈现。                                                                                                                                                                                                                                                                                                                                         | `1`                                                                                                                                                                                                                                                                       |
| `output_format_parquet_row_group_size`                                         | 目标行组大小 (按行数计) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `1000000`                                                                                                                                                                                                                                                                 |
| `output_format_parquet_row_group_size_bytes`                                   | 压缩前的目标行组大小，以字节为单位。                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `536870912`                                                                                                                                                                                                                                                               |
| `output_format_parquet_string_as_string`                                       | 对 String 类型列，使用 Parquet String 类型而不是 Binary。                                                                                                                                                                                                                                                                                                                                                                                                                                  | `1`                                                                                                                                                                                                                                                                       |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | 对 FixedString 列，使用 Parquet FIXED&#95;LEN&#95;BYTE&#95;ARRAY 类型而不是 Binary。                                                                                                                                                                                                                                                                                                                                                                                                     | `1`                                                                                                                                                                                                                                                                       |
| `output_format_parquet_compression_method`                                     | Parquet 输出格式的压缩方法。支持的编解码器包括：snappy、lz4、brotli、zstd、gzip、none (未压缩)                                                                                                                                                                                                                                                                                                                                                                                                            | `zstd`                                                                                                                                                                                                                                                                    |
| `output_format_parquet_parallel_encoding`                                      | 使用多个线程进行 Parquet 编码。                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `1`                                                                                                                                                                                                                                                                       |
| `output_format_parquet_data_page_size`                                         | 压缩前的目标页大小 (以字节为单位) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `1048576`                                                                                                                                                                                                                                                                 |
| `output_format_parquet_batch_size`                                             | 每处理这么多行就检查一次页大小。如果某些列的平均值大小超过几 KB，建议适当减小该值。                                                                                                                                                                                                                                                                                                                                                                                                                                   | `1024`                                                                                                                                                                                                                                                                    |
| `output_format_parquet_write_page_index`                                       | 支持将页索引写入 Parquet 文件。                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `1`                                                                                                                                                                                                                                                                       |
| `output_format_parquet_geometadata`                                            | 将 GeoParquet `geo` 元数据写入 Parquet 文件页脚，并将顶层 ClickHouse geo 列 ([`Point`](/zh/sql-reference/data-types/geo.md#point)、[`LineString`](/zh/sql-reference/data-types/geo.md#linestring)、[`Polygon`](/zh/sql-reference/data-types/geo.md#polygon)、[`MultiLineString`](/zh/sql-reference/data-types/geo.md#multilinestring)、[`MultiPolygon`](/zh/sql-reference/data-types/geo.md#multipolygon)) 编码为 WKB。如果为 `0`，则这些列会以其原生底层表示形式写入 (例如将 `Point` 写为 `Tuple(Float64, Float64)`) ，且不会生成任何 GeoParquet 元数据。 | `1`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_import_nested`                                           | 已废弃，无任何作用。                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `0`                                                                                                                                                                                                                                                                       |
| `input_format_parquet_local_time_as_utc`                                       | true                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | 确定对 isAdjustedToUTC=false 的 Parquet 时间戳进行 schema inference 时所使用的数据类型。若为 true：DateTime64(..., &#39;UTC&#39;)；若为 false：DateTime64(...)。这两种行为都不完全正确，因为 ClickHouse 没有用于本地挂钟时间的数据类型。虽然这有些违反直觉，但 &#39;true&#39; 可能是相对没那么不正确的选项，因为将 &#39;UTC&#39; 时间戳格式化为 String 时，会得到正确的本地时间表示。 |