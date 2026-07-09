---
alias: []
description: 'Parquet 포맷 문서'
input_format: true
keywords: ['Parquet']
output_format: true
slug: /interfaces/formats/Parquet
title: 'Parquet'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[Apache Parquet](https://parquet.apache.org/)은 Hadoop 생태계에서 널리 사용되는 열 지향 저장 포맷입니다. ClickHouse는 이 포맷의 읽기 및 쓰기를 지원합니다.

<div id="data-types-matching-parquet">
  ## 데이터 타입 매핑
</div>

아래 표는 Parquet 데이터 타입이 ClickHouse [데이터 타입](/ko/sql-reference/data-types/index.md)에 어떻게 매핑되는지 보여줍니다.

| Parquet 유형(논리, 변환 또는 물리)             | ClickHouse 데이터 타입                                                                          |
| ------------------------------------ | ------------------------------------------------------------------------------------------ |
| `BOOLEAN`                            | [Bool](/ko/sql-reference/data-types/boolean.md)                                               |
| `UINT_8`                             | [UInt8](/ko/sql-reference/data-types/int-uint.md)                                             |
| `INT_8`                              | [Int8](/ko/sql-reference/data-types/int-uint.md)                                              |
| `UINT_16`                            | [UInt16](/ko/sql-reference/data-types/int-uint.md)                                            |
| `INT_16`                             | [Int16](/ko/sql-reference/data-types/int-uint.md)/[Enum16](/ko/sql-reference/data-types/enum.md) |
| `UINT_32`                            | [UInt32](/ko/sql-reference/data-types/int-uint.md)                                            |
| `INT_32`                             | [Int32](/ko/sql-reference/data-types/int-uint.md)                                             |
| `UINT_64`                            | [UInt64](/ko/sql-reference/data-types/int-uint.md)                                            |
| `INT_64`                             | [Int64](/ko/sql-reference/data-types/int-uint.md)                                             |
| `DATE`                               | [Date32](/ko/sql-reference/data-types/date.md)                                                |
| `TIMESTAMP`, `TIME`                  | [DateTime64](/ko/sql-reference/data-types/datetime64.md)                                      |
| `FLOAT`                              | [Float32](/ko/sql-reference/data-types/float.md)                                              |
| `DOUBLE`                             | [Float64](/ko/sql-reference/data-types/float.md)                                              |
| `INT96`                              | [DateTime64(9, &#39;UTC&#39;)](/ko/sql-reference/data-types/datetime64.md)                    |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON` | [String](/ko/sql-reference/data-types/string.md)                                              |
| `JSON`                               | [JSON](/ko/sql-reference/data-types/newjson.md)                                               |
| `FIXED_LEN_BYTE_ARRAY`               | [FixedString](/ko/sql-reference/data-types/fixedstring.md)                                    |
| `DECIMAL`                            | [Decimal](/ko/sql-reference/data-types/decimal.md)                                            |
| `LIST`                               | [Array](/ko/sql-reference/data-types/array.md)                                                |
| `MAP`                                | [Map](/ko/sql-reference/data-types/map.md)                                                    |
| struct                               | [Tuple](/ko/sql-reference/data-types/tuple.md)                                                |
| `FLOAT16`                            | [Float32](/ko/sql-reference/data-types/float.md)                                              |
| `UUID`                               | [FixedString(16)](/ko/sql-reference/data-types/fixedstring.md)                                |
| `INTERVAL`                           | [FixedString(12)](/ko/sql-reference/data-types/fixedstring.md)                                |
| `Point` (GeoParquet)                 | [Point](/ko/sql-reference/data-types/geo.md#point)                                            |
| `LineString` (GeoParquet)            | [LineString](/ko/sql-reference/data-types/geo.md#linestring)                                  |
| `Polygon` (GeoParquet)               | [Polygon](/ko/sql-reference/data-types/geo.md#polygon)                                        |
| `MultiLineString` (GeoParquet)       | [MultiLineString](/ko/sql-reference/data-types/geo.md#multilinestring)                        |
| `MultiPolygon` (GeoParquet)          | [MultiPolygon](/ko/sql-reference/data-types/geo.md#multipolygon)                              |
| 혼합/알 수 없는 지오메트리 (GeoParquet)         | [Geometry](/ko/sql-reference/data-types/geo.md#geometry)                                      |

Parquet 파일을 쓸 때, 대응되는 Parquet 타입이 없는 데이터 타입은 가장 가까운 사용 가능한 타입으로 변환됩니다:

| ClickHouse 데이터 타입                                                      | Parquet 타입                                |
| ---------------------------------------------------------------------- | ----------------------------------------- |
| [IPv4](/ko/sql-reference/data-types/ipv4.md)                              | `UINT_32`                                 |
| [IPv6](/ko/sql-reference/data-types/ipv6.md)                              | `FIXED_LEN_BYTE_ARRAY` (16바이트)            |
| [Date](/ko/sql-reference/data-types/date.md) (16비트)                       | `DATE` (32비트)                             |
| [DateTime](/ko/sql-reference/data-types/datetime.md) (32비트, 초)            | `TIMESTAMP` (64비트, 밀리초)                   |
| [Int128/UInt128/Int256/UInt256](/ko/sql-reference/data-types/int-uint.md) | `FIXED_LEN_BYTE_ARRAY` (16/32바이트, 리틀 엔디언) |
| [Point](/ko/sql-reference/data-types/geo.md#point)                        | `BYTE_ARRAY` (WKB) + GeoParquet 메타데이터     |
| [LineString](/ko/sql-reference/data-types/geo.md#linestring)              | `BYTE_ARRAY` (WKB) + GeoParquet 메타데이터     |
| [Polygon](/ko/sql-reference/data-types/geo.md#polygon)                    | `BYTE_ARRAY` (WKB) + GeoParquet 메타데이터     |
| [MultiLineString](/ko/sql-reference/data-types/geo.md#multilinestring)    | `BYTE_ARRAY` (WKB) + GeoParquet 메타데이터     |
| [MultiPolygon](/ko/sql-reference/data-types/geo.md#multipolygon)          | `BYTE_ARRAY` (WKB) + GeoParquet 메타데이터     |

배열은 중첩할 수 있으며, 인수로 `Nullable` 타입의 값을 가질 수도 있습니다. `Tuple` 및 `Map` 타입도 중첩할 수 있습니다.

ClickHouse 테이블 컬럼의 데이터 타입은 삽입되는 Parquet 데이터의 해당 필드와 다를 수 있습니다. 데이터를 삽입할 때 ClickHouse는 위 표에 따라 데이터 타입을 해석한 다음, 데이터를 ClickHouse 테이블 컬럼에 설정된 데이터 타입으로 [형 변환](/ko/sql-reference/functions/type-conversion-functions#CAST)합니다. 예를 들어 `UINT_32` Parquet 컬럼은 [IPv4](/ko/sql-reference/data-types/ipv4.md) ClickHouse 컬럼으로 읽을 수 있습니다.

일부 Parquet 타입에는 가깝게 대응하는 ClickHouse 타입이 없습니다. 이러한 타입은 다음과 같이 읽습니다.

* `TIME` (하루 중 시간)은 타임스탬프로 읽습니다. 예를 들어 `10:23:13.000`은 `1970-01-01 10:23:13.000`이 됩니다.
* `isAdjustedToUTC=false`인 `TIMESTAMP`/`TIME`은 로컬 wall-clock time입니다(어떤 특정 시간대를 로컬로 간주하는지와 관계없이, 로컬 시간대의 연, 월, 일, 시, 분, 초 및 소수 초 필드). 이는 SQL `TIMESTAMP WITHOUT TIME ZONE`과 같습니다. ClickHouse는 이를 대신 UTC 타임스탬프인 것처럼 읽습니다. 예를 들어 `2025-09-29 18:42:13.000`(로컬 시계의 표시값을 나타냄)은 `2025-09-29 18:42:13.000`(특정 시점을 나타내는 `DateTime64(3, 'UTC')`)이 됩니다. 이를 `String`으로 변환하면 연, 월, 일, 시, 분, 초 및 소수 초가 올바르게 표시되므로, 이후 이를 UTC가 아닌 어떤 로컬 시간대의 값으로 해석할 수 있습니다. 직관에 반할 수 있지만 타입을 `DateTime64(3, 'UTC')`에서 `DateTime64(3)`로 변경해도 도움이 되지 않습니다. 두 타입 모두 시계의 표시값이 아니라 특정 시점을 나타내며, `DateTime64(3)`는 로컬 시간대를 사용해 잘못 포맷되기 때문입니다.
* `INTERVAL`은 현재 Parquet 파일에 인코딩된 시간 인터벌의 원시 이진 표현을 담은 `FixedString(12)`로 읽습니다.

<div id="geo-types">
  ## Geo 타입 (GeoParquet)
</div>

ClickHouse는 [GeoParquet](https://geoparquet.org/) 사양을 준수하여 지오메트리 컬럼을 읽고 쓸 수 있습니다. 지오메트리 컬럼은 [WKB](https://libgeos.org/specifications/wkb/)로 인코딩된 `BYTE_ARRAY` 페이로드로 저장되며(읽을 때는 WKT 사용), 파일 수준의 Parquet 메타데이터에 있는 JSON `geo` 키에는 각 지오메트리 컬럼의 인코딩, 지오메트리 타입, CRS가 설명됩니다.

<div id="read">
  ### 읽기 동작
</div>

읽을 때 지오메트리 컬럼은 해당하는 ClickHouse [geo 데이터 타입](/ko/sql-reference/data-types/geo.md)에 매핑됩니다:

* `Point`, `LineString`, `Polygon`, `MultiLineString` 또는 `MultiPolygon`으로 선언된 컬럼은 해당 ClickHouse geo 타입으로 읽힙니다.
* 여러 개이거나 알 수 없는 지오메트리 타입을 가진 컬럼은 지원되는 모든 geo 타입을 포괄하는 `Variant`인 [`Geometry`](/ko/sql-reference/data-types/geo.md#geometry) 타입으로 읽힙니다.
* 요청한 컬럼 타입이 `String`이면 GeoParquet 메타데이터는 무시되고, 원시 인코딩된 지오메트리 페이로드가 그대로 반환됩니다 — GeoParquet 컬럼에 선언된 인코딩에 따라 WKB 또는 WKT 바이트가 반환됩니다. 이는 설정 [`input_format_parquet_allow_geoparquet_parser`](/ko/operations/settings/settings-formats.md#input_format_parquet_allow_geoparquet_parser)를 `0`으로 설정한 경우에도 동일합니다.

<div id="write">
  ### 쓰기 동작
</div>

쓰기 시 `Point`, `LineString`, `Polygon`, `MultiLineString` 또는 `MultiPolygon` 타입의 최상위 컬럼은 `BYTE_ARRAY`(WKB)로 인코딩되며, 해당 `geo` JSON 메타데이터가 Parquet 파일 footer에 추가됩니다. 최상위 [`Geometry`](/ko/sql-reference/data-types/geo.md#geometry) `Variant`도 WKB `BYTE_ARRAY` 페이로드로 인코딩되지만(하위 값은 WKB로 변환되어 `Nullable(String)` 컬럼으로 저장됨), 이에 대해서는 `geo` 메타데이터가 출력되지 않으므로 읽을 때 GeoParquet 지오메트리 컬럼으로 인식되지 않습니다. [`Ring`](/ko/sql-reference/data-types/geo.md#ring)과 같은 다른 geo 관련 타입은 GeoParquet 메타데이터 없이 해당 네이티브 내부 표현으로 기록됩니다. 이 동작은 [`output_format_parquet_geometadata`](/ko/operations/settings/settings-formats.md#output_format_parquet_geometadata)를 `0`으로 설정해 완전히 비활성화할 수 있으며, 이 경우 지원되는 geo 타입도 네이티브 내부 표현(`Point`는 `Tuple(Float64, Float64)`, `LineString`은 `Array(Point)`, `Polygon`은 `Array(Array(Point))` 등)으로 기록되고 GeoParquet 메타데이터도 출력되지 않습니다.

지오메트리 컬럼은 스키마의 루트에 있거나 `Tuple`(`struct`) 내부에 중첩되어 있어야 하며, `Array` 또는 `Map` 내부에 중첩하는 것은 지원되지 않습니다. geo 컬럼에는 `Nullable`도 지원되지 않습니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

다음 데이터가 포함된 Parquet 파일 `football.parquet`를 사용합니다:

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

데이터를 삽입하세요:

```sql
INSERT INTO football FROM INFILE 'football.parquet' FORMAT Parquet;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

`Parquet` 포맷으로 데이터를 읽습니다:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.parquet'
FORMAT Parquet
```

:::tip
Parquet는 바이너리 형식이므로 터미널에서 사람이 읽을 수 있는 형태로 표시되지 않습니다. Parquet 파일로 출력하려면 `INTO OUTFILE`을 사용하세요.
:::

Hadoop와 데이터를 주고받으려면 [`HDFS 테이블 엔진`](/ko/engines/table-engines/integrations/hdfs.md)을 사용할 수 있습니다.

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                                             | 설명                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 기본값                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `input_format_parquet_case_insensitive_column_matching`                        | Parquet 컬럼을 CH 컬럼과 매칭할 때 대소문자를 구분하지 않습니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `0`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_preserve_order`                                          | Parquet 파일을 읽을 때 행 순서를 재정렬하지 않습니다. 일반적으로 속도가 훨씬 느려집니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `0`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_filter_push_down`                                        | Parquet 파일을 읽을 때 Parquet metadata의 WHERE/PREWHERE 표현식과 최소/최대 통계를 기반으로 전체 row group을 건너뜁니다.                                                                                                                                                                                                                                                                                                                                                                                                                 | `1`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_bloom_filter_push_down`                                  | Parquet 파일을 읽을 때 Parquet 메타데이터의 WHERE 표현식과 블룸 필터를 기반으로 전체 row group을 건너뜁니다.                                                                                                                                                                                                                                                                                                                                                                                                                                | `0`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_allow_missing_columns`                                   | Parquet 입력 형식을 읽는 동안 누락된 컬럼을 허용합니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `1`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Parquet 입력 형식에서 건너뛰며 읽는 대신 seek를 수행하는 데 필요한 로컬 읽기(파일)의 최소 바이트 수입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                        | `8192`                                                                                                                                                                                                                                                                                                                        |
| `input_format_parquet_enable_row_group_prefetch`                               | Parquet 파싱 중 row group 프리페치를 활성화합니다. 현재는 단일 스레드 파싱에서만 프리페치를 사용할 수 있습니다.                                                                                                                                                                                                                                                                                                                                                                                                                                    | `1`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Parquet 포맷의 스키마 추론 중 지원되지 않는 타입의 컬럼을 건너뜁니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `0`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_max_block_size`                                          | Parquet 리더의 최대 블록 크기입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `65409`                                                                                                                                                                                                                                                                                                                       |
| `input_format_parquet_prefer_block_bytes`                                      | Parquet 리더가 출력하는 평균 블록 바이트 수입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `16744704`                                                                                                                                                                                                                                                                                                                    |
| `input_format_parquet_enable_json_parsing`                                     | Parquet 파일을 읽을 때 JSON 컬럼을 ClickHouse JSON 컬럼으로 파싱합니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `1`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_allow_geoparquet_parser`                                 | Parquet 파일을 읽을 때 GeoParquet `geo` 메타데이터를 인식하고, 지오메트리 컬럼을 (컬럼에 선언된 인코딩에 따라 WKB 또는 WKT) ClickHouse Geo 데이터 타입으로 디코딩합니다. `0`이면 지오메트리 컬럼은 원시 물리적 (`String`) 표현으로 그대로 노출됩니다.                                                                                                                                                                                                                                                                                                                                    | `1`                                                                                                                                                                                                                                                                                                                           |
| `output_format_parquet_row_group_size`                                         | 목표 row group 크기(행 수 기준)입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `1000000`                                                                                                                                                                                                                                                                                                                     |
| `output_format_parquet_row_group_size_bytes`                                   | 압축 전 바이트 단위의 목표 row group 크기입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `536870912`                                                                                                                                                                                                                                                                                                                   |
| `output_format_parquet_string_as_string`                                       | String 컬럼에 Binary 대신 Parquet String 유형을 사용합니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `1`                                                                                                                                                                                                                                                                                                                           |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | FixedString 컬럼에 Binary 대신 Parquet FIXED&#95;LEN&#95;BYTE&#95;ARRAY 유형을 사용합니다.                                                                                                                                                                                                                                                                                                                                                                                                                              | `1`                                                                                                                                                                                                                                                                                                                           |
| `output_format_parquet_compression_method`                                     | Parquet 출력 형식에 사용할 압축 방식입니다. 지원되는 코덱: snappy, lz4, brotli, zstd, gzip, none (압축되지 않음)                                                                                                                                                                                                                                                                                                                                                                                                                      | `zstd`                                                                                                                                                                                                                                                                                                                        |
| `output_format_parquet_parallel_encoding`                                      | 여러 스레드로 Parquet 인코딩을 수행합니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `1`                                                                                                                                                                                                                                                                                                                           |
| `output_format_parquet_data_page_size`                                         | 압축 전 목표 페이지 크기(바이트)입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `1048576`                                                                                                                                                                                                                                                                                                                     |
| `output_format_parquet_batch_size`                                             | 지정한 행 수마다 페이지 크기를 확인합니다. 평균 값 크기가 몇 KB를 넘는 컬럼이 있으면 값을 줄이는 것이 좋습니다.                                                                                                                                                                                                                                                                                                                                                                                                                                         | `1024`                                                                                                                                                                                                                                                                                                                        |
| `output_format_parquet_write_page_index`                                       | Parquet 파일에 페이지 인덱스를 쓸 수 있도록 합니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `1`                                                                                                                                                                                                                                                                                                                           |
| `output_format_parquet_geometadata`                                            | GeoParquet `geo` 메타데이터를 Parquet file footer에 기록하고, 최상위 ClickHouse geo 컬럼([`Point`](/ko/sql-reference/data-types/geo.md#point), [`LineString`](/ko/sql-reference/data-types/geo.md#linestring), [`Polygon`](/ko/sql-reference/data-types/geo.md#polygon), [`MultiLineString`](/ko/sql-reference/data-types/geo.md#multilinestring), [`MultiPolygon`](/ko/sql-reference/data-types/geo.md#multipolygon))을 WKB로 인코딩합니다. `0`이면 해당 컬럼은 네이티브 내부 표현(예: `Point`를 `Tuple(Float64, Float64)`로)으로 기록되며, GeoParquet 메타데이터는 기록되지 않습니다. | `1`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_import_nested`                                           | 더 이상 사용되지 않는 설정으로, 아무 동작도 하지 않습니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `0`                                                                                                                                                                                                                                                                                                                           |
| `input_format_parquet_local_time_as_utc`                                       | true                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | isAdjustedToUTC=false인 Parquet 타임스탬프에 대해 스키마 추론에서 사용할 데이터 타입을 결정합니다. true이면 DateTime64(..., &#39;UTC&#39;), false이면 DateTime64(...)가 사용됩니다. ClickHouse에는 로컬 wall-clock time에 해당하는 데이터 타입이 없으므로 어느 쪽도 완전히 올바르지는 않습니다. 직관과 다르게 &#39;true&#39;가 아마도 더 덜 부정확한 선택입니다. &#39;UTC&#39; 타임스탬프를 String으로 포맷하면 올바른 로컬 시간 표현이 생성되기 때문입니다. |