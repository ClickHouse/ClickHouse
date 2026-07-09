---
alias: []
description: 'Arrow 형식 문서'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[Apache Arrow](https://arrow.apache.org/)는 기본적으로 2가지 열 지향 저장 포맷을 제공합니다.
ClickHouse는 이러한 포맷의 읽기와 쓰기를 지원합니다.
`Arrow`는 메모리 내 임의 액세스를 위해 설계된 Apache Arrow의 &quot;file mode&quot; 포맷입니다.

<div id="data-types-matching">
  ## 데이터 타입 매핑
</div>

아래 표는 지원되는 데이터 타입과 `INSERT` 및 `SELECT` 쿼리에서 ClickHouse [데이터 타입](/ko/sql-reference/data-types/index.md)에 어떻게 매핑되는지를 보여줍니다.

| Arrow 데이터 타입 (`INSERT`)                 | ClickHouse 데이터 타입                                                                          | Arrow 데이터 타입 (`SELECT`) |
| --------------------------------------- | ------------------------------------------------------------------------------------------ | ----------------------- |
| `BOOL`                                  | [Bool](/ko/sql-reference/data-types/boolean.md)                                               | `BOOL`                  |
| `UINT8`, `BOOL`                         | [UInt8](/ko/sql-reference/data-types/int-uint.md)                                             | `UINT8`                 |
| `INT8`                                  | [Int8](/ko/sql-reference/data-types/int-uint.md)/[Enum8](/ko/sql-reference/data-types/enum.md)   | `INT8`                  |
| `UINT16`                                | [UInt16](/ko/sql-reference/data-types/int-uint.md)                                            | `UINT16`                |
| `INT16`                                 | [Int16](/ko/sql-reference/data-types/int-uint.md)/[Enum16](/ko/sql-reference/data-types/enum.md) | `INT16`                 |
| `UINT32`                                | [UInt32](/ko/sql-reference/data-types/int-uint.md)                                            | `UINT32`                |
| `INT32`                                 | [Int32](/ko/sql-reference/data-types/int-uint.md)                                             | `INT32`                 |
| `UINT64`                                | [UInt64](/ko/sql-reference/data-types/int-uint.md)                                            | `UINT64`                |
| `INT64`                                 | [Int64](/ko/sql-reference/data-types/int-uint.md)                                             | `INT64`                 |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/ko/sql-reference/data-types/float.md)                                              | `FLOAT32`               |
| `DOUBLE`                                | [Float64](/ko/sql-reference/data-types/float.md)                                              | `FLOAT64`               |
| `DATE32`                                | [Date32](/ko/sql-reference/data-types/date32.md)                                              | `UINT16`                |
| `DATE64`                                | [DateTime](/ko/sql-reference/data-types/datetime.md)                                          | `UINT32`                |
| `TIMESTAMP`                             | [DateTime64](/ko/sql-reference/data-types/datetime64.md)                                      | `TIMESTAMP`             |
| `TIME32`, `TIME64`                      | [Time64](/ko/sql-reference/data-types/time64.md)                                              | `TIME32`, `TIME64`      |
| `STRING`, `BINARY`                      | [String](/ko/sql-reference/data-types/string.md)                                              | `BINARY`                |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/ko/sql-reference/data-types/fixedstring.md)                                    | `FIXED_SIZE_BINARY`     |
| `DECIMAL`                               | [Decimal](/ko/sql-reference/data-types/decimal.md)                                            | `DECIMAL`               |
| `DECIMAL256`                            | [Decimal256](/ko/sql-reference/data-types/decimal.md)                                         | `DECIMAL256`            |
| `LIST`                                  | [Array](/ko/sql-reference/data-types/array.md)                                                | `LIST`                  |
| `STRUCT`                                | [Tuple](/ko/sql-reference/data-types/tuple.md)                                                | `STRUCT`                |
| `MAP`                                   | [Map](/ko/sql-reference/data-types/map.md)                                                    | `MAP`                   |
| `UINT32`                                | [IPv4](/ko/sql-reference/data-types/ipv4.md)                                                  | `UINT32`                |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/ko/sql-reference/data-types/ipv6.md)                                                  | `FIXED_SIZE_BINARY`     |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/ko/sql-reference/data-types/int-uint.md)                     | `FIXED_SIZE_BINARY`     |
| `DURATION`                              | [Interval](/ko/sql-reference/data-types/special-data-types/interval.md) (나노초/마이크로초/밀리초/초)     | `DURATION`              |
| `INT64`                                 | [Interval](/ko/sql-reference/data-types/special-data-types/interval.md) (분/시간/일/주/월/분기/년)     | `INT64`                 |

배열은 중첩할 수 있으며, 인수로 `Nullable` 타입 값을 사용할 수 있습니다. `Tuple` 및 `Map` 타입도 중첩할 수 있습니다.

`DICTIONARY` 타입은 `INSERT` 쿼리에서 지원되며, `SELECT` 쿼리에서는 [`output_format_arrow_low_cardinality_as_dictionary`](/ko/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) 설정을 통해 [LowCardinality](/ko/sql-reference/data-types/lowcardinality.md) 타입을 `DICTIONARY` 타입으로 출력할 수 있습니다. 출력 시 `LowCardinality` 딕셔너리에 사용되지 않는 값이 있을 수 있으며, 이로 인해 Arrow `DICTIONARY`에도 사용되지 않는 값이 포함될 수 있습니다.

지원되지 않는 Arrow 데이터 타입:

* `FIXED_SIZE_BINARY`
* `JSON`
* `UUID`
* `ENUM`.

ClickHouse 테이블 컬럼의 데이터 타입은 해당 Arrow 데이터 필드와 일치할 필요가 없습니다. 데이터를 삽입할 때 ClickHouse는 위 표에 따라 데이터 타입을 해석한 다음, 데이터를 ClickHouse 테이블 컬럼에 설정된 데이터 타입으로 [형변환합니다](/ko/sql-reference/functions/type-conversion-functions#CAST).

<div id="example-usage">
  ## 사용 예시
</div>

아래 예시에서는 [ClickHouse SQL playground](https://sql.clickhouse.com)에서 제공되는 `forex` 데이터셋을 사용합니다.

<div id="selecting-data">
  ### 데이터 선택
</div>

Playground에서 `EUR/USD` 환율 하루치 데이터를 선택해 로컬 `forex_eurusd.arrow` 파일에 저장합니다. HTTP
인터페이스를 통해 Playground에 쿼리하며, 이때 host는 `sql-clickhouse.clickhouse.com`이고 사용자는
`demo`입니다(비밀번호는 설정되지 않음):

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### 파일 다시 읽기
</div>

이제 [`clickhouse-local`](/ko/operations/utilities/clickhouse-local)과
[`file`](/ko/sql-reference/table-functions/file) 테이블 함수를 사용하여
로컬 Arrow 파일을 다시 읽을 수 있습니다. 파일 자체에 구조 정보가 포함되어 있으므로
`Arrow` 포맷이 스키마(schema)를 자동으로 추론합니다:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### 데이터 삽입
</div>

Arrow 파일을 ClickHouse 테이블에 로드하려면 `FORMAT Arrow`를 사용하여
`clickhouse-client`로 파이프하십시오:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## 포맷 설정
</div>

| Setting                                                                      | Description                                                         | Default     |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | Arrow 입력 형식을 읽을 때 누락된 컬럼을 허용합니다                                     | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Arrow 컬럼을 CH 컬럼과 매칭할 때 대소문자를 구분하지 않습니다                              | `0`         |
| `input_format_arrow_import_nested`                                           | 더 이상 사용되지 않는 설정으로, 아무 동작도 하지 않습니다.                                  | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Arrow 포맷의 스키마 추론 중 지원되지 않는 타입이 있는 컬럼을 건너뜁니다                         | `0`         |
| `output_format_arrow_compression_method`                                     | Arrow 출력 형식에 사용할 압축 방식입니다. 지원되는 코덱: lz4&#95;frame, zstd, none(비압축)  | `lz4_frame` |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | FixedString 컬럼에 Binary 대신 Arrow FIXED&#95;SIZE&#95;BINARY 타입을 사용합니다 | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | LowCardinality 타입을 Arrow 딕셔너리 타입으로 출력하도록 설정합니다                      | `0`         |
| `output_format_arrow_string_as_string`                                       | String 컬럼에 Binary 대신 Arrow String 타입을 사용합니다                         | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Arrow 포맷에서 딕셔너리 인덱스에 항상 64비트 정수를 사용합니다                              | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Arrow 포맷에서 딕셔너리 인덱스에 부호 있는 정수를 사용합니다                                | `1`         |