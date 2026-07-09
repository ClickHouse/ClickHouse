---
alias: []
description: 'BSONEachRow 포맷 문서'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`BSONEachRow` 포맷은 데이터 사이에 구분자 없이 Binary JSON(BSON) 문서의 시퀀스로 데이터를 파싱합니다.
각 행은 단일 문서로 포맷되며, 각 컬럼은 컬럼 이름을 키로 하는 단일 BSON 문서 필드로 포맷됩니다.

<div id="data-types-matching">
  ## 데이터 타입 매핑
</div>

출력 시에는 ClickHouse 타입과 BSON 타입 간에 다음과 같은 대응 관계를 사용합니다:

| ClickHouse type                                                                                       | BSON 유형                                                                                                               |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [Bool](/ko/sql-reference/data-types/boolean.md)                                                          | `\x08` boolean                                                                                                          |
| [Int8/UInt8](/ko/sql-reference/data-types/int-uint.md)/[Enum8](/ko/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                            |
| [Int16/UInt16](/ko/sql-reference/data-types/int-uint.md)/[Enum16](/ko/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                            |
| [Int32](/ko/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                            |
| [UInt32](/ko/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                            |
| [Int64/UInt64](/ko/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                            |
| [Float32/Float64](/ko/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                           |
| [Date](/ko/sql-reference/data-types/date.md)/[Date32](/ko/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                            |
| [DateTime](/ko/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                            |
| [DateTime64](/ko/sql-reference/data-types/datetime64.md)                                                 | `\x09` datetime                                                                                                         |
| [Decimal32](/ko/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                            |
| [Decimal64](/ko/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                            |
| [Decimal128](/ko/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` binary subtype, size = 16                                                                         |
| [Decimal256](/ko/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` binary subtype, size = 32                                                                         |
| [Int128/UInt128](/ko/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` binary subtype, size = 16                                                                         |
| [Int256/UInt256](/ko/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` binary subtype, size = 32                                                                         |
| [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` binary subtype 또는 output&#95;format&#95;bson&#95;string&#95;as&#95;string 설정이 활성화된 경우 \x02 string |
| [UUID](/ko/sql-reference/data-types/uuid.md)                                                             | `\x05` binary, `\x04` uuid subtype, size = 16                                                                           |
| [Array](/ko/sql-reference/data-types/array.md)                                                           | `\x04` array                                                                                                            |
| [Tuple](/ko/sql-reference/data-types/tuple.md)                                                           | `\x04` array                                                                                                            |
| [Named Tuple](/ko/sql-reference/data-types/tuple.md)                                                     | `\x03` document                                                                                                         |
| [Map](/ko/sql-reference/data-types/map.md)                                                               | `\x03` document                                                                                                         |
| [IPv4](/ko/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                            |
| [IPv6](/ko/sql-reference/data-types/ipv6.md)                                                             | `\x05` binary, `\x00` binary subtype                                                                                    |

입력 시에는 BSON 타입과 ClickHouse 타입 간에 다음과 같은 대응 관계를 사용합니다:

| BSON 유형                           | ClickHouse 유형                                                                                                                                                                                       |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                     | [Float32/Float64](/ko/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` 문자열                        | [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` 문서                         | [Map](/ko/sql-reference/data-types/map.md)/[Named Tuple](/ko/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` 배열                         | [Array](/ko/sql-reference/data-types/array.md)/[Tuple](/ko/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` 바이너리, `\x00` 바이너리 하위 유형    | [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md)/[IPv6](/ko/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` 바이너리, `\x02` 이전 바이너리 하위 유형 | [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` 바이너리, `\x03` 이전 uuid 하위 유형 | [UUID](/ko/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` 바이너리, `\x04` uuid 하위 유형    | [UUID](/ko/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                   | [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` 불리언                        | [Bool](/ko/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` 날짜/시간                      | [DateTime64](/ko/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` NULL 값                     | [NULL](/ko/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` JavaScript 코드              | [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` 심볼                         | [String](/ko/sql-reference/data-types/string.md)/[FixedString](/ko/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                      | [Int32/UInt32](/ko/sql-reference/data-types/int-uint.md)/[Decimal32](/ko/sql-reference/data-types/decimal.md)/[IPv4](/ko/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/ko/sql-reference/data-types/enum.md) |
| `\x12` int64                      | [Int64/UInt64](/ko/sql-reference/data-types/int-uint.md)/[Decimal64](/ko/sql-reference/data-types/decimal.md)/[DateTime64](/ko/sql-reference/data-types/datetime64.md)                                       |

다른 BSON 타입은 지원하지 않습니다. 또한 서로 다른 정수 타입 간 변환도 수행합니다.
예를 들어 BSON `int32` 값은 ClickHouse에 [`UInt8`](../../sql-reference/data-types/int-uint.md)로 삽입할 수 있습니다.

`Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256`과 같은 큰 정수와 Decimal은 `\x00` 바이너리 하위 유형의 BSON Binary 값에서 파싱할 수 있습니다.
이 경우 포맷은 바이너리 데이터의 크기가 기대하는 값의 크기와 일치하는지 검증합니다.

:::note
이 포맷은 Big-Endian 플랫폼에서 올바르게 동작하지 않습니다.
:::

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-data">
  ### 데이터 삽입
</div>

다음 데이터가 포함된 BSON 파일 `football.bson`을 사용합니다:

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
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### 데이터 읽기
</div>

`BSONEachRow` 포맷으로 데이터를 읽습니다:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON은 터미널에서 사람이 읽을 수 있는 형태로 표시되지 않는 바이너리 형식입니다. BSON 파일로 출력하려면 `INTO OUTFILE`을 사용하세요.
:::

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                                                                                                                                                                    | 설명                                                         | 기본값     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | String 컬럼에 Binary 대신 BSON String 타입을 사용합니다.                | `false` |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | BSONEachRow 포맷의 스키마 추론 중 지원되지 않는 타입의 컬럼을 스키핑할 수 있도록 허용합니다. | `false` |