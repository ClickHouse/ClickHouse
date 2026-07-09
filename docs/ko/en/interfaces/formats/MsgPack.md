---
alias: []
description: 'MsgPack 포맷 문서'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

ClickHouse는 [MessagePack](https://msgpack.org/) 데이터 파일을 읽고 쓸 수 있습니다.

<div id="data-types-matching">
  ## 데이터 타입 매핑
</div>

| MessagePack 데이터 타입 (`INSERT`)                                      | ClickHouse 데이터 타입                                                                           | MessagePack 데이터 타입 (`SELECT`)      |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | ---------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/ko/sql-reference/data-types/int-uint.md)                                            | `uint N`                           |
| `int N`, `negative fixint`                                         | [`IntN`](/ko/sql-reference/data-types/int-uint.md)                                             | `int N`                            |
| `bool`                                                             | [`UInt8`](/ko/sql-reference/data-types/int-uint.md)                                            | `uint 8`                           |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/ko/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`        |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/ko/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`        |
| `float 32`                                                         | [`Float32`](/ko/sql-reference/data-types/float.md)                                             | `float 32`                         |
| `float 64`                                                         | [`Float64`](/ko/sql-reference/data-types/float.md)                                             | `float 64`                         |
| `uint 16`                                                          | [`Date`](/ko/sql-reference/data-types/date.md)                                                 | `uint 16`                          |
| `int 32`                                                           | [`Date32`](/ko/sql-reference/data-types/date32.md)                                             | `int 32`                           |
| `uint 32`                                                          | [`DateTime`](/ko/sql-reference/data-types/datetime.md)                                         | `uint 32`                          |
| `uint 64`                                                          | [`DateTime64`](/ko/sql-reference/data-types/datetime.md)                                       | `uint 64`                          |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/ko/sql-reference/data-types/array.md)/[`Tuple`](/ko/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/ko/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`       |
| `uint 32`                                                          | [`IPv4`](/ko/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                          |
| `bin 8`                                                            | [`String`](/ko/sql-reference/data-types/string.md)                                             | `bin 8`                            |
| `int 8`                                                            | [`Enum8`](/ko/sql-reference/data-types/enum.md)                                                | `int 8`                            |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/ko/sql-reference/data-types/int-uint.md)                            | `bin 8`                            |
| `int 32`                                                           | [`Decimal32`](/ko/sql-reference/data-types/decimal.md)                                         | `int 32`                           |
| `int 64`                                                           | [`Decimal64`](/ko/sql-reference/data-types/decimal.md)                                         | `int 64`                           |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/ko/sql-reference/data-types/decimal.md)                           | `bin 8 `                           |

<div id="example-usage">
  ## 사용 예시
</div>

&quot;.msgpk&quot; 파일에 쓰기:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## 포맷 설정
</div>

| 설정                                                                                                                                 | 설명                                                         | 기본값   |
| ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ----- |
| [`input_format_msgpack_number_of_columns`](/ko/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | 삽입되는 MsgPack 데이터의 컬럼 수입니다. 데이터에서 자동으로 스키마 추론을 수행할 때 사용됩니다. | `0`   |
| [`output_format_msgpack_uuid_representation`](/ko/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | MsgPack 포맷에서 UUID를 출력하는 방식입니다.                             | `EXT` |