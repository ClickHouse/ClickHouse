---
alias: []
description: 'CapnProto 문서'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`CapnProto` 포맷은 [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) 포맷 및 [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift)와 유사한 바이너리 메시지 포맷이지만, [JSON](./JSON/JSON.md)이나 [MessagePack](https://msgpack.org/)과는 다릅니다.
CapnProto 메시지는 엄격한 타입이 지정되어 있으며 self-describing 방식이 아니므로 외부 스키마 설명이 필요합니다. 스키마는 즉시 적용되며 각 쿼리별로 캐시됩니다.

관련 항목: [Format Schema](/ko/interfaces/formats/#formatschema).

<div id="data_types-matching-capnproto">
  ## 데이터 타입 매핑
</div>

아래 표는 지원되는 데이터 타입과, `INSERT` 및 `SELECT` 쿼리에서 해당 타입이 ClickHouse [데이터 타입](/ko/sql-reference/data-types/index.md)에 어떻게 매핑되는지 보여줍니다.

| CapnProto 데이터 타입 (`INSERT`)                          | ClickHouse 데이터 타입                                                                                                                                      | CapnProto 데이터 타입 (`SELECT`)                          |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/ko/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/ko/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/ko/sql-reference/data-types/int-uint.md), [Date](/ko/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/ko/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/ko/sql-reference/data-types/int-uint.md), [DateTime](/ko/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/ko/sql-reference/data-types/int-uint.md), [Decimal32](/ko/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/ko/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/ko/sql-reference/data-types/int-uint.md), [DateTime64](/ko/sql-reference/data-types/datetime.md), [Decimal64](/ko/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/ko/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/ko/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/ko/sql-reference/data-types/string.md), [FixedString](/ko/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/ko/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/ko/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/ko/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/ko/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/ko/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/ko/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/ko/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/ko/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/ko/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* 정수 타입은 입력 및 출력 과정에서 서로 변환할 수 있습니다.
* CapnProto 포맷에서 `Enum`을 사용하려면 [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/ko/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) 설정을 사용하십시오.
* 배열은 중첩할 수 있으며, 인수로 `Nullable` 타입의 값을 사용할 수 있습니다. `Tuple` 및 `Map` 타입도 중첩할 수 있습니다.

<div id="example-usage">
  ## 사용 예시
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### 데이터 삽입 및 조회
</div>

다음 명령을 사용하여 파일의 CapnProto 데이터를 ClickHouse 테이블에 삽입할 수 있습니다:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

`schema.capnp`의 내용은 다음과 같습니다:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

다음 명령을 사용하여 ClickHouse 테이블에서 데이터를 선택한 후 `CapnProto` 포맷의 파일로 저장할 수 있습니다:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### 자동 생성된 스키마 사용하기
</div>

데이터용 외부 `CapnProto` 스키마가 없어도 자동 생성된 스키마를 사용해 `CapnProto` 포맷으로 데이터를 입력하거나 출력할 수 있습니다.

예시:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

이 경우 ClickHouse는 함수 [structureToCapnProtoSchema](/ko/sql-reference/functions/other-functions.md#structureToCapnProtoSchema)를 사용해 테이블 구조를 바탕으로 CapnProto 스키마를 자동 생성하고, 이 스키마를 사용해 데이터를 CapnProto 포맷으로 직렬화합니다.

자동 생성된 스키마를 사용해 CapnProto 파일을 읽을 수도 있습니다(이 경우 파일도 동일한 스키마로 생성되어야 합니다):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## 포맷 설정
</div>

설정 [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema)은 기본적으로 활성화되어 있으며, [`format_schema`](/ko/interfaces/formats#formatschema)가 설정되지 않은 경우에 적용됩니다.

입력/출력 시 설정 [`output_format_schema`](/ko/operations/settings/formats#output_format_schema)를 사용하여 자동 생성된 스키마를 파일에 저장할 수도 있습니다.

예시:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

이 경우 자동으로 생성된 `CapnProto` 스키마는 파일 `path/to/schema/schema.capnp`에 저장됩니다.