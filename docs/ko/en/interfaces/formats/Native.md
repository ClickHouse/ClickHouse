---
alias: []
description: 'Native 형식 문서'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: '참고'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`Native` 형식의 전체 공식 사양은 [여기](/ko/interfaces/specs/NativeFormat)에서 확인할 수 있으며, 이를 전달하는 TCP wire 프로토콜인 `Native` 프로토콜의 관련 사양은 [여기](/ko/interfaces/specs/NativeProtocol)에서 확인할 수 있습니다.

:::note
두 사양 모두 ClickHouse 소스 코드를 바탕으로 LLM이 생성했습니다. 코드는 여전히 가장 중요한 기준입니다. 사양과 코드가 서로 일치하지 않으면 코드가 맞습니다.
:::

`Native` 형식은 컬럼을 행으로 변환하지 않는 진정한 &quot;열 지향&quot; 방식이므로 ClickHouse에서 가장 효율적인 포맷입니다.

이 포맷에서는 데이터가 바이너리 형식으로 [블록](/ko/development/architecture#block) 단위로 기록되고 읽힙니다.
각 블록에는 행 수, 컬럼 수, 컬럼 이름과 타입, 그리고 블록에 포함된 컬럼의 일부가 차례대로 기록됩니다.

이 포맷은 서버 간 상호작용을 위한 네이티브 인터페이스, command-line client, 그리고 C++ 클라이언트에서 사용됩니다.

:::tip
이 포맷을 사용하면 ClickHouse DBMS에서만 읽을 수 있는 덤프를 빠르게 생성할 수 있습니다.
다만 이 포맷을 직접 다루는 것은 실용적이지 않을 수 있습니다.
:::

<div id="data-types-wire-format">
  ## 데이터 타입 wire 형식
</div>

데이터는 wire 상에서 컬럼형 포맷으로 전송되므로 각 컬럼은 별도로 전송되며,
각 컬럼의 모든 값은 하나의 배열로 함께 전송됩니다.

블록의 각 컬럼에는 [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md)와 유사한 헤더가 포함됩니다.

:::note
네이티브 TCP 바이너리 프로토콜을 사용할 때(또는 HTTP 엔드포인트가 `?client_protocol_version=<n>`을 수신할 때),
컬럼 수와 행 수에 앞서 `BlockInfo` 구조체가 기록됩니다. 이 섹션의 예시는
프로토콜 버전이 없는 일반 HTTP 인터페이스를 사용하므로 `BlockInfo`가 생략됩니다.
:::

<div id="block-structure">
  ### 블록 구조
</div>

다음 쿼리는 `number`와 `str` 두 개의 컬럼과 3개의 행을 반환합니다:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

출력 데이터는 하나의 ClickHouse 블록에 담기며, 다음과 같습니다:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

<div id="multiple-blocks">
  ### 여러 블록
</div>

하지만 많은 경우 데이터가 하나의 블록에 담기지 않으므로, ClickHouse는 데이터를 여러 블록으로 나누어 전송합니다.
다음 쿼리는 블록 크기를 줄여 데이터가 블록당 한 행씩 분할되도록 강제한 상태에서 두 개의 행을 가져오는 예입니다:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

출력 결과:

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

<div id="simple-data-types">
  ### 간단한 데이터 타입
</div>

더 간단한 데이터 타입에 속하는 개별 값의 wire 형식은 `RowBinary`/`RowBinaryWithNamesAndTypes`와 유사합니다.
여기에 해당하는 타입의 전체 목록은 다음과 같습니다.

* (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
* Float32, Float64
* Bool
* String
* FixedString(N)
* Date
* Date32
* DateTime
* DateTime64
* IPv4
* IPv6
* UUID

자세한 내용은 [&quot;RowBinary 데이터 타입 wire 형식&quot;](/ko/interfaces/formats/RowBinary#data-types-wire-format)의 위 타입 설명을 참조하십시오.

<div id="complex-data-types">
  ### 복합 데이터 타입
</div>

다음 데이터 타입의 인코딩은 `RowBinary` 및 `RowBinaryWithNamesAndTypes`와 다릅니다.

* 널 허용
* LowCardinality
* 배열
* 맵
* Variant
* Dynamic
* JSON

<div id="nullable">
  #### 널 허용
</div>

`Native` 형식에서는 널 허용 컬럼의 실제 데이터 앞에 블록의 행 수와 동일한 바이트 수가 먼저 저장됩니다. 각 바이트는 해당 값이 `NULL`인지 아닌지를 나타냅니다. 예를 들어, 다음 쿼리에서는 각 홀수가 `NULL`로 표시됩니다:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

출력 결과는 다음과 같습니다:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

`Nullable(String)`도 비슷한 방식으로 동작합니다. null 표시자는 항상 널 허용 마스크 바이트에서 가져오며 —
마스크 값이 `0x01`이면 문자열 내용과 관계없이 해당 행은 `NULL`입니다. `NULL` 행의 경우,
실제 문자열 값은 빈 문자열(LEB128 길이 `0`)로 저장됩니다. `NULL`이 아닌 빈
문자열도 LEB128 길이가 `0`이므로, 두 경우를 구분하는 것은 마스크 바이트뿐입니다. 예를 들어, 다음 쿼리가 있습니다:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

출력은 다음과 같습니다:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

<div id="lowcardinality">
  #### LowCardinality
</div>

[RowBinary](RowBinary/RowBinary.md#lowcardinality)에서는 `LowCardinality`가 투명하게 처리되지만, Native 형식에서는 딕셔너리 기반 열 지향 인코딩을 사용합니다. 컬럼은 버전 접두사, 고유 값의 딕셔너리, 그리고 해당 딕셔너리를 참조하는 정수 인덱스 배열로 인코딩됩니다.

:::note
컬럼은 `LowCardinality(Nullable(T))`로 정의할 수 있지만 `Nullable(LowCardinality(T))`로는 정의할 수 없습니다. 이렇게 정의하면 항상 server에서 오류가 발생합니다.
:::

버전 접두사는 값이 `1`인 `UInt64(LE)`이며, 컬럼마다 한 번 기록됩니다. 그다음 각 블록마다 다음이 기록됩니다.

* `UInt64(LE)` — `IndexesSerializationType` 비트필드. 비트 0–7은 인덱스 너비를 인코딩합니다(0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). 비트 8(`NeedGlobalDictionaryBit`)은 Native 형식에서는 절대 설정되지 않습니다(이 값이 나오면 server가 예외를 발생시킵니다). 비트 9는 추가 딕셔너리 키가 있음을 나타냅니다. 비트 10은 딕셔너리를 재설정해야 함을 나타냅니다.
* `UInt64(LE)` — 딕셔너리 키 개수이며, 이어서 내부 타입 인코딩을 사용해 키를 일괄 직렬화합니다.
* `UInt64(LE)` — 행 수이며, 이어서 적절한 UInt 너비를 사용해 인덱스 값을 일괄 직렬화합니다.

딕셔너리에는 항상 인덱스 0에 기본값이 들어 있습니다(예: `String`은 빈 문자열, 숫자 타입은 0). `LowCardinality(Nullable(T))`의 경우 인덱스 0은 `NULL`을 나타내며, 키는 `Nullable` 래퍼 없이 직렬화됩니다.

예를 들어 `['foo', 'bar', 'baz', 'foo', 'bar']`의 5개 행을 가진 `LowCardinality(String)`은 다음과 같습니다:

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

`LowCardinality(Nullable(String))`의 경우 인덱스 0은 `NULL`입니다:

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

<div id="array">
  #### 배열
</div>

각 배열 앞에 LEB128 요소 수가 붙는 [RowBinary](RowBinary/RowBinary.md#array)와 달리, Native 형식은 배열을 2개의 열 지향 하위 스트림으로 인코딩합니다.

* 누적 `UInt64` 오프셋 N개(리틀 엔디언, 각 8바이트)입니다. 행 `i`의 요소 수는 `offset[i] - offset[i-1]`이며, `offset[-1]`은 암묵적으로 0입니다.
* 모든 행에 걸친 모든 중첩 요소를 연속된 형태로 일괄 직렬화합니다.

예를 들어, 3개의 행 `[[0, 10], [1, 11], [2, 12]]`로 이루어진 `Array(UInt32)`는 다음과 같습니다:

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

빈 배열의 offset 값은 이전 행과 동일합니다. 예를 들어, 4개의 행이 있는 `Array(String)` `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

<div id="map">
  #### 맵
</div>

`Map(K, V)`는 `Array(Tuple(K, V))`로 인코딩됩니다. 즉, 배열 오프셋 뒤에 모든 키가 오고, 그다음 모든 값이 옵니다. 이는 [RowBinary](RowBinary/RowBinary.md#map)와는 다릅니다. RowBinary에서는 각 entry마다 키와 값이 번갈아 배치됩니다.

예를 들어, 3개의 행이 있는 `Map(String, UInt64)` `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`는 다음과 같이 인코딩됩니다:

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

<div id="variant">
  #### Variant
</div>

각 행이 자체 판별자 바이트를 갖고 그 뒤에 값이 인라인으로 이어지는 [RowBinary](RowBinary/RowBinary.md#variant)와 달리, Native 형식은 판별자를 데이터와 분리합니다.

:::warning
RowBinary와 마찬가지로 정의에 있는 타입은 항상 알파벳순으로 정렬되며, 판별자는 그 정렬된 목록에서의 인덱스입니다. `0xFF` (255)는 `NULL`을 나타냅니다.
:::

`Variant` 컬럼은 다음과 같이 인코딩됩니다.

* `UInt64(LE)` 판별자 모드 접두사 (`0` = BASIC, `1` = COMPACT). Native 형식 출력은 일반적으로 BASIC (`0`)을 사용합니다. COMPACT 모드는 `use_compact_variant_discriminators_serialization`이 활성화된 상태로 저장된 데이터를 읽을 때 나타날 수 있습니다.
* N개의 `UInt8` 판별자. 각 행마다 하나씩 있습니다.
* 각 variant type의 데이터는 일치하는 행만 포함하는 별도의 벌크 컬럼으로 인코딩되며, 판별자 순서대로 저장됩니다.

예를 들어, 5개의 행 `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`가 있는 `Variant(String, UInt32)`의 경우(정렬: `String` = 0, `UInt32` = 1):

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

<div id="dynamic">
  #### Dynamic
</div>

각 값이 자체 기술형(타입 접두사 + 값)인 [RowBinary](RowBinary/RowBinary.md#dynamic)와 달리, Native 형식에서는 `Dynamic`을 구조체 접두사 뒤에 [Variant](#variant) 컬럼이 이어지는 형태로 직렬화합니다.

구조체 접두사에는 `UInt64(LE)` 직렬화 버전이 포함되며, 그 다음에는 동적 타입의 개수(VarUInt 형식)와 문자열 형태의 타입 이름이 옵니다. V1 버전에서는 호환성을 위해 타입 개수를 두 번 기록합니다. 그 뒤에 오는 데이터는 동적 타입에 내부 `SharedVariant` 타입을 추가한 타입 목록을 알파벳순으로 정렬한 `Variant` 컬럼입니다.

예를 들어, 5개의 행 `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`으로 이루어진 `Dynamic`은 다음과 같습니다:

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

<div id="json">
  #### JSON
</div>

각 행이 경로 이름과 값으로 자체 기술적(self-describing)으로 표현되는 [RowBinary](RowBinary/RowBinary.md#json)와 달리, Native 형식은 `JSON`을 열 지향 구조로 직렬화합니다. 인코딩은 복잡하고 버전에 따라 달라집니다. 직렬화 버전, 동적 경로 이름, 공유 데이터 레이아웃을 포함하는 구조체 접두사로 이루어지며, 그 뒤에 타입이 지정된 경로(각각 하나의 bulk column), 동적 경로(각각 하나의 [Dynamic](#dynamic) 컬럼), 그리고 오버플로우 경로용 공유 데이터가 이어집니다.

더 간단한 상호 운용성을 위해 `output_format_native_write_json_as_string=1` 설정 사용을 고려하십시오. 이 설정은 JSON 컬럼을 일반 JSON 텍스트 문자열(각 행당 `String` 1개)로 직렬화합니다.