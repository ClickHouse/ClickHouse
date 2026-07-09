---
alias: []
description: 'RowBinary 포맷 문서'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
doc_type: '참고'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

`RowBinary` 포맷은 데이터를 바이너리 형식의 행 단위로 파싱합니다.
행과 값은 구분자 없이 연속해서 나열됩니다.
데이터가 바이너리 형식이므로 `FORMAT RowBinary` 뒤의 구분자는 다음과 같이 엄격하게 지정됩니다.

* 공백 문자 0개 이상:
  * `' '` (공백 - 코드 `0x20`)
  * `'\t'` (탭 - 코드 `0x09`)
  * `'\f'` (폼 피드 - 코드 `0x0C`)
* 이어서 정확히 하나의 줄바꿈 시퀀스:
  * Windows 스타일 `"\r\n"`
  * 또는 Unix 스타일 `'\n'`
* 그 직후에 바이너리 데이터가 이어집니다.

:::note
이 포맷은 행 기반이므로 [Native](../Native.md) 포맷보다 효율이 떨어집니다.
:::

<div id="data-types-wire-format">
  ## 데이터 타입 wire 형식
</div>

:::tip
예시에 나온 대부분의 쿼리는 결과를 파일로 출력하도록 curl로 실행할 수 있습니다.

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```

:::

그런 다음 16진수 편집기로 데이터를 확인할 수 있습니다.

<div id="unsigned-leb128">
  ### 부호 없는 LEB128 (리틀 엔디언 Base 128)
</div>

`String`, `Array`, `Map`과 같은 가변 크기 데이터 타입의 길이를 인코딩하는 데 사용되는 **부호 없는 리틀 엔디언** 가변 길이 정수 인코딩입니다. 예시 구현은 [LEB128 위키 페이지](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer)에서 확인할 수 있습니다.

<div id="integer-types">
  ### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
</div>

모든 정수 타입은 **리틀 엔디언** 바이트 순서로, 타입에 맞는 바이트 수를 사용해 인코딩됩니다. 부호 있는 타입(`Int8`~`Int256`)은 **2의 보수** 표현을 사용합니다. 대부분의 언어에서는 내장 기능이나 널리 사용되는 라이브러리를 통해 바이트 배열에서 이러한 정수를 추출할 수 있습니다. `Int128`/`Int256` 및 `UInt128`/`UInt256`은 대부분의 언어에서 지원하는 네이티브 정수 크기를 초과하므로, 사용자 정의 역직렬화가 필요할 수 있습니다.

<div id="bool">
  ### Bool
</div>

불리언 값은 단일 바이트로 인코딩되며, `UInt8`와 유사하게 역직렬화할 수 있습니다.

* `0`은 `false`입니다
* `1`은 `true`입니다

<div id="float32-float64">
  ### Float32, Float64
</div>

**리틀 엔디언** 부동소수점 수로, `Float32`는 4바이트, `Float64`는 8바이트로 인코딩됩니다. 정수와 마찬가지로 대부분의 언어는 이러한 값을 역직렬화할 수 있는 적절한 도구를 제공합니다.

<div id="bfloat16">
  ### BFloat16
</div>

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point)은 Float32의 범위를 유지하면서 정밀도를 낮춘 16비트 부동소수점 포맷으로, 머신 러닝 워크로드에 유용합니다. wire 형식은 기본적으로 Float32 값의 상위 16비트입니다. 사용하는 언어에서 이를 네이티브로 지원하지 않는다면, 가장 쉬운 방법은 UInt16으로 읽고 쓴 뒤 Float32로 변환하고 다시 Float32에서 변환하는 것입니다.

BFloat16을 Float32로 변환하는 방법(pseudocode):

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

Float32를 BFloat16으로 변환하려면(의사 코드):

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

`BFloat16`의 예시 내부 값:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

<div id="decimal">
  ### Decimal32, Decimal64, Decimal128, Decimal256
</div>

Decimal 타입은 각 비트 너비에 맞는 **리틀 엔디언** 정수로 표현됩니다.

* `Decimal32` - 4바이트 또는 `Int32`
* `Decimal64` - 8바이트 또는 `Int64`
* `Decimal128` - 16바이트 또는 `Int128`
* `Decimal256` - 32바이트 또는 `Int256`

Decimal 값을 역직렬화할 때는 다음 의사 코드를 사용해 정수부와 소수부를 구할 수 있습니다:

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

여기서 `trunc`는 0을 향해 버림을 수행하며(음수 값에서는 결과가 달라지는 내림 나눗셈이 아님), `scale`은 소수점 아래 자릿수입니다. 예를 들어 `Decimal(10, 2)`(`Decimal32(2)`와 동등함)의 경우 `scale`은 `2`이며, 값 `12345`는 `(123, 45)`로 표현됩니다.

직렬화에는 이 연산의 역연산이 필요합니다:

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

자세한 내용은 [ClickHouse 문서의 Decimal 타입](https://clickhouse.com/docs/sql-reference/data-types/decimal)에서 확인하십시오.

<div id="string">
  ### String
</div>

ClickHouse 문자열은 **임의의 바이트 열**입니다. 유효한 UTF-8일 필요는 없습니다. 길이 접두어는 문자 수가 아니라 **바이트 길이**를 나타냅니다.

다음 두 부분으로 인코딩됩니다:

1. 문자열 길이를 바이트 단위로 나타내는 가변 길이 정수(LEB128)
2. 문자열의 raw bytes

예를 들어, 문자열 `foobar`는 다음과 같이 *7*바이트를 사용해 인코딩됩니다:

```text
0x06, // LEB128 length of the string (6)
0x66, // 'f'
0x6f, // 'o'
0x6f, // 'o'
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

<div id="fixedstring">
  ### FixedString
</div>

`String`과 달리 `FixedString`은 스키마(schema)에서 정의된 고정 길이를 가집니다. 바이트 시퀀스로 인코딩되며, 값이 `N`보다 짧으면 뒤쪽이 0 바이트로 채워집니다.

:::note
`FixedString`을 읽을 때 뒤쪽의 0 바이트는 패딩일 수도 있고 데이터 안의 실제 `\0` 문자일 수도 있으므로, wire 상에서는 서로 구분할 수 없습니다. ClickHouse는 `N` 바이트 전체를 있는 그대로 보존합니다.
:::

비어 있는 `FixedString(3)`에는 패딩 0만 들어 있습니다:

```text
0x00, 0x00, 0x00
```

문자열 `hi`가 포함된 비어 있지 않은 `FixedString(3)`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

`bar` 문자열이 들어 있는 비어 있지 않은 `FixedString(3)`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

마지막 예시에서는 *세* 바이트를 모두 사용하므로 패딩이 필요하지 않습니다.

<div id="date">
  ### Date
</div>

`1970-01-01` ***이후*** 경과한 일 수를 나타내는 `UInt16`(2바이트)로 저장됩니다.

지원되는 값 범위: `[1970-01-01, 2149-06-06]`.

`Date`의 내부 값 예시:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (little-endian) = 19737 days since 1970-01-01
```

<div id="date32">
  ### Date32
</div>

`1970-01-01` ***전후***&#xC758; 일수를 나타내는 `Int32`(4바이트)로 저장됩니다.

지원되는 값 범위: `[1900-01-01, 2299-12-31]`.

`Date32`의 예시 내부 값:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (little-endian) = 19737 days since 1970-01-01
```

epoch 이전 날짜:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (little-endian) = 25567 days before 1970-01-01
```

<div id="datetime">
  ### DateTime
</div>

`1970-01-01 00:00:00 UTC` ***이후부터*** 경과한 초 수를 나타내는 `UInt32`(4바이트)로 저장됩니다.

구문:

```text
DateTime([timezone])
```

예를 들어 `DateTime` 또는 `DateTime('UTC')`입니다.

:::note
이진 값은 항상 UTC epoch 오프셋입니다. 시간대는 인코딩을 바꾸지 않습니다. 하지만 시간대는 문자열 값을 삽입할 때 그 문자열이 어떻게 해석되는지에는 **분명히** 영향을 줍니다. 예를 들어 `'2024-01-15 10:30:00'`을 `DateTime('America/New_York')` 컬럼에 삽입하면, 같은 문자열을 `DateTime('UTC')` 컬럼에 삽입할 때와는 다른 epoch 값이 저장됩니다. 이는 해당 문자열이 컬럼의 시간대 기준 로컬 시간으로 해석되기 때문입니다. wire 상에서는 둘 다 단지 `UInt32` epoch 초일 뿐입니다.
:::

지원되는 값 범위: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

`DateTime`의 내부 값 예시:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (little-endian)
```

<div id="datetime64">
  ### DateTime64
</div>

`1970-01-01 00:00:00 UTC`를 기준으로 ***이전 또는 이후의*** **틱** 수를 나타내는 `Int64`(8바이트)로 저장됩니다. 틱 해상도는 `precision` 매개변수로 정의되며, 아래 구문을 참조하십시오:

```text
DateTime64(precision, [timezone])
```

여기서 `precision`은 `0`부터 `9`까지의 정수입니다. 일반적으로는 `3`(밀리초), `6`(마이크로초),
`9`(나노초)만 사용됩니다.

유효한 DateTime64 정의 예시는 `DateTime64(0)`, `DateTime64(3)`, `DateTime64(6, 'UTC')`, `DateTime64(9, 'Europe/Amsterdam')`입니다.

:::note
`DateTime`과 마찬가지로 바이너리 값은 항상 UTC epoch 기준 오프셋입니다. 시간대는 문자열 값이 삽입 시 어떻게 해석되는지에 영향을 주지만([DateTime](#datetime)의 참고 사항 참조), 인코딩 자체는 항상 UTC epoch 이후의 `Int64` 틱입니다.
:::

`DateTime64` 타입의 내부 `Int64` 값은 UNIX epoch 이전 또는 이후의 다음 단위 수로 해석할 수 있습니다:

* `DateTime64(0)` - 초.
* `DateTime64(3)` - 밀리초.
* `DateTime64(6)` - 마이크로초.
* `DateTime64(9)` - 나노초.

지원되는 값 범위: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

`DateTime64`의 내부 값 예시:

* `DateTime64(3)`: 값 `1546300800000`은 `2019-01-01 00:00:00 UTC`를 나타냅니다.
* `DateTime64(6)`: 값 `1705314600123456`은 `2024-01-15 10:30:00.123456 UTC`를 나타냅니다.
* `DateTime64(9)`: 값 `1705314600123456789`은 `2024-01-15 10:30:00.123456789 UTC`를 나타냅니다.

:::note
최댓값의 정밀도는 8입니다. 최대 정밀도인 9자리(나노초)를 사용하면 지원되는 최댓값은 UTC 기준 `2262-04-11 23:47:16`입니다.
:::

<div id="time">
  ### Time
</div>

초 단위의 시간 값을 나타내는 `Int32`로 저장됩니다. 음수 값도 유효합니다.

지원되는 값 범위는 `[-999:59:59, 999:59:59]`(즉, `[-3599999, 3599999]`초)입니다.

:::note
현재 `Time` 또는 `Time64`를 사용하려면 setting `enable_time_time64_type`을 `1`로 설정해야 합니다.
:::

`Time`의 예시 내부 값:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```text
0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
```

<div id="time64">
  ### Time64
</div>

내부적으로는 `Decimal64`(`Int64`로 저장됨)로 저장되며, 소수 초를 포함한 시간 값을 나타내고 정밀도는 구성할 수 있습니다. 음수 값도 유효합니다.

구문:

```text
Time64(precision)
```

`precision`은 `0`부터 `9`까지의 정수입니다. 일반적으로는 `3`(밀리초), `6`(마이크로초), `9`(나노초)를 사용합니다.

지원되는 값 범위: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

:::note
현재 `Time` 또는 `Time64`를 사용하려면 `enable_time_time64_type` 설정을 `1`로 지정해야 합니다.
:::

내부 `Int64` 값은 `10^precision`이 적용된 초의 소수 부분을 나타냅니다.

`Time64`의 내부 값 예시:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```text
0x40, 0x82, 0x0D, 0x06,
0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

<div id="interval-types">
  ### 인터벌 타입
</div>

모든 인터벌 타입은 `Int64`(8바이트, 리틀 엔디언)로 저장됩니다. 값은 해당 시간 단위의 개수를 나타냅니다. 음수 값도 유효합니다.

인터벌 타입은 다음과 같습니다: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`.

:::note
인터벌 타입 이름(예: `IntervalSecond`와 `IntervalDay`)에 따라 저장된 값의 단위가 결정됩니다. wire 인코딩은 항상 동일합니다.
:::

내부 저장 값 예시:

```sql
SELECT INTERVAL 5 SECOND   AS a,
     INTERVAL 10 DAY     AS b,
     INTERVAL -7 DAY     AS c,
     INTERVAL 3 YEAR     AS d,
     INTERVAL 500 MICROSECOND AS e
```

```text
// IntervalSecond: 5
0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: 10
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: -7
0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
// IntervalYear: 3
0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalMicrosecond: 500
0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

<div id="enum8-enum16">
  ### Enum8, Enum16
</div>

enum 정의에서 enum 값의 인덱스를 나타내는 1바이트(`Enum8` == `Int8`) 또는 2바이트(`Enum16` == `Int16`)로 저장됩니다. 저장 유형은 **signed**이므로 enum 값은 음수일 수 있습니다(예: `Enum8('a' = -128, 'b' = 0)`).

Enum은 다음과 같이 간단하게 정의할 수 있습니다:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

위에서 정의한 Enum8은 클라이언트 측에서 다음과 같은 값 매핑을 가집니다:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

또는 다음과 같이 더 복잡하게 표현할 수도 있습니다:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

위에서 정의한 Enum16은 클라이언트 측에서 다음과 같은 값 매핑을 가집니다:

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

데이터 타입 파서의 주요 챌린지는 `\'`처럼 enum 정의 안의 이스케이프된 기호와, 따옴표로 묶인 문자열 내부에 나타날 수 있는 `=` 같은 특수 기호를 추적하는 것입니다.

<div id="uuid">
  ### UUID
</div>

16바이트 시퀀스로 표현됩니다. UUID는 **2개의 리틀 엔디언 `UInt64` 값**으로 저장됩니다. 즉, 표준 UUID 표현에서 앞의 8바이트는 바이트 순서가 반전되어 저장되고, 뒤의 8바이트도 별도로 바이트 순서가 반전되어 저장됩니다.

예를 들어, UUID `61f0c404-5cb3-11e7-907b-a6006ad3dba0`가 주어졌을 때:

* 표준 바이트 표현: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
* 첫 번째 절반 반전(LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
* 두 번째 절반 반전(LE UInt64): `a0 db d3 6a 00 a6 7b 90`

`UUID`의 내부 값 예시:

* `61f0c404-5cb3-11e7-907b-a6006ad3dba0`는 다음과 같이 표현됩니다:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

* 기본 UUID `00000000-0000-0000-0000-000000000000`는 0으로 채워진 16바이트로 표현됩니다:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

새 레코드가 삽입되었지만 UUID 값이 지정되지 않았을 때 사용할 수 있습니다.

<div id="ipv4">
  ### IPv4
</div>

4바이트의 `UInt32`로 **리틀 엔디언** 바이트 순서로 저장됩니다. 이는 IP 주소에 일반적으로 사용되는 전통적인 네트워크 바이트 순서(빅 엔디언)와 다릅니다. `IPv4`의 내부 저장 값 예시는 다음과 같습니다:

```sql
SELECT    
  CAST('0.0.0.0',         'IPv4') AS a,
  CAST('127.0.0.1',       'IPv4') AS b,
  CAST('192.168.0.1',     'IPv4') AS c,
  CAST('255.255.255.255', 'IPv4') AS d,
  CAST('168.212.226.204', 'IPv4') AS e
```

```text
0x00, 0x00, 0x00, 0x00, // 0.0.0.0
0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
0xff, 0xff, 0xff, 0xff, // 255.255.255.255
0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
```

<div id="ipv6">
  ### IPv6
</div>

**빅 엔디언 / 네트워크 바이트 순서**(MSB 우선)로 16바이트에 저장됩니다. `IPv6`의 예시 내부 값은 다음과 같습니다:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```text
// 2a02:aa08:e000:3100::2
0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
// 2001:44c8:129:2632:33:0:252:2
0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
// 2a02:e980:1e::1
0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
```

<div id="nullable">
  ### 널 허용
</div>

널 허용 데이터 타입은 다음과 같이 인코딩됩니다.

1. 값이 `NULL`인지 여부를 나타내는 1바이트:
   * `0x00`은 값이 `NULL`이 아님을 의미합니다.
   * `0x01`은 값이 `NULL`임을 의미합니다.
2. 값이 `NULL`이 아니면 내부 데이터 타입이 평소처럼 인코딩됩니다. 값이 `NULL`이면 내부 타입에 대해서는 **추가 바이트를 전혀** 기록하지 않습니다.

예를 들어, `Nullable(UInt32)` 값:

```sql
SELECT    
   CAST(42,   'Nullable(UInt32)') AS a,
   CAST(NULL, 'Nullable(UInt32)') AS b
```

```text
0x00,                   // Not NULL - the value follows
0x2A, 0x00, 0x00, 0x00, // UInt32(42)
0x01,                   // NULL - nothing follows
```

<div id="lowcardinality">
  ### LowCardinality
</div>

RowBinary 형식에서는 low-cardinality 마커가 wire 형식에 영향을 미치지 않습니다. 예를 들어 `LowCardinality(String)`은 일반 `String`과 동일한 방식으로 인코딩됩니다.

:::warning
이는 RowBinary에만 적용됩니다. Native 형식에서는 `LowCardinality`가 딕셔너리 기반의 다른 인코딩을 사용합니다.
:::

:::note
컬럼은 `LowCardinality(Nullable(T))`로 정의할 수 있지만, `Nullable(LowCardinality(T))`로는 정의할 수 없으며, 이 경우 서버에서 항상 오류가 발생합니다.
:::

테스트 시에는 적용 범위를 넓히기 위해 [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types)를 `1`로 설정하여 대부분의 데이터 타입을 `LowCardinality` 내부에서 허용할 수 있습니다.

<div id="array">
  ### 배열
</div>

배열은 다음과 같이 인코딩됩니다.

1. 배열의 요소 수를 나타내는 [가변 길이 정수(LEB128)](#unsigned-leb128)
2. 기본 데이터 타입과 동일한 방식으로 인코딩된 배열의 요소

예를 들어, `UInt32` 값으로 구성된 배열은 다음과 같습니다.

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

좀 더 복잡한 예시는 다음과 같습니다:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```text
0x02,             // LEB128 - the array has 2 elements
0x06,             // LEB128 - the first string has 6 bytes
0x66, 0x6f, 0x6f, 
0x62, 0x61, 0x72, // 'foobar'
0x03,             // LEB128 - the second string has 3 bytes
0x71, 0x61, 0x7a, // 'qaz'
```

:::note
배열에는 널 허용 값을 포함할 수 있지만, 배열 자체는 널 허용일 수 없습니다.
:::

다음은 올바른 예입니다:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

다음과 같이 인코딩됩니다:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

다차원 배열을 다루는 예시는 [Geo 섹션](#geo-types)에서 확인할 수 있습니다.

<div id="tuple">
  ### 튜플
</div>

튜플은 추가적인 메타정보나 구분 기호 없이, 각 요소가 해당 wire 형식으로 차례대로 이어지는 방식으로 인코딩됩니다.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```text
0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
0x03,                   // LEB128 - the string has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x02,                   // LEB128 - the array has 2 elements
0x63,                   // 99 as UInt8
0x90,                   // 144 as UInt8
```

튜플 데이터 타입의 문자열 인코딩은 이스케이프된 기호와 특수 문자를 추적해야 한다는 점에서 [Enum type](#enum8-enum16)과 비슷한 어려움이 있으며, 튜플에서는 여기에 더해 여는 괄호와 닫는 괄호까지 추적해야 합니다. 또한 복잡한 튜플은 다른 중첩된 튜플, 배열, 맵, 심지어 enum까지 포함할 수 있다는 점에 유의하십시오.

예를 들어, 다음 표의 튜플에는 이름에 틱과 괄호가 포함된 enum이 들어 있어, 이를 적절히 처리하지 않으면 parsing 문제가 발생할 수 있습니다:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),
          Array(Nullable(Tuple(UInt32, String)))
       )
) ENGINE = Memory;
```

<div id="map">
  ### 맵
</div>

맵은 `Array(Tuple(K, V))`로 볼 수 있으며, 여기서 `K`는 키 타입이고 `V`는 값 타입입니다. 맵은 다음과 같이 인코딩됩니다.

1. 맵에 포함된 요소 수를 나타내는 [가변 길이 정수(LEB128)](#unsigned-leb128)
2. 각 타입에 따라 인코딩된 key-value 쌍 형태의 맵 요소

예를 들어, 키가 `String`이고 값이 `UInt32`인 맵은 다음과 같습니다:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```text
0x02,                   // LEB128 - the map has 2 elements
0x03,                   // LEB128 - the first key has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x03,                   // LEB128 - the second key has 3 bytes
0x62, 0x61, 0x72,       // 'bar'
0x02, 0x00, 0x00, 0x00, // UInt32(2)
```

:::note
`Map(String, Map(Int32, Array(Nullable(String))))`처럼 매우 깊게 중첩된 구조의 맵도 사용할 수 있으며, 이런 경우에도 위에서 설명한 방식과 유사하게 인코딩됩니다.
:::

<div id="variant">
  ### Variant
</div>

이 타입은 여러 다른 데이터 타입을 묶는 유니온 타입입니다. `Variant(T1, T2, ..., TN)` 타입은 이 타입의 각 행이 `T1`, `T2`, …, `TN` 중 하나의 값을 가지거나, 어느 타입에도 속하지 않는 `NULL` 값을 가질 수 있음을 의미합니다.

:::warning
최종 사용자 입장에서는 `Variant(T1, T2)`와 `Variant(T2, T1)`가 정확히 동일한 의미이지만, 정의에서 타입의 순서는 wire 형식에 중요합니다. 정의에 있는 타입은 항상 알파벳순으로 정렬되며, 이는 정확히 어떤 variant인지를 &quot;판별자&quot;, 즉 정의에 있는 데이터 타입 인덱스로 인코딩하기 때문입니다.
:::

다음 예시를 살펴보십시오:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- It does not matter what is the order of types in the user input;
  -- the types are always sorted alphabetically in the wire format.
  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES (true), ('foobar' :: FixedString(6)), (100.5 :: Float64), (100 :: Int128), ([1, 2, 3] :: Array(Int16));
SELECT * FROM foo FORMAT RowBinary;
```

```text
0x01,                               // type index -> Bool
 0x01,                               // true
 0x03,                               // type index -> FixedString(6)
 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
 0x05,                               // type index -> Float64
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
 0x06,                               // type index -> Int128
 0x64, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00,             // 100 as Int128
 0x00,                               // type index -> Array(Int16)
 0x03,                               // LEB128 - the array has 3 elements
 0x01, 0x00,                         // 1 as Int16
 0x02, 0x00,                         // 2 as Int16
 0x03, 0x00,                         // 3 as Int16
```

`NULL` 값은 판별자 바이트 `0xFF`로 인코딩됩니다:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

[allow&#95;suspicious&#95;variant&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) 설정을 사용하면 `Variant` 타입을 보다 폭넓게 테스트할 수 있습니다.

<div id="dynamic">
  ### Dynamic
</div>

`Dynamic` 타입은 런타임에 결정되는 임의의 타입 값을 저장할 수 있습니다. RowBinary 형식에서는 각 값이 자체적으로 타입 정보를 포함합니다. 첫 번째 부분은 [이 포맷](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding)의 타입 명세입니다. 이어서 이 문서에 설명된 값 인코딩 방식에 따라 실제 내용이 뒤따릅니다. 따라서 값을 파싱하려면 타입 인덱스를 사용해 적절한 파서를 결정한 다음, 이미 다른 곳에서 사용 중인 RowBinary 파싱 로직을 다시 사용하면 됩니다.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

여기서 `BinaryTypeIndex`는 타입을 식별하는 1바이트 값입니다. 타입 인덱스와 매개변수는 [여기](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding)의 참고 문서를 참조하십시오.

`NULL` Dynamic 값은 추가 바이트 없이 `BinaryTypeIndex` `0x00`(`Nothing` 타입)으로 인코딩됩니다:

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**예시:**

```sql
SELECT 42::Dynamic
```

```text
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

<div id="json">
  ### JSON
</div>

JSON 타입은 데이터를 두 가지 범주로 인코딩합니다:

1. **타입이 지정된 경로** - 스키마에 명시적으로 타입이 선언된 경로(예: `JSON(user_id UInt32, name String)`)
2. **동적 경로/동적 경로 한도를 초과했을 때의 오버플로우 경로** - 런타임에 발견된 경로이며 `Dynamic` 타입으로 저장됩니다. 값 인코딩 앞에는 타입 정의가 추가됩니다.

이 두 범주의 wire 형식과 규칙은 서로 다릅니다.

| 경로 범주          | 직렬화에 포함         | 값 인코딩       | Variant/널 허용 가능 여부 |
| -------------- | --------------- | ----------- | ------------------ |
| **타입이 지정된 경로** | 항상 포함됨(NULL이어도) | 유형별 바이너리 형식 | 예                  |
| **Dynamic 경로** | non-NULL인 경우에만  | Dynamic     | 아니요                |

경로는 세 그룹으로 직렬화되어 순차적으로 기록됩니다: 타입 경로(typed paths), 동적 경로(dynamic paths), 공유 데이터(shared data) 오버플로우 경로 순입니다. 타입 경로와 동적 경로는 구현 정의 순서(내부 해시맵 반복 순서에 따라 결정)로 기록되며, 공유 데이터 경로는 알파벳 순서로 기록됩니다. 역직렬화기는 각 경로를 위치가 아닌 이름을 기준으로 처리하므로, 특정 경로 순서에 의존해서는 안 됩니다.

RowBinary 포맷에서 각 JSON 행은 다음과 같이 직렬화됩니다:

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**예시:**

**1. 유형이 지정된 경로(path)만 있는 단순 JSON:**

Schema: `JSON(user_id UInt32, active Bool)`

행: `{"user_id": 42, "active": true}`

바이너리 인코딩 (어노테이션이 포함된 16진수):

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. 유형이 지정된 경로와 동적(Dynamic) 경로를 포함한 단순 JSON:**

Schema: `JSON(user_id UInt32, active Bool)`

행: `{"user_id": 42, "active": true, "name": "Alice"}`

이진 인코딩(어노테이션이 포함된 16진수):

```text
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

**3. NULL 처리:**

유형이 지정된 널 허용(Nullable) 컬럼을 사용하면 null이 반환됩니다:

스키마(Schema): `JSON(score Nullable(Int32))`

행: `{"score": null }`

바이너리 인코딩 (어노테이션이 포함된 16진수):

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

타입이 지정된 널 비허용 컬럼의 경우 기본값이 반환됩니다:

Schema: `JSON(name String)`

행: `{"name": null}`

바이너리 인코딩:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

동적 경로의 경우 무시됩니다:

스키마(Schema): `JSON(id UInt64)`

행: `{"id": 100, "metadata": null}`

바이너리 인코딩:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

참고: NULL 값을 가진 `metadata` 경로는 **포함되지 않습니다**. 동적 경로(dynamic path)는 non-null인 경우에만 직렬화되기 때문입니다. 이는 타입이 지정된 경로(typed path)와의 핵심적인 차이점입니다.

**4. 중첩된 JSON 객체(Nested JSON objects):**

스키마: `JSON()`

행: `{"user": {"name": "Bob", "age": 30}}`

이진 인코딩(어노테이션을 포함한 16진수):

```text
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

참고: 중첩 객체는 점(.)으로 구분된 경로로 평탄화됩니다(예: 중첩 구조 대신 `user.name`).

**대안: JSON을 String 모드로 사용**

설정 `output_format_binary_write_json_as_string=1`을 사용하면 JSON 컬럼은 구조화된 바이너리 형식 대신 단일 JSON 텍스트 문자열로 직렬화됩니다. JSON 컬럼에 쓰기 위한 대응 설정으로 `input_format_binary_read_json_as_string`도 있습니다. 여기서 어떤 설정을 사용할지는 JSON을 클라이언트에서 파싱할지, 서버에서 파싱할지에 따라 달라집니다.

<div id="geo-types">
  ### Geo 타입
</div>

Geo는 지리 데이터를 나타내는 데이터 타입의 범주입니다. 다음이 포함됩니다.

* `Point` - `Tuple(Float64, Float64)` 형태입니다.
* `Ring` - `Array(Point)` 또는 `Array(튜플(Float64, Float64))` 형태입니다.
* `Polygon` - `Array(Ring)` 또는 `Array(Array(튜플(Float64, Float64)))` 형태입니다.
* `MultiPolygon` - `Array(Polygon)` 또는 `Array(Array(Array(튜플(Float64, Float64))))` 형태입니다.
* `LineString` - `Array(Point)` 또는 `Array(튜플(Float64, Float64))` 형태입니다.
* `MultiLineString` - `Array(LineString)` 또는 `Array(Array(튜플(Float64, Float64)))` 형태입니다.

Geo 값의 wire 형식은 튜플 및 Array와 정확히 동일합니다. `RowBinaryWithNamesAndTypes` 포맷 헤더에는 이러한 타입의 별칭이 포함되며, 예를 들면 `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString`, `MultiLineString`가 있습니다.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```text
// Point - or Tuple(Float64, Float64)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
// Ring - or Array(Tuple(Float64, Float64))
0x02, // LEB128 - the "ring" array has 2 points
   // Ring - Point #1
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
   // Ring - Point #2
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
// Polygon - or Array(Array(Tuple(Float64, Float64)))
0x02, // LEB128 - the "polygon" array has 2 rings
   0x02, // LEB128 - the first ring has 2 points
      // Polygon - Ring #1 - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
      // Polygon - Ring #1 - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
  0x01, // LEB128 - the second ring has 1 point
      // Polygon - Ring #2 - Point #1 (the only one)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, 
// MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
0x01, // LEB128 - the "multi_polygon" array has 1 polygon
   0x02, // LEB128 - the first polygon has 2 rings
      0x02, // LEB128 - the first ring has 2 points
         // MultiPolygon - Polygon #1 - Ring #1 - Point #1
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
         // MultiPolygon - Polygon #1 - Ring #1 - Point #2
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
      0x01, // LEB128 - the second ring has 1 point
        // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
 // LineString - or Array(Tuple(Float64, Float64))
 0x02, // LEB128 - the line string has 2 points
    // LineString - Point #1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
    // LineString - Point #2
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
 // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
 0x02, // LEB128 - the multi line string has 2 line strings
   0x02, // LEB128 - the first line string has 2 points
     // MultiLineString - LineString #1 - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
     // MultiLineString - LineString #1 - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
   0x01, // LEB128 - the second line string has 1 point
     // MultiLineString - LineString #2 - Point #1 (the only one)
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40,
```

<div id="geometry">
  ### Geometry
</div>

`Geometry`는 위에 나열된 Geo 타입을 모두 담을 수 있는 `Variant` 타입입니다. wire 상에서는 `Variant`와 정확히 동일한 방식으로 인코딩되며, 뒤에 오는 geo 타입을 나타내는 판별자 바이트가 포함됩니다.

Geometry의 판별자 인덱스는 다음과 같습니다.

| 인덱스 | 유형              |
| --- | --------------- |
| 0   | LineString      |
| 1   | MultiLineString |
| 2   | MultiPolygon    |
| 3   | Point           |
| 4   | Polygon         |
| 5   | Ring            |

wire 형식 구조:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

`Point`를 `Geometry`로 인코딩하는 예시:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

`Ring`을 `Geometry`로 인코딩한 예:

```text
0x05,       // discriminant = 5 (Ring)
0x02,       // LEB128 - array has 2 points
// Point #1
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
// Point #2
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
```

<div id="nested">
  ### Nested
</div>

`Nested`의 wire 형식은 `flatten_nested` 설정에 따라 달라집니다.

:::warning
하나의 행에 있는 모든 구성 요소 배열은 **반드시 길이가 같아야 합니다**. 이는 서버에서 강제되는 제약 조건입니다. 길이가 일치하지 않으면 삽입 오류가 발생합니다.
:::

<div id="nested-flattened">
  #### `flatten_nested = 1` (default)
</div>

기본 설정에서는 `Nested`가 각각의 독립적인 배열로 펼쳐집니다. 각 하위 컬럼은 점으로 구분된 이름을 갖는 별도의 `Array` 컬럼이 됩니다:

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo`는 평면화된 컬럼을 표시합니다:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

각 배열은 [배열](#array) 섹션에서 설명한 대로 독립적으로 직렬화됩니다:

```text
0x02,                   // LEB128 - 2 String elements in the first array (n.a)
 0x03,                   // LEB128 - the first string has 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x03,                   // LEB128 - the second string has 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
0x02,                   // LEB128 - 2 Int32 elements in the second array (n.b)
 0x2A, 0x00, 0x00, 0x00, // 42 as Int32
 0x90, 0x00, 0x00, 0x00, // 144 as Int32
```

<div id="nested-unflattened">
  #### `flatten_nested = 0`
</div>

`flatten_nested = 0`으로 설정하면 `Nested`는 `Array(튜플(...))` 유형의 단일 컬럼으로 유지됩니다. 컬럼 이름은 점으로 구분되지 않습니다:

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

`DESCRIBE TABLE foo`는 하나의 컬럼을 표시합니다:

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

인코딩은 `Array(튜플(String, Int32))`입니다: 먼저 배열 길이 프리픽스가 오고, 그다음 각 요소의 튜플 필드가 순서대로 옵니다:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

평탄화된 표현에서처럼 컬럼(a₁, a₂, b₁, b₂)별로 묶이는 것이 아니라, 필드가 요소별로(a₁, b₁, a₂, b₂) 번갈아 배치된다는 점에 유의하십시오.

<div id="simpleaggregatefunction">
  ### SimpleAggregateFunction
</div>

`SimpleAggregateFunction(func, T)`는 내부 데이터 타입 `T`와 동일한 방식으로 인코딩됩니다. 집계 함수 이름은 wire 형식에 영향을 미치지 않습니다.

예를 들어, `SimpleAggregateFunction(max, UInt32)`는 일반 `UInt32`와 동일한 방식으로 인코딩됩니다:

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

RowBinaryWithNamesAndTypes 헤더는 유형을 `SimpleAggregateFunction(max, UInt32)`로 표시하지만, wire 상의 값은 단순히 `UInt32`입니다:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

<div id="aggregatefunction">
  ### AggregateFunction
</div>

`AggregateFunction(func, T)`은 집계 함수의 전체 중간 상태를 저장합니다. `SimpleAggregateFunction`도 중간 상태를 저장하지만 이를 기반 데이터 타입과 동일한 방식으로 인코딩하는 반면, `AggregateFunction`은 각 집계 함수별로 포맷이 다른 불투명한 이진 blob을 저장합니다.

:::warning
집계 상태에는 RowBinary에서 **길이 접두사(length prefix)가 없습니다**. 따라서 파서는 몇 바이트를 읽어야 하는지 판단하려면 각 집계 함수별 내부 serialization 포맷을 이해해야 합니다. 실제로는 대부분의 클라이언트가 집계 상태를 불투명한 값으로 취급하고, serialization은 서버가 처리하도록 `*State` / `*Merge` combinator를 사용합니다.
:::

내부 포맷은 함수마다 다릅니다. 간단한 예시는 다음과 같습니다.

**`countState`** — count를 VarUInt (LEB128)로 저장합니다:

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — 누적된 합계를 고정 크기 정수에 저장합니다. 비트 너비는 인수 유형에 따라 달라집니다(정수 인수의 경우 `UInt64`):

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — 플래그 바이트 뒤에 기본 유형의 값을 저장합니다. 플래그는 빈 상태(관측된 값이 없음)일 때 `0x00`이고, 값이 있으면 `0x01`입니다:

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

빈 상태(집계된 행이 없음):

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
`uniq`, `quantile`, `groupArray`와 같은 더 복잡한 함수는 구현에 따라 다른 포맷을 사용합니다. 이러한 상태를 읽거나 써야 한다면, 해당 함수의 구체적인 구현은 ClickHouse 소스 코드를 참조하십시오.
:::

<div id="qbit">
  ### QBit
</div>

`QBit`은 다양한 정밀도 수준에서 효율적으로 조회할 수 있는 벡터 타입입니다. 내부적으로는 전치 포맷으로 저장됩니다. 전송 시 QBit은 기본 요소 타입(`Int8`, `Float32`, `Float64` 또는 `BFloat16`)의 `Array`일 뿐입니다. 저장을 위한 bit-transpose 최적화는 `RowBinary` 프로토콜이 아니라 서버 측에서 수행됩니다.

구문:

```text
QBit(element_type, dimension[, stride])
```

여기서 `element_type`은 `Int8`, `Float32`, `Float64` 또는 `BFloat16`이며, `dimension`은 고정된 벡터 차원입니다. 선택적 `stride`는 서버 측에서 비트 평면이 저장 스트림으로 그룹화되는 방식만 제어할 뿐, 항상 `dimension`개 요소로 이루어진 전체 배열인 RowBinary wire 형식에는 영향을 주지 않습니다.

wire 형식: `Array(element_type)`와 동일합니다.

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

`[1.0, 2.0, 3.0, 4.0]`를 포함하는 `QBit(Float32, 4)`의 예시 인코딩:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```text
0x04,                   // LEB128 - array has 4 elements
0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
```

<div id="format-settings">
  ## 포맷 설정
</div>

<RowBinaryFormatSettings />