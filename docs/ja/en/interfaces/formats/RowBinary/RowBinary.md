---
alias: []
description: 'RowBinary フォーマットに関するドキュメント'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`RowBinary` フォーマットは、データをバイナリ形式で行単位に解析します。
行と値は区切りなしで連続して並びます。
データはバイナリ形式であるため、`FORMAT RowBinary` の後の区切りは、厳密に次のように定められています。

* 任意個の空白文字:
  * `' '` (スペース - code `0x20`) 
  * `'\t'` (タブ - code `0x09`) 
  * `'\f'` (フォームフィード - code `0x0C`) 
* その後に、ちょうど 1 つの改行シーケンス:
  * Windows スタイル `"\r\n"`
  * または Unix スタイル `'\n'`
* その直後にバイナリデータが続きます。

:::note
このフォーマットは行ベースのため、[Native](../Native.md) フォーマットより効率が劣ります。
:::

<div id="data-types-wire-format">
  ## データ型のワイヤ形式
</div>

:::tip
例にあるクエリのほとんどは、出力をファイルに指定して `curl` で実行できます。

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```

:::

その後、データは16進エディタで確認できます。

<div id="unsigned-leb128">
  ### 符号なし LEB128 (リトルエンディアン Base 128)
</div>

`String`、`Array`、`Map` などの可変サイズのデータ型の長さをエンコードするために使われる、**符号なしリトルエンディアン**の可変長整数エンコーディングです。実装例は [LEB128 の Wikipedia ページ](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer)で確認できます。

<div id="integer-types">
  ### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
</div>

すべての整数型は、適切なバイト数の **リトルエンディアン** 形式でエンコードされます。符号付き型 (`Int8` から `Int256`) では、**2 の補数** 表現が使用されます。ほとんどの言語では、組み込み機能または広く利用されているライブラリを使って、この種の整数をバイト配列から取り出せます。`Int128`/`Int256` および `UInt128`/`UInt256` は、多くの言語のネイティブな整数サイズを超えるため、独自のデシリアライゼーションが必要になる場合があります。

<div id="bool">
  ### Bool
</div>

ブール値は 1 バイトでエンコードされ、`UInt8` と同様にデシリアライズできます。

* `0` は `false`
* `1` は `true`

<div id="float32-float64">
  ### Float32, Float64
</div>

`Float32` は4バイト、`Float64` は8バイトでエンコードされる**リトルエンディアン**の浮動小数点数です。整数と同様に、ほとんどの言語にはこれらの値をデシリアライズするための適切な手段が用意されています。

<div id="bfloat16">
  ### BFloat16
</div>

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point) は、Float32 と同じ範囲を持ちながら精度を抑えた 16 ビットの浮動小数点フォーマットで、機械学習のワークロードに適しています。ワイヤ形式は、基本的には Float32 値の上位 16 ビットです。使用している言語でこれがネイティブにサポートされていない場合は、UInt16 として読み書きし、Float32 との間で相互変換するのが最も簡単です。

BFloat16 を Float32 に変換するには (擬似コード) :

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

Float32をBFloat16に変換するには (擬似コード) :

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

`BFloat16` の内部値の例:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

<div id="decimal">
  ### Decimal32, Decimal64, Decimal128, Decimal256
</div>

Decimal 型は、それぞれのビット幅を持つ**リトルエンディアン**整数として表現されます。

* `Decimal32` - 4 バイト、または `Int32`。
* `Decimal64` - 8 バイト、または `Int64`。
* `Decimal128` - 16 バイト、または `Int128`。
* `Decimal256` - 32 バイト、または `Int256`。

Decimal 値をデシリアライズする際は、整数部と小数部を次の擬似コードで求められます。

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

ここで `trunc` は 0 方向への切り捨てを行い (負の値では結果が異なる床除算ではありません) 、`scale` は小数点以下の桁数です。たとえば、`Decimal(10, 2)` (`Decimal32(2)` と等価) では、`scale` は `2` で、値 `12345` は `(123, 45)` として表されます。

シリアライゼーションでは、この逆の操作が必要です。

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

詳細は、[Decimal types の ClickHouse Docs](https://clickhouse.com/docs/sql-reference/data-types/decimal)をご覧ください。

<div id="string">
  ### String
</div>

ClickHouse の文字列は、**任意のバイト列**です。有効な UTF-8 である必要はありません。長さプレフィックスは**バイト長**であり、文字数ではありません。

次の 2 つの部分でエンコードされます。

1. 文字列の長さをバイト単位で示す可変長整数 (LEB128) 。
2. 文字列の生のバイト列。

たとえば、文字列 `foobar` は次のように *7* バイトでエンコードされます。

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

`String` とは異なり、`FixedString` は長さが固定されており、その長さはスキーマで定義されます。バイト列としてエンコードされ、値が `N` より短い場合は末尾がゼロバイトで埋められます。

:::note
`FixedString` を読み取る際、末尾のゼロバイトはパディングの場合もあれば、データ中の実際の `\0` 文字の場合もあり、伝送上は区別できません。ClickHouse 自体は、`N` バイトをすべてそのまま保持します。
:::

空の `FixedString(3)` には、パディング用のゼロだけが含まれます。

```text
0x00, 0x00, 0x00
```

文字列 `hi` を含む空でない `FixedString(3)`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

文字列 `bar` を含む空でない `FixedString(3)`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

最後の例では、*3* バイトすべてが使われているため、パディングは不要です。

<div id="date">
  ### Date
</div>

`1970-01-01` からの経過日数を表す `UInt16` (2 バイト) として格納されます。

サポートされる値の範囲: `[1970-01-01, 2149-06-06]`。

`Date` の内部値の例:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (リトルエンディアン) = 19737 days since 1970-01-01
```

<div id="date32">
  ### Date32
</div>

`1970-01-01` から&#x306E;***前後の日数***&#x3092;表す `Int32` (4 バイト) として保存されます。

サポートされる値の範囲: `[1900-01-01, 2299-12-31]`。

`Date32` の内部値の例:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (リトルエンディアン) = 19737 days since 1970-01-01
```

エポックより前の日付:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (リトルエンディアン) = 25567 days before 1970-01-01
```

<div id="datetime">
  ### DateTime
</div>

`1970-01-01 00:00:00 UTC` ***からの*** 秒数を表す `UInt32` (4バイト) として格納されます。

構文:

```text
DateTime([timezone])
```

たとえば、`DateTime` または `DateTime('UTC')` です。

:::note
バイナリ値は常に UTC エポックからのオフセットです。タイムゾーンによってエンコードが変わることはありません。ただし、INSERT 時に文字列値がどのように解釈されるかには、タイムゾーンが**実際に**影響します。たとえば、`DateTime('America/New_York')` カラムに `'2024-01-15 10:30:00'` を挿入すると、同じ文字列を `DateTime('UTC')` カラムに挿入した場合とは異なるエポック値が格納されます。これは、その文字列がカラムのタイムゾーンにおけるローカル時刻として解釈されるためです。ワイヤ形式では、どちらも単なる `UInt32` のエポック秒です。
:::

サポートされる値の範囲: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`。

`DateTime` の内部値の例:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (リトルエンディアン)
```

<div id="datetime64">
  ### DateTime64
</div>

`1970-01-01 00:00:00 UTC` よ&#x308A;***前後***&#x306E; **ticks** 数を表す `Int64` (8バイト) として格納されます。tick の分解能は `precision` パラメーターで定義されます。以下の構文を参照してください。

```text
DateTime64(precision, [timezone])
```

`precision` は `0` から `9` までの整数です。通常使用されるのは、`3` (ミリ秒) 、`6` (マイクロ秒) 、
`9` (ナノ秒) のみです。

有効な DateTime64 定義の例: `DateTime64(0)`、`DateTime64(3)`、`DateTime64(6, 'UTC')`、`DateTime64(9, 'Europe/Amsterdam')`。

:::note
`DateTime` と同様に、バイナリ値は常に UTC エポックからのオフセットです。タイムゾーンは、文字列値が INSERT 時にどのように解釈されるかに影響します ([DateTime](#datetime) の注記を参照) 。ただし、エンコード自体は常に UTC エポックからの `Int64` tick です。
:::

`DateTime64` 型の内部の `Int64` 値は、UNIX epoch の前後における次の単位数として解釈できます。

* `DateTime64(0)` - 秒。
* `DateTime64(3)` - ミリ秒。
* `DateTime64(6)` - マイクロ秒。
* `DateTime64(9)` - ナノ秒。

サポートされる値の範囲: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`。

`DateTime64` の内部値の例:

* `DateTime64(3)`: 値 `1546300800000` は `2019-01-01 00:00:00 UTC` を表します。
* `DateTime64(6)`: 値 `1705314600123456` は `2024-01-15 10:30:00.123456 UTC` を表します。
* `DateTime64(9)`: 値 `1705314600123456789` は `2024-01-15 10:30:00.123456789 UTC` を表します。

:::note
最大値の精度は 8 桁です。最大精度の 9 桁 (ナノ秒) を使用する場合、サポートされる最大値は UTC で 2262-04-11 23:47:16 です。
:::

<div id="time">
  ### Time
</div>

秒単位の時刻値を表す `Int32` として格納されます。負の値も有効です。

サポートされる値の範囲は `[-999:59:59, 999:59:59]` (つまり `[-3599999, 3599999]` 秒) です。

:::note
現時点では、`Time` または `Time64` を使用するには、設定 `enable_time_time64_type` を `1` に設定する必要があります。
:::

`Time` の内部値の例:

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

内部的には `Decimal64` (`Int64` として保存) で格納され、小数秒を含む時刻の値を表します。精度は設定可能です。負の値も有効です。

構文:

```text
Time64(precision)
```

`precision` は `0` から `9` までの整数です。一般的な値は、`3` (ミリ秒) 、`6` (マイクロ秒) 、`9` (ナノ秒) です。

サポートされる値の範囲は `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]` です。

:::note
現時点では、`Time` または `Time64` を使用するには、設定 `enable_time_time64_type` を `1` に設定する必要があります。
:::

内部の `Int64` 値は、`10^precision` 倍にスケーリングされた秒の小数部を表します。

`Time64` の内部値の例:

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
  ### インターバル型
</div>

すべてのインターバル型は `Int64` (8バイト、リトルエンディアン) として格納されます。この値は、対応する時間単位の個数を表します。負の値も有効です。

インターバル型は次のとおりです: `IntervalNanosecond`、`IntervalMicrosecond`、`IntervalMillisecond`、`IntervalSecond`、`IntervalMinute`、`IntervalHour`、`IntervalDay`、`IntervalWeek`、`IntervalMonth`、`IntervalQuarter`、`IntervalYear`。

:::note
インターバルの型名 (たとえば `IntervalSecond` と `IntervalDay`) によって、格納される値の単位が決まります。ワイヤエンコーディングは常に同じです。
:::

内部値の例:

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

enum 定義内での enum 値の索引を表す 1 バイト (`Enum8` == `Int8`) または 2 バイト (`Enum16` == `Int16`) として格納されます。ストレージ型は**符号付き**である点に注意してください。つまり、enum 値には負の値を指定できます (例: `Enum8('a' = -128, 'b' = 0)`) 。

Enum は、次のようにシンプルに定義できます。

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

上記で定義したEnum8では、クライアント側のvalues mapは次のようになります:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

あるいは、次のように、より複雑な方法で表すこともできます。

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

上で定義したEnum16は、クライアント側では次のような値の対応表になります：

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

データ型パーサーにとっての主な課題は、`\'` のような enum 定義内のエスケープされた記号や、引用符付き文字列内に現れる可能性のある `=` のような特殊記号を正しく追跡することです。

<div id="uuid">
  ### UUID
</div>

16 バイトの数列として表現されます。UUID は **2 つのリトルエンディアン `UInt64` 値**として格納されます。標準的な UUID 表現の先頭 8 バイトはバイト順が反転され、後続の 8 バイトも個別にバイト順が反転されます。

たとえば、UUID `61f0c404-5cb3-11e7-907b-a6006ad3dba0` が与えられた場合:

* 標準的なバイト表現: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
* 前半を反転 (LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
* 後半を反転 (LE UInt64): `a0 db d3 6a 00 a6 7b 90`

`UUID` の内部値の例:

* `61f0c404-5cb3-11e7-907b-a6006ad3dba0` は次のように表現されます:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

* 既定の UUID `00000000-0000-0000-0000-000000000000` は、16バイトすべてが 0 として表されます:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

新しいレコードが挿入されたが、UUID が指定されていない場合に使用できます。

<div id="ipv4">
  ### IPv4
</div>

4 バイトの `UInt32` として **リトルエンディアン** のバイト順で格納されます。これは、IP アドレスで一般的に使用される従来のネットワークバイトオーダー (ビッグエンディアン) とは異なる点に注意してください。`IPv4` の内部値の例:

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

**ビッグエンディアン / ネットワークバイトオーダー** (MSB が先頭) の 16 バイトで格納されます。`IPv6` の内部値の例:

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
  ### Nullable
</div>

Nullable データ型は、次のようにエンコードされます。

1. 値が `NULL` かどうかを示す 1 バイト:
   * `0x00` は、値が `NULL` ではないことを示します。
   * `0x01` は、値が `NULL` であることを示します。
2. 値が `NULL` でない場合は、基になるデータ型が通常どおりエンコードされます。値が `NULL` の場合は、基になる型に対して **追加のバイトは一切** 書き込まれません。

たとえば、`Nullable(UInt32)` 型の値:

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

RowBinary フォーマットでは、low-cardinality マーカーはワイヤ形式に影響しません。たとえば、`LowCardinality(String)` は通常の `String` と同じ方法でエンコードされます。

:::warning
これは RowBinary にのみ適用されます。Native format では、`LowCardinality` は Dictionary ベースの別のエンコードを使用します。
:::

:::note
カラムは `LowCardinality(Nullable(T))` として定義できますが、`Nullable(LowCardinality(T))` として定義することはできません。これは常にサーバーからエラーになります。
:::

テスト時には、カバレッジを高めるために、[allow&#95;suspicious&#95;low&#95;cardinality&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) を `1` に設定して、`LowCardinality` 内でほとんどのデータ型を許可できます。

<div id="array">
  ### Array
</div>

Array は次のようにエンコードされます。

1. 配列の要素数を示す [可変長整数 (LEB128) ](#unsigned-leb128)。
2. 配列の各要素。基になるデータ型と同じ方法でエンコードされます。

たとえば、値が `UInt32` の配列は次のようになります。

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

もう少し複雑な例:

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
配列には Nullable な値を含めることはできますが、配列自体を Nullable にすることはできません。
:::

以下は有効です。

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

次のようにエンコードされます:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

多次元配列の扱いに関する例は、[Geo セクション](#geo-types)にあります。

<div id="tuple">
  ### Tuple
</div>

Tuple は、追加のメタ情報や区切り文字を含まず、すべての要素をそれぞれ対応するワイヤ形式で順に並べた形でエンコードされます。

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

Tuple データ型の文字列表現では、エスケープされた記号や特殊文字を追跡する必要があるなど、[Enum type](#enum8-enum16) と同様の課題があります。さらに、Tuple では開き括弧と閉じ括弧も追跡しなければなりません。加えて、複雑な Tuple には、ネストされたほかの Tuple、Array、Map、さらには enum まで含まれることがある点にも注意してください。

たとえば、次の table では、Tuple に名前の中にバッククォートと括弧を含む enum が含まれており、適切に処理しないとパース上の問題を引き起こす可能性があります。

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
  ### Map
</div>

Map は `Array(Tuple(K, V))` と見なすことができます。ここで、`K` はキーの型、`V` は値の型です。Map は次のようにエンコードされます。

1. Map 内の要素数を示す [可変長整数 (LEB128) ](#unsigned-leb128)。
2. キー・バリューのペアとして表される Map の要素。各要素は対応する型でエンコードされます。

たとえば、キーが `String`、値が `UInt32` の Map は次のとおりです。

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
`Map(String, Map(Int32, Array(Nullable(String))))` のような深くネストされた構造の Map 型も可能で、その場合もエンコード方法は上で説明したものと同様です。
:::

<div id="variant">
  ### Variant
</div>

この型は、他のデータ型のユニオンを表します。型 `Variant(T1, T2, ..., TN)` は、この型の各行が、型 `T1`、`T2`、…、`TN` のいずれか、またはどれにも属さない値 (`NULL` 値) を持つことを意味します。

:::warning
エンドユーザーにとっては `Variant(T1, T2)` と `Variant(T2, T1)` はまったく同じ意味ですが、ワイヤ形式では定義内の型の順序が重要です。定義内の型は常にアルファベット順にソートされます。これは、どの Variant であるかが &quot;判別子&quot;、つまり定義内のデータ型の索引によってエンコードされるためです。
:::

次の例を考えてみましょう。

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

`NULL` 値は、判別子バイト `0xFF` でエンコードされます:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

[allow&#95;suspicious&#95;variant&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) 設定を使用すると、`Variant` 型について、より網羅的なテストを行えます。

<div id="dynamic">
  ### Dynamic
</div>

`Dynamic` 型は、実行時に決まる任意の型の値を保持できます。RowBinary フォーマットでは、各値は自己記述的です。最初の部分には、[このフォーマット](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding)による型指定が入ります。続いて内容が続き、値はこのドキュメントで説明されているとおりにエンコードされます。したがって値をパースするには、型の索引を使って適切なパーサーを判断し、その後はすでに他の箇所で使っている RowBinary のパース処理を再利用するだけで済みます。

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

ここで `BinaryTypeIndex` は、型を識別する 1 バイトの値です。型インデックスとパラメータについては、[こちら](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding)のリファレンスを参照してください。

`NULL` の Dynamic 値は、`BinaryTypeIndex` `0x00` (`Nothing` 型) としてエンコードされ、追加のバイトはありません。

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**例:**

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

JSON型はデータを2つの異なるカテゴリにエンコードします：

1. **型付きパス** - スキーマ内で明示的な型を指定して宣言されたパス (例: `JSON(user_id UInt32, name String)`)
2. **動的パス/動的パス数の上限を超えた場合のオーバーフローパス** - 実行時に検出されたパスは `Dynamic` 型として格納されます。値のエンコードの前に型定義が付加されます。

ワイヤ形式とルールは、この2つのカテゴリで異なります。

| パスのカテゴリ       | シリアライゼーションに含まれるか      | 値のエンコード    | Variant/Nullable の使用可否 |
| ------------- | --------------------- | ---------- | ---------------------- |
| **型付きパス**     | 常に含まれる (NULL の場合も含む)  | 型固有のバイナリ形式 | はい                     |
| **Dynamicパス** | 非 NULL の場合のみ          | Dynamic    | 不可                     |

パスは3つのグループに分けてシリアライズされ、typed paths、dynamic paths、shared data (オーバーフロー) pathsの順に書き込まれます。typed pathsとdynamic pathsはimplementation-definedな順序 (内部ハッシュマップのイテレーションによって決定) で書き込まれ、shared data pathsはアルファベット順で書き込まれます。デシリアライザは各パスを位置ではなく名前によってディスパッチするため、readerは特定のパスの順序に依存しないようにしてください。

RowBinary フォーマットにおける各 JSON 行は、次のようにシリアライズされます：

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**例：**

**1. 型付きパスのみを含むシンプルなJSON：**

スキーマ: `JSON(user_id UInt32, active Bool)`

行: `{"user_id": 42, "active": true}`

バイナリエンコーディング (注釈付き16進数) ：

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. 型付きパスと動的パスを持つシンプルなJSON：**

スキーマ: `JSON(user_id UInt32, active Bool)`

行: `{"user_id": 42, "active": true, "name": "Alice"}`

バイナリエンコーディング (注釈付き16進数) ：

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

**3. NULLの処理:**

型付きNullableカラムの場合、nullが返されます：

スキーマ: `JSON(score Nullable(Int32))`

行: `{"score": null }`

バイナリエンコーディング (注釈付き16進数) ：

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

型付きの非 NULL カラムでは、デフォルト値が取得されます：

Schema: `JSON(name String)`

行: `{"name": null}`

バイナリエンコーディング：

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

動的パスの場合、これは無視されます：

スキーマ: `JSON(id UInt64)`

行: `{"id": 100, "metadata": null}`

バイナリエンコーディング:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

注意: NULL 値を持つ `metadata` パスは、動的パスが非 NULL の場合にのみシリアライズされるため、**含まれません**。これは型付きパスとの重要な差です。

**4. ネストされた JSON オブジェクト:**

スキーマ: `JSON()`

行: `{"user": {"name": "Bob", "age": 30}}`

バイナリエンコーディング (注釈付きの16進表記) :

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

注: ネストされたオブジェクトは、ネスト構造ではなく、ドット区切りのパスにフラット化されます (例: `user.name`) 。

**代替: JSON as String Mode**

`output_format_binary_write_json_as_string=1` を設定すると、JSON カラムは構造化されたバイナリ形式ではなく、1 つの JSON テキスト文字列としてシリアライズされます。JSON カラムへの書き込みには、対応する設定 `input_format_binary_read_json_as_string` があります。ここでどちらの設定を選ぶかは、JSON をクライアント側でパースするか、サーバー側でパースするかによって決まります。

<div id="geo-types">
  ### Geo 型
</div>

Geo は地理データを表すデータ型のカテゴリで、以下が含まれます。

* `Point` - `Tuple(Float64, Float64)` として表現されます。
* `Ring` - `Array(Point)` または `Array(Tuple(Float64, Float64))` として表現されます。
* `Polygon` - `Array(Ring)` または `Array(Array(Tuple(Float64, Float64)))` として表現されます。
* `MultiPolygon` - `Array(Polygon)` または `Array(Array(Array(Tuple(Float64, Float64))))` として表現されます。
* `LineString` - `Array(Point)` または `Array(Tuple(Float64, Float64))` として表現されます。
* `MultiLineString` - `Array(LineString)` または `Array(Array(Tuple(Float64, Float64)))` として表現されます。

Geo の値のワイヤ形式は、Tuple および Array の場合と完全に同じです。`RowBinaryWithNamesAndTypes` フォーマットのヘッダーには、これらの型の別名 (たとえば `Point`、`Ring`、`Polygon`、`MultiPolygon`、`LineString`、`MultiLineString`) が含まれます。

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

`Geometry` は `Variant` 型で、上記の Geo 型のいずれも保持できます。ワイヤ形式では、後続の Geo 型を示す 判別子 バイトを含め、`Variant` とまったく同じようにエンコードされます。

Geometry の 判別子 インデックスは次のとおりです。

| インデックス | 型               |
| ------ | --------------- |
| 0      | LineString      |
| 1      | MultiLineString |
| 2      | MultiPolygon    |
| 3      | Point           |
| 4      | Polygon         |
| 5      | Ring            |

ワイヤ形式の構造:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

`Point` を `Geometry` としてエンコードした例:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

`Ring` を `Geometry` としてエンコードした例:

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

`Nested` のワイヤ形式は、`flatten_nested` 設定によって異なります。

:::warning
1 つの行内にあるすべての構成配列は、**同じ長さである必要があります**。これはサーバー側で強制される制約です。長さが一致しないと、挿入時にエラーが発生します。
:::

<div id="nested-flattened">
  #### `flatten_nested = 1` (デフォルト)
</div>

デフォルト設定では、`Nested` は個別の配列にフラット化されます。各サブカラムは、ドット区切りの名前を持つ独立した `Array` カラムになります。

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo` は展開されたカラムを表示します:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

各Arrayは、[Array](#array) セクションで説明しているとおり、それぞれ独立してシリアライズされます。

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

`flatten_nested = 0` では、`Nested` は `Array(Tuple(...))` 型の1つのカラムとして保持されます。カラム名はドット区切りにはなりません。

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

`DESCRIBE TABLE foo` では、1 つのカラムが表示されます：

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

エンコーディングは `Array(Tuple(String, Int32))` です。まず配列長のプレフィックスがあり、その後に各要素の Tuple のフィールドが順に続きます:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

フラット化された表現のようにカラムごとにまとめられている (a₁, a₂, b₁, b₂) のではなく、フィールドが要素ごとに交互に並んでいる (a₁, b₁, a₂, b₂) ことに注目してください。

<div id="simpleaggregatefunction">
  ### SimpleAggregateFunction
</div>

`SimpleAggregateFunction(func, T)` は、基になるデータ型 `T` とまったく同じ形式でエンコードされます。aggregate function の名前はワイヤ形式に影響しません。

たとえば、`SimpleAggregateFunction(max, UInt32)` は通常の `UInt32` と同じ形式でエンコードされます。

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

RowBinaryWithNamesAndTypes のヘッダーでは型は `SimpleAggregateFunction(max, UInt32)` と示されますが、実際にワイヤ上の値は単なる `UInt32` です:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

<div id="aggregatefunction">
  ### AggregateFunction
</div>

`AggregateFunction(func, T)` は、集約関数の完全な中間状態を格納します。同じく中間状態を格納するものの、その表現が基になるデータ型と同一である `SimpleAggregateFunction` とは異なり、`AggregateFunction` は各集約関数固有のフォーマットを持つ不透明なバイナリブロブを格納します。

:::warning
RowBinary では、集約状態に **長さプレフィックスがありません**。そのため、何バイト読み取るべきかを判断するには、パーサーが各集約関数固有の内部シリアライゼーションフォーマットを理解している必要があります。実際には、ほとんどのクライアントは集約状態を不透明なものとして扱い、`*State` / `*Merge` 集約関数コンビネータを使って、シリアライゼーションの処理をサーバーに任せます。
:::

内部フォーマットは関数ごとに異なります。簡単な例をいくつか示します。

**`countState`** — カウントを VarUInt (LEB128) として格納します。

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — 累積した合計を固定長の整数に格納します。ビット幅は引数の型によって異なります (整数型の引数では `UInt64`) :

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — 基になる型の値が続くフラグバイトを格納します。フラグは、空の状態 (値がまだない) の場合は `0x00`、値がある場合は `0x01` です：

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

空の状態 (集計された行がない場合) :

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
`uniq`、`quantile`、`groupArray` のような、より複雑な関数では、実装固有のフォーマットが使用されます。これらの状態を読み書きする必要がある場合は、対象の関数に対応する ClickHouse のソースコードを参照してください。
:::

<div id="qbit">
  ### QBit
</div>

`QBit` は、異なる精度で効率的にルックアップを行うためのベクトル型です。内部的には転置フォーマットで格納されます。on the wire では、QBit は基底となる要素型 (`Int8`、`Float32`、`Float64`、または `BFloat16`) の `Array` にすぎません。ストレージ向けのビット転置最適化は RowBinary プロトコルではなく、サーバー側で行われます。

構文:

```text
QBit(element_type, dimension[, stride])
```

ここで、`element_type` は `Int8`、`Float32`、`Float64`、または `BFloat16`、`dimension` は固定のベクトル次元です。オプションの `stride` は、ビットプレーンをサーバー側でストレージストリームにどうグループ化するかだけを制御し、`RowBinary` のワイヤ形式には影響しません。`RowBinary` のワイヤ形式は常に `dimension` 個の要素を持つ完全な配列です。

ワイヤ形式: `Array(element_type)` と同一:

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

`[1.0, 2.0, 3.0, 4.0]` を含む `QBit(Float32, 4)` のエンコード例:

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
  ## フォーマット設定
</div>

<RowBinaryFormatSettings />