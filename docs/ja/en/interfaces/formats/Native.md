---
alias: []
description: 'Native フォーマットのドキュメント'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`Native` フォーマットの完全な公式仕様は[こちら](/ja/interfaces/specs/NativeFormat)にあり、それを伝送する TCP ワイヤプロトコルである `Native` プロトコルの対応する仕様は[こちら](/ja/interfaces/specs/NativeProtocol)にあります。

:::note
これら両方の仕様は、ClickHouse のソースコードから LLM によって生成されたものです。コードが引き続き主要な信頼できる情報源であり、仕様とコードに不一致がある場合は、コードが正しいものとします。
:::

`Native` フォーマットは、カラムを行に変換しない真の「列指向」フォーマットであるため、ClickHouse でもっとも効率的なフォーマットです。

このフォーマットでは、データはバイナリ形式で[ブロック](/ja/development/architecture#block)単位に書き込まれ、読み取られます。
各ブロックでは、行数、カラム数、カラム名と型、そしてブロック内の各カラムのデータ部分が順に記録されます。

これは、サーバー間のやり取り、コマンドラインクライアントの利用、および C++ クライアントで使用されるネイティブインターフェイスのフォーマットです。

:::tip
このフォーマットを使うと、ClickHouse DBMS でしか読み取れないダンプをすばやく生成できます。
ただし、このフォーマットを直接扱うのは現実的でない場合があります。
:::

<div id="data-types-wire-format">
  ## データ型のワイヤ形式
</div>

データは列指向フォーマットでワイヤ上を送信されます。つまり、各カラムは個別に送信され、
各カラムのすべての値は 1 つの配列としてまとめて送信されます。

ブロック内の各カラムには、[RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md) と同様のヘッダーが含まれます。

:::note
ネイティブ TCP バイナリプロトコルを使用する場合 (または HTTP エンドポイントが `?client_protocol_version=<n>` を受信する場合) 、
カラム数と行数の前に `BlockInfo` 構造体が書き込まれます。このセクションの例では、
プロトコルバージョンを指定しないプレーンな HTTP インターフェイスを使用しているため、`BlockInfo` は省略されます。
:::

<div id="block-structure">
  ### ブロック構造
</div>

次のクエリは、`number` と `str` の2つのカラムからなる3行を返します。

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

出力データは 1 つの ClickHouse ブロックに収まり、次のようになります：

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
  ### 複数のブロック
</div>

ただし、多くの場合、データは1つのブロックには収まらないため、ClickHouse はデータを複数のブロックに分けて送信します。
次のクエリでは、ブロックサイズを小さくして、データが1ブロックあたり1行に分割されるようにしたうえで、2行を取得します。

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

出力:

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
  ### 単純なデータ型
</div>

比較的単純なデータ型の個々の値に対するワイヤ形式は、`RowBinary`/`RowBinaryWithNamesAndTypes` とほぼ同じです。
この説明に該当する型の一覧は次のとおりです。

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

詳細については、[&quot;RowBinary のデータ型のワイヤ形式&quot;](/ja/interfaces/formats/RowBinary#data-types-wire-format) にある上記の型の説明を参照してください。

<div id="complex-data-types">
  ### 複合データ型
</div>

以下の型のエンコード形式は、`RowBinary` および `RowBinaryWithNamesAndTypes` とは異なります。

* Nullable
* LowCardinality
* Array
* Map
* Variant
* Dynamic
* JSON

<div id="nullable">
  #### Nullable
</div>

`Native` フォーマットでは、Nullable カラムでは実際のデータの前に、ブロック内の行数と同じ数のバイトが配置されます。各バイトは、その値が `NULL` かどうかを示します。たとえば、このクエリでは各奇数が代わりに `NULL` になります。

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

出力は次のようになります：

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

`Nullable(String)` でも同様です。null インジケーターは常に nullable のマスクバイトに由来し、
マスク値が `0x01` の場合は、文字列の内容に関係なくその行は `NULL` です。`NULL` の行では、
実際の文字列は空文字列 (LEB128 長 `0`) として保存されます。なお、`NULL` ではない空文字列も
LEB128 長は `0` であるため、この 2 つを区別できるのはマスクバイトだけです。たとえば、次のクエリです。

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

出力は次のようになります。

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

`LowCardinality` が透過的な [RowBinary](RowBinary/RowBinary.md#lowcardinality) とは異なり、Nativeフォーマットでは辞書ベースの列指向エンコーディングを使用します。カラムは、バージョンプレフィックス、続いて一意な値の辞書、その辞書を参照する整数の索引の Array としてエンコードされます。

:::note
カラムは `LowCardinality(Nullable(T))` として定義できますが、`Nullable(LowCardinality(T))` として定義することはできません。これは常にサーバーエラーになります。
:::

バージョンプレフィックスは値 `1` の `UInt64(LE)` で、カラムごとに 1 回書き込まれます。続いて、ブロックごとに次の内容が書き込まれます。

* `UInt64(LE)` — `IndexesSerializationType` のビットフィールド。ビット 0–7 は索引の幅をエンコードします (0 = UInt8、1 = UInt16、2 = UInt32、3 = UInt64) 。ビット 8 (`NeedGlobalDictionaryBit`) は Nativeフォーマットでは設定されません (これが現れた場合、サーバーは例外をスローします) 。ビット 9 は追加の辞書キーが存在することを示します。ビット 10 は辞書をリセットすべきことを示します。
* `UInt64(LE)` — 辞書キーの数。続いて、内部型のエンコーディングを使ってキーが一括シリアライズされます。
* `UInt64(LE)` — 行数。続いて、適切な UInt 幅を使って索引値が一括シリアライズされます。

辞書には、索引 0 に常にデフォルト値が含まれます (たとえば `String` では空文字列、数値型では 0) 。`LowCardinality(Nullable(T))` の場合、索引 0 は `NULL` を表し、キーは `Nullable` ラッパーなしでシリアライズされます。

たとえば、5 行 `['foo', 'bar', 'baz', 'foo', 'bar']` を持つ `LowCardinality(String)` は次のようになります。

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

`LowCardinality(Nullable(String))` では、インデックス 0 が `NULL` です:

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
  #### Array
</div>

各配列の先頭に LEB128 の要素数が付与される [RowBinary](RowBinary/RowBinary.md#array) とは異なり、Native フォーマット では配列は次の 2 つの列指向サブストリームとしてエンコードされます。

* 累積 `UInt64` オフセットが N 個 (リトルエンディアン、各 8 バイト) 。行 `i` の要素数は `offset[i] - offset[i-1]` で、`offset[-1]` は暗黙的に 0 とみなされます。
* すべての行にまたがるネストされた要素全体。連続した領域にまとめてシリアライズされます。

たとえば、3 行 `[[0, 10], [1, 11], [2, 12]]` を持つ `Array(UInt32)` の場合:

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

空の配列のオフセットは、前の行と同じになります。たとえば、4 行の `Array(String)` `[[], ['0'], ['0','1'], ['0','1','2']]` は次のようになります。

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
  #### Map
</div>

`Map(K, V)` は `Array(Tuple(K, V))` としてエンコードされます。つまり、配列のオフセットに続いて、すべてのキー、その後にすべての値が並びます。これは、[RowBinary](RowBinary/RowBinary.md#map) では各エントリごとにキーと値が交互に配置されるのとは異なります。

たとえば、3 行の `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]` を持つ `Map(String, UInt64)` の場合:

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

各行がそれぞれ判別子バイトを持ち、その直後に値がインラインで続く [RowBinary](RowBinary/RowBinary.md#variant) とは異なり、Native フォーマット では判別子とデータは分離されます。

:::warning
RowBinary と同様に、定義内の型は常にアルファベット順にソートされ、判別子はそのソート済みリスト内の索引になります。`0xFF` (255) は `NULL` を表します。
:::

`Variant` カラムは次のようにエンコードされます。

* `UInt64(LE)` の discriminator モードのプレフィックス (`0` = BASIC、`1` = COMPACT) 。Native フォーマット の出力では通常 BASIC (`0`) が使用されます。COMPACT モードは、`use_compact_variant_discriminators_serialization` を有効にして保存されたデータを読み取る際に現れることがあります。
* N 個の `UInt8` 判別子。各行に 1 つ。
* 各 variant type のデータは、対応する行のみを含む個別の bulk column として、判別子の順に格納されます。

たとえば、5 行 `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` を持つ `Variant(String, UInt32)` の場合 (ソート順: `String` = 0、`UInt32` = 1) :

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

各値が自己記述的 (型プレフィックス + 値) である [RowBinary](RowBinary/RowBinary.md#dynamic) とは異なり、Native フォーマット では `Dynamic` は structure prefix の後に [Variant](#variant) カラムが続く形でシリアライズされます。

structure prefix には、`UInt64(LE)` のシリアル化バージョン、動的型の数 (VarUInt として) 、続いて型名が文字列として含まれます。バージョン V1 では、互換性のために型の数が 2 回書き込まれます。後続のデータは `Variant` カラムで、その type list は動的型に内部的な `SharedVariant` 型を加えたもので、アルファベット順にソートされています。

たとえば、5 行の `Dynamic` `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` は次のように表されます。

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

パス名と値によって各行が自己記述型になっている [RowBinary](RowBinary/RowBinary.md#json) とは異なり、Native フォーマット は `JSON` を列指向構造でシリアライズします。このエンコードは複雑で、バージョンにも依存します。具体的には、シリアル化バージョン、動的なパス名、共有データレイアウトを含む structure prefix で構成され、その後に型付きパス (それぞれが bulk column) 、動的パス (それぞれが [Dynamic](#dynamic) カラム) 、さらに overflow パス用の共有データが続きます。

よりシンプルな相互運用性を重視する場合は、設定 `output_format_native_write_json_as_string=1` の使用を検討してください。これにより、JSON カラムは通常の JSON テキスト文字列 (各行につき `String` 1 つ) としてシリアライズされます。