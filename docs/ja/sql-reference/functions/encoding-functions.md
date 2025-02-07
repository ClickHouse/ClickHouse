---
slug: /ja/sql-reference/functions/encoding-functions
sidebar_position: 65
sidebar_label: エンコーディング
---

# エンコーディング関数

## char

渡された引数の数を長さとし、それぞれの引数に対応するバイト値を持つ文字列を返します。数値型の引数を複数受け取ります。引数の値が UInt8 データ型の範囲外である場合は、丸めやオーバーフローが発生する可能性がありますが、UInt8 に変換されます。

**構文**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**引数**

- `number_1, number_2, ..., number_n` — 整数として解釈される数値引数。型: [Int](../data-types/int-uint.md), [Float](../data-types/float.md)。

**返される値**

- 指定されたバイトの文字列。[String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello;
```

結果:

``` text
┌─hello─┐
│ hello │
└───────┘
```

対応するバイトを渡して任意のエンコーディングの文字列を構築できます。UTF-8の例がここにあります:

クエリ:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

結果:

``` text
┌─hello──┐
│ привет │
└────────┘
```

クエリ:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

結果:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## hex

引数の16進数表現を含む文字列を返します。

別名: `HEX`.

**構文**

``` sql
hex(arg)
```

この関数は大文字の `A-F` を使用し、接頭辞（例えば `0x`）や接尾辞（例えば `h`）は使用しません。

整数引数の場合、重大な桁から最小桁までの16進数の数字（"ニブル"）を印刷します（ビッグエンディアンまたは「人間が読める」順序）。最も重要な非ゼロバイトから始めます（リーディングゼロバイトは省略されます）が、各バイトの先行ゼロの桁がゼロであっても常に両方の桁を印刷します。

型 [Date](../data-types/date.md) および [DateTime](../data-types/datetime.md) の値は、対応する整数としてフォーマットされます（Date の場合はエポック以降の日数、DateTime の場合はUnix タイムスタンプの値）。

[String](../data-types/string.md) および [FixedString](../data-types/fixedstring.md) の場合、すべてのバイトが2つの16進数として単純にエンコードされます。ゼロバイトは省略されません。

型 [Float](../data-types/float.md) および [Decimal](../data-types/decimal.md) の値は、メモリ内での表現としてエンコードされます。リトルエンディアンアーキテクチャをサポートしているため、リトルエンディアンでエンコードされます。ゼロのリーディング/トレーリングバイトは省略されません。

型 [UUID](../data-types/uuid.md) の値は、ビッグエンディアン順序の文字列としてエンコードされます。

**引数**

- `arg` — 16進数に変換する値。型: [String](../data-types/string.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md), [Decimal](../data-types/decimal.md), [Date](../data-types/date.md) もしくは [DateTime](../data-types/datetime.md)。

**返される値**

- 引数の16進数表現を持つ文字列。[String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT hex(1);
```

結果:

``` text
01
```

クエリ:

``` sql
SELECT hex(toFloat32(number)) AS hex_presentation FROM numbers(15, 2);
```

結果:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

クエリ:

``` sql
SELECT hex(toFloat64(number)) AS hex_presentation FROM numbers(15, 2);
```

結果:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

クエリ:

``` sql
SELECT lower(hex(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) as uuid_hex
```

結果:

``` text
┌─uuid_hex─────────────────────────┐
│ 61f0c4045cb311e7907ba6006ad3dba0 │
└──────────────────────────────────┘
```

## unhex

[hex](#hex) の反対の操作を行います。引数内の各ペアの16進数の数字を数値として解釈し、その数値を表すバイトに変換します。返される値はバイナリ文字列（BLOB）です。

結果を数値に変換したい場合は、[reverse](../../sql-reference/functions/string-functions.md#reverse) および [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#type-conversion-functions) 関数を使用できます。

:::note
`unhex`が`clickhouse-client`内から呼び出された場合、バイナリ文字列はUTF-8を使用して表示されます。
:::

別名: `UNHEX`.

**構文**

``` sql
unhex(arg)
```

**引数**

- `arg` — 16進数の数字を含む文字列。[String](../data-types/string.md), [FixedString](../data-types/fixedstring.md)。

大文字と小文字の両方の`A-F`をサポートします。16進数の桁数は偶数である必要はありません。奇数の場合、最後の桁は`00-0F`バイトの最下位半分として解釈されます。引数文字列に16進数の数字以外のものが含まれている場合、実装定義の結果が返されます（例外はスローされません）。数値引数の場合、hex(N)の逆はunhex()によって実行されません。

**返される値**

- バイナリ文字列（BLOB）。[String](../data-types/string.md)。

**例**

クエリ:
``` sql
SELECT unhex('303132'), UNHEX('4D7953514C');
```

結果:
``` text
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
```

クエリ:

``` sql
SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num;
```

結果:

``` text
┌──num─┐
│ 4095 │
└──────┘
```

## bin

引数のバイナリ表現を含む文字列を返します。

**構文**

``` sql
bin(arg)
```

別名: `BIN`.

整数引数の場合、重大な桁から最小桁までのbinの数字を印刷します（ビッグエンディアンまたは「人間が読める」順序）。最も重要な非ゼロバイトから始めます（リーディングゼロバイトは省略されます）が、各バイトの先行ゼロの桁がゼロであっても常に8桁を印刷します。

型 [Date](../data-types/date.md) および [DateTime](../data-types/datetime.md) の値は、対応する整数としてフォーマットされます（Date の場合はエポック以降の日数、DateTime の場合は Unix タイムスタンプの値）。

[String](../data-types/string.md) および [FixedString](../data-types/fixedstring.md) の場合、すべてのバイトが8つのバイナリ数値として単純にエンコードされます。ゼロバイトは省略されません。

型 [Float](../data-types/float.md) および [Decimal](../data-types/decimal.md) の値は、メモリ内での表現としてエンコードされます。リトルエンディアンアーキテクチャをサポートしているため、リトルエンディアンでエンコードされます。ゼロのリーディング/トレーリングバイトは省略されません。

型 [UUID](../data-types/uuid.md) の値は、ビッグエンディアン順序の文字列としてエンコードされます。

**引数**

- `arg` — バイナリに変換する値。[String](../data-types/string.md), [FixedString](../data-types/fixedstring.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md), [Decimal](../data-types/decimal.md), [Date](../data-types/date.md) もしくは [DateTime](../data-types/datetime.md)。

**返される値**

- 引数のバイナリ表現を持つ文字列。[String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT bin(14);
```

結果:

``` text
┌─bin(14)──┐
│ 00001110 │
└──────────┘
```

クエリ:

``` sql
SELECT bin(toFloat32(number)) AS bin_presentation FROM numbers(15, 2);
```

結果:

``` text
┌─bin_presentation─────────────────┐
│ 00000000000000000111000001000001 │
│ 00000000000000001000000001000001 │
└──────────────────────────────────┘
```

クエリ:

``` sql
SELECT bin(toFloat64(number)) AS bin_presentation FROM numbers(15, 2);
```

結果:

``` text
┌─bin_presentation─────────────────────────────────────────────────┐
│ 0000000000000000000000000000000000000000000000000010111001000000 │
│ 0000000000000000000000000000000000000000000000000011000001000000 │
└──────────────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT bin(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) as bin_uuid
```

結果:

``` text
┌─bin_uuid─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 01100001111100001100010000000100010111001011001100010001111001111001000001111011101001100000000001101010110100111101101110100000 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## unbin

引数内の各ペアのバイナリ数字を数値として解釈し、その数値を表すバイトに変換します。関数は [bin](#bin) の逆の操作を行います。

**構文**

``` sql
unbin(arg)
```

別名: `UNBIN`.

数値引数について `unbin()` は `bin()` の逆を返しません。結果を数値に変換したい場合は、[reverse](../../sql-reference/functions/string-functions.md#reverse) および [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#reinterpretasuint8163264) 関数を使用できます。

:::note
`unbin`が`clickhouse-client`内から呼び出された場合、バイナリ文字列はUTF-8を使用して表示されます。
:::

バイナリ数字`0`と`1`をサポートします。バイナリ数字の数が8の倍数である必要はありません。引数文字列にバイナリ数字以外のものが含まれている場合、実装定義の結果が返されます（例外はスローされません）。

**引数**

- `arg` — バイナリ数字を含む文字列。[String](../data-types/string.md)。

**返される値**

- バイナリ文字列（BLOB）。[String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT UNBIN('001100000011000100110010'), UNBIN('0100110101111001010100110101000101001100');
```

結果:

``` text
┌─unbin('001100000011000100110010')─┬─unbin('0100110101111001010100110101000101001100')─┐
│ 012                               │ MySQL                                             │
└───────────────────────────────────┴───────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT reinterpretAsUInt64(reverse(unbin('1110'))) AS num;
```

結果:

``` text
┌─num─┐
│  14 │
└─────┘
```

## bitmaskToList(num)

整数を受け取り、元の数値を合計したときに構成する2の累乗の一覧を含む文字列を返します。コンマで区切られてスペースがなく、テキスト形式では昇順です。

## bitmaskToArray(num)

整数を受け取り、元の数値を合計したときに構成する2の累乗の一覧を含む `UInt64` 数値の配列を返します。配列内の数値は昇順です。

## bitPositionsToArray(num)

整数を受け取り、それを符号なし整数に変換します。`arg`のビットの位置で`1`と等しいものの一覧を含む`UInt64` 数値の配列を昇順で返します。

**構文**

```sql
bitPositionsToArray(arg)
```

**引数**

- `arg` — 整数値。[Int/UInt](../data-types/int-uint.md)。

**返される値**

- `1`と等しいビットの位置の一覧を含む配列を、昇順で。[Array](../data-types/array.md)([UInt64](../data-types/int-uint.md))。

**例**

クエリ:

``` sql
SELECT bitPositionsToArray(toInt8(1)) AS bit_positions;
```

結果:

``` text
┌─bit_positions─┐
│ [0]           │
└───────────────┘
```

クエリ:

``` sql
SELECT bitPositionsToArray(toInt8(-1)) AS bit_positions;
```

結果:

``` text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```

## mortonEncode

符号なし整数のリストに対してモートンエンコーディング（ZCurve）を計算します。

この関数には2つの操作モードがあります:
- シンプル
- 拡張

### シンプルモード

引数として最大8つの符号なし整数を受け取り、`UInt64` コードを生成します。

**構文**

```sql
mortonEncode(args)
```

**パラメータ**

- `args`: 最大8つの[符号なし整数](../data-types/int-uint.md)またはそれに関連する型のカラム。

**返される値**

- `UInt64` コード。[UInt64](../data-types/int-uint.md)

**例**

クエリ:

```sql
SELECT mortonEncode(1, 2, 3);
```

結果:

```response
53
```

### 拡張モード

最初の引数としてレンジマスク（[タプル](../data-types/tuple.md)）を受け取り、他の引数として最大8つの[符号なし整数](../data-types/int-uint.md)を受け取ります。

マスク内の各数値は、範囲展開の量を設定します:<br/>
1 - 拡張なし<br/>
2 - 2倍拡張<br/>
3 - 3倍拡張<br/>
...<br/>
最大8倍拡張。<br/>

**構文**

```sql
mortonEncode(range_mask, args)
```

**パラメータ**
- `range_mask`: 1-8。
- `args`: 最大8つの[符号なし整数](../data-types/int-uint.md)またはそれに関連する型のカラム。

注: `args`にカラムを使用する場合、提供された`range_mask`タプルは依然として定数である必要があります。

**返される値**

- `UInt64` コード。[UInt64](../data-types/int-uint.md)

**例**

例えば、異なる範囲やカーディナリティを持つ引数に対して、類似の分布が必要な場合に範囲拡張が有用です。 例えば、'IPアドレス' (0...FFFFFFFF) と '国コード' (0...FF) の場合。

クエリ:

```sql
SELECT mortonEncode((1,2), 1024, 16);
```

結果:

```response
1572864
```

注: タプルのサイズは他の引数の数と等しくなければなりません。

**例**

1つの引数に対するモートンエンコードは常に自身となります：

クエリ:

```sql
SELECT mortonEncode(1);
```

結果:

```response
1
```

**例**

一つの引数を拡張することもできます：

クエリ:

```sql
SELECT mortonEncode(tuple(2), 128);
```

結果:

```response
32768
```

**例**

関数内でカラム名を使用することもできます。

クエリ:

まずテーブルを作成し、いくつかのデータを挿入します。

```sql
create table morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
Engine=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into morton_numbers (*) values(1,2,3,4,5,6,7,8);
```
カラム名を使用して`mortonEncode`の関数引数として定数の代わりに使用

クエリ:

```sql
SELECT mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8) FROM morton_numbers;
```

結果:

```response
2155374165
```

**実装の詳細**

`UInt64`のモートンコードには情報のビット数が制限されます。2つの引数は各引数の最大範囲が2^32（64/2）です。3つの引数には最大範囲が2^21（64/3）です。すべてのオーバーフローはゼロにクリップされます。

## mortonDecode

モートンエンコーディング（ZCurve）を符号なし整数の組にデコードし、対応する座標を返します。

この関数にも `mortonEncode` 関数と同様に2つの操作モードがあります:
- シンプル
- 拡張

### シンプルモード

最初の引数として結果のタプルサイズを受け取り、第2引数としてコードを受け取ります。

**構文**

```sql
mortonDecode(tuple_size, code)
```

**パラメータ**
- `tuple_size`: 8以下の整数値。
- `code`: [UInt64](../data-types/int-uint.md) コード。

**返される値**

- 指定されたサイズの[タプル](../data-types/tuple.md)。[UInt64](../data-types/int-uint.md)

**例**

クエリ:

```sql
SELECT mortonDecode(3, 53);
```

結果:

```response
["1","2","3"]
```

### 拡張モード

最初の引数としてレンジマスク（タプル）を受け取り、第2引数としてコードを受け取ります。
マスク内の各数値は、対応する引数を範囲内で実質的にスケーリングするために左シフトされるビット数を設定します。

似たような範囲やカーディナリティを持つ引数に対しては、範囲拡張が有益です。
たとえば、'IPアドレス' (0...FFFFFFFF) および '国コード' (0...FF)。
エンコード関数と同様に、最大8つの数値に制限されています。

**例**

1つの引数に対するモートンコードは常に自身の引数となります（タプル）。

クエリ:

```sql
SELECT mortonDecode(1, 1);
```

結果:

```response
["1"]
```

**例**

1つの引数がビットシフトを指定するタプルと一緒に提供された場合、関数はこの引数を指定されたビット数だけ右シフトします。

クエリ:

```sql
SELECT mortonDecode(tuple(2), 32768);
```

結果:

```response
["128"]
```

**例**

関数はセカンド引数としてコードのカラムを受け付けます:

まずテーブルを作成し、いくつかのデータを挿入します。

クエリ:
```sql
create table morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
Engine=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into morton_numbers (*) values(1,2,3,4,5,6,7,8);
```
カラム名を使用して`mortonDecode`の関数引数として定数の代わりに使用

クエリ:

```sql
select untuple(mortonDecode(8, mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8))) from morton_numbers;
```

結果:

```response
1	2	3	4	5	6	7	8
```

## hilbertEncode

ヒルベルト曲線のコードを符号なし整数のリストに対して計算します。

この関数は2つの操作モードを持ちます:
- シンプル
- 拡張

### シンプルモード

引数として最大2つの符号なし整数を受け取り、`UInt64` コードを生成します。

**構文**

```sql
hilbertEncode(args)
```

**パラメータ**

- `args`: 最大2つの[符号なし整数](../../sql-reference/data-types/int-uint.md)またはそれに関連する型のカラム。

**返される値**

- `UInt64` コード

型: [UInt64](../../sql-reference/data-types/int-uint.md)

**例**

クエリ:

```sql
SELECT hilbertEncode(3, 4);
```
結果:

```response
31
```

### 拡張モード

最初の引数としてレンジマスク（[タプル](../../sql-reference/data-types/tuple.md)）を受け取り、他の引数として最大2つの[符号なし整数](../../sql-reference/data-types/int-uint.md)を受け取ります。

マスク内の各数値は、対応する引数を左にシフトするビット数を設定し、実質的にその範囲内で引数をスケーリングします。

**構文**

```sql
hilbertEncode(range_mask, args)
```

**パラメータ**
- `range_mask`: ([タプル](../../sql-reference/data-types/tuple.md))
- `args`: 最大2つの[符号なし整数](../../sql-reference/data-types/int-uint.md)またはそれに関連する型のカラム。

注: `args`にカラムを使用する場合、提供された`range_mask`タプルは依然として定数である必要があります。

**返される値**

- `UInt64` コード

型: [UInt64](../../sql-reference/data-types/int-uint.md)

**例**

例えば、異なる範囲やカーディナリティを持つ引数に対して、類似の分布が必要な場合に範囲拡張が有用です。 例えば、'IPアドレス' (0...FFFFFFFF) と '国コード' (0...FF) の場合。

クエリ:

```sql
SELECT hilbertEncode((10,6), 1024, 16);
```

結果:

```response
4031541586602
```

注: タプルのサイズは他の引数の数と等しくなければなりません。

**例**

単一の引数に対してタプルを指定しない場合、関数はヒルベルトインデックスとして自身の引数を返します。

クエリ:

```sql
SELECT hilbertEncode(1);
```

結果:

```response
1
```

**例**

単一の引数がビットシフトを指定するタプルと共に提供された場合、関数はこの引数を指定されたビット数だけ左シフトします。

クエリ:

```sql
SELECT hilbertEncode(tuple(2), 128);
```

結果:

```response
512
```

**例**

この関数はカラムとしての引数も受け入れます:

クエリ:

まずテーブルを作成し、いくつかのデータを挿入します。

```sql
create table hilbert_numbers(
    n1 UInt32,
    n2 UInt32
)
Engine=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into hilbert_numbers (*) values(1,2);
```
カラム名を使用して`hilbertEncode`の関数引数として定数の代わりに使用

クエリ:

```sql
SELECT hilbertEncode(n1, n2) FROM hilbert_numbers;
```

結果:

```response
13
```

**実装の詳細**

`UInt64` の Hilbert コードには情報のビット数が制限されます。2つの引数は各引数の最大範囲が2^32（64/2）です。すべてのオーバーフローはゼロにクリップされます。

## hilbertDecode

ヒルベルト曲線インデックスを符号なし整数のタプルにデコードし、マルチ次元空間の座標を表します。

この関数にも `hilbertEncode` 関数と同様に2つの操作モードがあります:
- シンプル
- 拡張

### シンプルモード

最大2つの符号なし整数を引数として受け取り、`UInt64` コードを生成します。

**構文**

```sql
hilbertDecode(tuple_size, code)
```

**パラメータ**
- `tuple_size`: 2以下の整数値。
- `code`: [UInt64](../../sql-reference/data-types/int-uint.md) コード。

**返される値**

- 指定されたサイズの[タプル](../../sql-reference/data-types/tuple.md)。

型: [UInt64](../../sql-reference/data-types/int-uint.md)

**例**

クエリ:

```sql
SELECT hilbertDecode(2, 31);
```

結果:

```response
["3", "4"]
```

### 拡張モード

最初の引数としてレンジマスク（タプル）を受け取り、最大2つの符号なし整数の他の引数を受け取ります。
マスク内の各数値は、対応する引数を左にシフトするビット数を設定し、実質的にその範囲内で引数をスケーリングします。

似たような範囲やカーディナリティを持つ引数に対しては、範囲拡張が有益です。
たとえば、'IPアドレス' (0...FFFFFFFF) および '国コード' (0...FF)。
エンコード関数と同様に、最大8つの数値に制限されています。

**例**

1つの引数に対するヒルベルトコードは常に自身の引数（タプル）となります。

クエリ:

```sql
SELECT hilbertDecode(1, 1);
```

結果:

```response
["1"]
```

**例**

1つの引数がビットシフトを指定するタプルと共に提供された場合、この引数は指定されたビット数だけ右シフトされます。

クエリ:

```sql
SELECT hilbertDecode(tuple(2), 32768);
```

結果:

```response
["128"]
```

**例**

関数は第2引数としてのカラムのコードも受け入れます:

まずテーブルを作成し、いくつかのデータを挿入します。

クエリ:
```sql
create table hilbert_numbers(
    n1 UInt32,
    n2 UInt32
)
Engine=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into hilbert_numbers (*) values(1,2);
```
カラム名を使用して`hilbertDecode`の関数引数として定数の代わりに使用

クエリ:

```sql
select untuple(hilbertDecode(2, hilbertEncode(n1, n2))) from hilbert_numbers;
```

結果:

```response
1	2
```

