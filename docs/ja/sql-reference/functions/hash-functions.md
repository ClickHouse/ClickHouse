---
slug: /ja/sql-reference/functions/hash-functions
sidebar_position: 85
sidebar_label: Hash
---

# ハッシュ関数

ハッシュ関数は、要素の決定的な擬似ランダムシャッフルに使用できます。

Simhashは、類似した引数に対して近いハッシュ値を返すハッシュ関数です。

## halfMD5

すべての入力パラメータを文字列として[解釈](../functions/type-conversion-functions.md/#type_conversion_functions-reinterpretAsString)し、それぞれに対して[MD5](https://en.wikipedia.org/wiki/MD5)ハッシュ値を計算します。その後、ハッシュを結合し、結果の文字列のハッシュの最初の8バイトを取り出して、ビッグエンディアンバイトオーダーで`UInt64`として解釈します。

```sql
halfMD5(par1, ...)
```

この関数は比較的遅いです（1秒あたり500万の短い文字列をプロセッサコアごとに処理します）。
代わりに[sipHash64](#siphash64)関数の使用を検討してください。

**引数**

この関数は可変数の入力パラメータを取ります。引数は[サポートされているデータ型](../data-types/index.md)のいずれかであり得ます。異なる型の引数でも、同じ値に対して計算されるハッシュ関数の値が同じになる場合があります（異なるサイズの整数、同じデータを持つ名前付きおよび名前なしの`Tuple`、対応するデータを持つ`Map`および`Array(Tuple(key, value))`型など）。

**返される値**

[UInt64](../data-types/int-uint.md)データ型のハッシュ値。

**例**

```sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type;
```

```response
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD4

文字列からMD4を計算し、結果のバイトセットをFixedString(16)として返します。

## MD5

文字列からMD5を計算し、結果のバイトセットをFixedString(16)として返します。
特定のMD5が必要でない場合、暗号化的に確かな128ビットハッシュが必要な場合には、代わりに‘sipHash128’関数を使用してください。
md5sumユーティリティと同じ結果を得たい場合は、lower(hex(MD5(s)))を使用してください。

## RIPEMD160

[RIPEMD-160](https://en.wikipedia.org/wiki/RIPEMD)ハッシュ値を生成します。

**構文**

```sql
RIPEMD160(input)
```

**パラメータ**

- `input`: 入力文字列。[String](../data-types/string.md)

**返される値**

- 160ビットの`RIPEMD-160` ハッシュ値、[FixedString(20)](../data-types/fixedstring.md)型。

**例**

結果を16進エンコードされた文字列として表すには、[hex](../functions/encoding-functions.md/#hex)関数を使用します。

クエリ:

```sql
SELECT HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'));
```

```response
┌─HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'))─┐
│ 37F332F68DB77BD9D7EDD4969571AD671CF9DD3B                      │
└───────────────────────────────────────────────────────────────┘
```

## sipHash64

64ビットの[SipHash](https://en.wikipedia.org/wiki/SipHash)ハッシュ値を生成します。

```sql
sipHash64(par1,...)
```

これは暗号化ハッシュ関数です。少なくとも[MD5](#md5)ハッシュ関数の3倍の速さで動作します。

この関数はすべての入力パラメータを文字列として[解釈](../functions/type-conversion-functions.md/#type_conversion_functions-reinterpretAsString)し、各パラメータごとにハッシュ値を計算します。次に、以下のアルゴリズムでハッシュを結合します。

1. 最初と二番目のハッシュ値を連結し、結果をハッシュします。
2. 前回計算されたハッシュ値と第三の入力パラメータのハッシュを同様にハッシュします。
3. この計算を元の入力の残りのすべてのハッシュ値に対して繰り返します。

**引数**

この関数は任意の[サポートされているデータ型](../data-types/index.md)の可変数の入力パラメータを取ります。

**返される値**

[UInt64](../data-types/int-uint.md)データ型のハッシュ値。

同じ入力値でも異なる引数型の場合、計算されるハッシュ値が等しい場合があることに注意してください。例えば、異なるサイズの整数、同じデータを持つ名前あり・なしの`Tuple`、同じデータを持つ`Map`と対応する`Array(Tuple(key, value))`型です。

**例**

```sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;
```

```response
┌──────────────SipHash─┬─type───┐
│ 11400366955626497465 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash64Keyed

[sipHash64](#siphash64)と同様ですが、固定キーを使う代わりに明示的なキー引数を追加で取ります。

**構文**

```sql
sipHash64Keyed((k0, k1), par1,...)
```

**引数**

[sipHash64](#siphash64)と同様ですが、最初の引数はキーを表す二つの`UInt64`値のタプルです。

**返される値**

[UInt64](../data-types/int-uint.md)データ型のハッシュ値。

**例**

クエリ:

```sql
SELECT sipHash64Keyed((506097522914230528, 1084818905618843912), array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;
```

```response
┌─────────────SipHash─┬─type───┐
│ 8017656310194184311 │ UInt64 │
└─────────────────────┴────────┘
```

## sipHash128

[sipHash64](#siphash64)と同様ですが、128ビットのハッシュ値を生成します。最終的なxor折りたたみ状態は128ビットまで実行されます。

:::note
この128ビットのバリアントはリファレンス実装とは異なり、弱いです。
これは、書かれた当時、SipHashの公式な128ビット拡張が存在しなかったためです。
新しいプロジェクトではおそらく[sipHash128Reference](#siphash128reference)を使用すべきです。
:::

**構文**

```sql
sipHash128(par1,...)
```

**引数**

[sipHash64](#siphash64)と同様。

**返される値**

128ビットの`SipHash`ハッシュ値。[FixedString(16)](../data-types/fixedstring.md)型。

**例**

クエリ:

```sql
SELECT hex(sipHash128('foo', '\x01', 3));
```

結果:

```response
┌─hex(sipHash128('foo', '', 3))────┐
│ 9DE516A64A414D4B1B609415E4523F24 │
└──────────────────────────────────┘
```

## sipHash128Keyed

[sipHash128](#siphash128)と同様ですが、固定キーを使用する代わりに明示的なキー引数を追加で取ります。

:::note
この128ビットのバリアントはリファレンス実装とは異なり、弱いです。
これは、書かれた当時、SipHashの公式な128ビット拡張が存在しなかったためです。
新しいプロジェクトではおそらく[sipHash128ReferenceKeyed](#siphash128referencekeyed)を使用すべきです。
:::

**構文**

```sql
sipHash128Keyed((k0, k1), par1,...)
```

**引数**

[sipHash128](#siphash128)と同様ですが、最初の引数はキーを表す二つの`UInt64`値のタプルです。

**返される値**

128ビットの`SipHash`ハッシュ値。[FixedString(16)](../data-types/fixedstring.md)型。

**例**

クエリ:

```sql
SELECT hex(sipHash128Keyed((506097522914230528, 1084818905618843912),'foo', '\x01', 3));
```

結果:

```response
┌─hex(sipHash128Keyed((506097522914230528, 1084818905618843912), 'foo', '', 3))─┐
│ B8467F65C8B4CFD9A5F8BD733917D9BF                                              │
└───────────────────────────────────────────────────────────────────────────────┘
```

## sipHash128Reference

[sipHash128](#siphash128)と同様ですが、SipHashのオリジナル作者による128ビットアルゴリズムを実装しています。

**構文**

```sql
sipHash128Reference(par1,...)
```

**引数**

[sipHash128](#siphash128)と同様。

**返される値**

128ビットの`SipHash`ハッシュ値。[FixedString(16)](../data-types/fixedstring.md)型。

**例**

クエリ:

```sql
SELECT hex(sipHash128Reference('foo', '\x01', 3));
```

結果:

```response
┌─hex(sipHash128Reference('foo', '', 3))─┐
│ 4D1BE1A22D7F5933C0873E1698426260       │
└────────────────────────────────────────┘
```

## sipHash128ReferenceKeyed

[sipHash128Reference](#siphash128reference)と同様ですが、固定キーを使用する代わりに明示的なキー引数を追加で取ります。

**構文**

```sql
sipHash128ReferenceKeyed((k0, k1), par1,...)
```

**引数**

[sipHash128Reference](#siphash128reference)と同様ですが、最初の引数はキーを表す二つの`UInt64`値のタプルです。

**返される値**

128ビットの`SipHash`ハッシュ値。[FixedString(16)](../data-types/fixedstring.md)型。

**例**

クエリ:

```sql
SELECT hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912),'foo', '\x01', 3));
```

結果:

```response
┌─hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912), 'foo', '', 3))─┐
│ 630133C9722DC08646156B8130C4CDC8                                                       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## cityHash64

64ビットの[CityHash](https://github.com/google/cityhash)ハッシュ値を生成します。

```sql
cityHash64(par1,...)
```

これは高速な非暗号化ハッシュ関数です。文字列パラメータにはCityHashアルゴリズムを使用し、他のデータ型のパラメータには実装固有の高速非暗号化ハッシュ関数を使用します。この関数はCityHashのコンビネータを使用して最終結果を得ます。

GoogleがCityHashのアルゴリズムをClickHouseに追加された後に変更したことに注意してください。つまり、ClickHouseのcityHash64とGoogleの上流のCityHashは異なる結果を生成します。ClickHouseのcityHash64はCityHash v1.0.2に対応しています。

**引数**

この関数は可変数の入力パラメータを取ります。引数は[サポートされているデータ型](../data-types/index.md)のいずれかであり得ます。異なる型の引数でも、同じ値に対して計算されるハッシュ関数の値が同じになる場合があります（異なるサイズの整数、同じデータを持つ名前付きおよび名前なしの`Tuple`、同じデータを持つ`Map`および対応する`Array(Tuple(key, value))`型）。

**返される値**

[UInt64](../data-types/int-uint.md)データ型のハッシュ値。

**例**

呼び出し例:

```sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type;
```

```response
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

次の例は、行順序までの精度でテーブル全体のチェックサムを計算する方法を示しています:

```sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32

任意の型の整数から32ビットのハッシュコードを計算します。
これは、数値用の平均的な品質の高速な非暗号化ハッシュ関数です。

**構文**

```sql
intHash32(int)
```

**引数**

- `int` — ハッシュ化する整数。[(U)Int*](../data-types/int-uint.md)。

**返される値**

- 32ビットのハッシュコード。[UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT intHash32(42);
```

結果:

```response
┌─intHash32(42)─┐
│    1228623923 │
└───────────────┘
```

## intHash64

任意の型の整数から64ビットのハッシュコードを計算します。
これは、数値用の平均的な品質の高速な非暗号化ハッシュ関数です。
[intHash32](#inthash32)よりも高速に動作します。

**構文**

```sql
intHash64(int)
```

**引数**

- `int` — ハッシュ化する整数。[(U)Int*](../data-types/int-uint.md)。

**返される値**

- 64ビットのハッシュコード。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT intHash64(42);
```

結果:

```response
┌────────intHash64(42)─┐
│ 11490350930367293593 │
└──────────────────────┘
```

## SHA1, SHA224, SHA256, SHA512, SHA512_256

文字列からSHA-1, SHA-224, SHA-256, SHA-512, SHA-512-256のハッシュを計算し、結果のバイトセットを[FixedString](../data-types/fixedstring.md)として返します。

**構文**

```sql
SHA1('s')
...
SHA512('s')
```

この関数は比較的遅いです（SHA-1は1秒あたり500万の短い文字列をプロセッサコアごとに処理し、SHA-224とSHA-256は約220万を処理します）。
この関数を使用するのは、特定のハッシュ関数が必要であり、選択ができない場合のみをお勧めします。
このような場合でも、テーブルに挿入する際にオフラインで関数を適用し、値を事前に計算することをお勧めします。

**引数**

- `s` — SHAハッシュ計算のための入力文字列。[String](../data-types/string.md)。

**返される値**

- SHAハッシュを16進数非エンコードされたFixedStringとして返します。SHA-1はFixedString(20)として、SHA-224はFixedString(28)、SHA-256はFixedString(32)、SHA-512はFixedString(64)です。[FixedString](../data-types/fixedstring.md)。

**例**

結果を16進エンコードされた文字列として表すには、[hex](../functions/encoding-functions.md/#hex)関数を使用します。

クエリ:

```sql
SELECT hex(SHA1('abc'));
```

結果:

```response
┌─hex(SHA1('abc'))─────────────────────────┐
│ A9993E364706816ABA3E25717850C26C9CD0D89D │
└──────────────────────────────────────────┘
```

## BLAKE3

BLAKE3ハッシュ文字列を計算し、結果のバイトセットを[FixedString](../data-types/fixedstring.md)として返します。

**構文**

```sql
BLAKE3('s')
```

この暗号化ハッシュ関数はBLAKE3 Rustライブラリと統合されています。この関数はかなり速く、SHA-2と比較して約2倍の性能を示し、SHA-256と同じ長さのハッシュを生成します。

**引数**

- s - BLAKE3ハッシュ計算のための入力文字列。[String](../data-types/string.md)。

**返される値**

- BLAKE3ハッシュを型FixedString(32)のバイト配列として返します。[FixedString](../data-types/fixedstring.md)。

**例**

結果を16進エンコードされた文字列として表すには、[hex](../functions/encoding-functions.md/#hex)関数を使用します。

クエリ:
```sql
SELECT hex(BLAKE3('ABC'))
```

結果:
```sql
┌─hex(BLAKE3('ABC'))───────────────────────────────────────────────┐
│ D1717274597CF0289694F75D96D444B992A096F1AFD8E7BBFA6EBB1D360FEDFC │
└──────────────────────────────────────────────────────────────────┘
```

## URLHash(url\[, N\])

文字列をURLから取得し、ある種の正規化を行った後、高速で質の良い非暗号化ハッシュ関数を計算します。
`URLHash(s)` – 終わりに`/`、`?`、`#`のいずれかの追加入力シンボルがある場合、それを除いた状態で文字列からハッシュを計算します。
`URLHash(s, N)` – 終わりに`/`、`?`、`#`のいずれかの追加入力シンボルがある場合、それを除いた状態でURL階層のNレベルまでハッシュを計算します。
レベルはURLHierarchyと同じです。

## farmFingerprint64

## farmHash64

64ビットの[FarmHash](https://github.com/google/farmhash)またはFingerprint値を生成します。`farmFingerprint64`は、安定していてポータブルな値が好ましい場合にお勧めします。

```sql
farmFingerprint64(par1, ...)
farmHash64(par1, ...)
```

これらの関数は、[使用可能なすべてのメソッド](https://github.com/google/farmhash/blob/master/src/farmhash.h)から、それぞれ`Fingerprint64`と`Hash64`メソッドを使用します。

**引数**

この関数は可変数の入力パラメータを取ります。引数は[サポートされているデータ型](../data-types/index.md)のいずれかであり得ます。異なる型の引数でも、同じ値に対して計算されるハッシュ関数の値が同じになる場合があります（異なるサイズの整数、同じデータを持つ名前付きおよび名前なしの`Tuple`、同じデータを持つ`Map`および対応する`Array(Tuple(key, value))`型）。

**返される値**

[UInt64](../data-types/int-uint.md)データ型のハッシュ値。

**例**

```sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type;
```

```response
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash

Javaの[String](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452)からJavaHashを計算します,
[Byte](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Byte.java#l405),
[Short](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Short.java#l410),
[Integer](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Integer.java#l959),
[Long](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Long.java#l1060).
このハッシュ関数は速くもなく、高品質でもありません。このアルゴリズムが他のシステムで既に使用されていて、全く同じ結果を計算する必要がある場合にのみ使う理由があります。

Javaは符号付き整数のハッシュしか計算をサポートしていないことに注意してください。符号なし整数のハッシュを計算したい場合は、適切な符号付きClickHouse型にキャストする必要があります。

**構文**

```sql
SELECT javaHash('')
```

**返される値**

`Int32`データ型のハッシュ値。

**例**

クエリ:

```sql
SELECT javaHash(toInt32(123));
```

結果:

```response
┌─javaHash(toInt32(123))─┐
│               123      │
└────────────────────────┘
```

クエリ:

```sql
SELECT javaHash('Hello, world!');
```

結果:

```response
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE

文字列からJavaHashを計算し、それがUTF-16LEエンコーディングであるバイトを持つと仮定します。

**構文**

```sql
javaHashUTF16LE(stringUtf16le)
```

**引数**

- `stringUtf16le` — UTF-16LEエンコーディングされた文字列。

**返される値**

`Int32`データ型のハッシュ値。

**例**

UTF-16LEエンコードされた文字列を用いた正しいクエリ。

クエリ:

```sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'));
```

結果:

```response
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash

文字列から`HiveHash`を計算します。

```sql
SELECT hiveHash('')
```

これは、符号ビットをゼロ化したただの[JavaHash](#javahash)です。この関数は、[Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive)バージョン3.0以前で使用されます。このハッシュ関数は速くも高品質でもありません。このアルゴリズムが他のシステムですでに使用されていて、完全に同じ結果を計算する必要がある場合にのみ使用する理由があります。

**返される値**

- `hiveHash`ハッシュ値。[Int32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT hiveHash('Hello, world!');
```

結果:

```response
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64

64ビットの[MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/)ハッシュ値を生成します。

```sql
metroHash64(par1, ...)
```

**引数**

この関数は可変数の入力パラメータを取ります。引数は[サポートされているデータ型](../data-types/index.md)のいずれかであり得ます。異なる型の引数でも、同じ値に対して計算されるハッシュ関数の値が同じになる場合があります（異なるサイズの整数、同じデータを持つ名前付きおよび名前なしの`Tuple`、同じデータを持つ`Map`および対応する`Array(Tuple(key, value))`型）。

**返される値**

[UInt64](../data-types/int-uint.md)データ型のハッシュ値。

**例**

```sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type;
```

```response
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash

UInt64からJumpConsistentHashを計算します。
2つの引数を受け取ります: `UInt64`型のキーとバケット数。返されるのは`Int32`です。
詳細は、リンクをご覧ください: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## kostikConsistentHash

Konstantin 'kostik' OblakovによるO(1)時間および空間の一貫性ハッシュアルゴリズム。以前は`yandexConsistentHash`。

**構文**

```sql
kostikConsistentHash(input, n)
```

別名: `yandexConsistentHash`（後方互換性のために残されています）。

**パラメータ**

- `input`: `UInt64`型のキー。[UInt64](../data-types/int-uint.md)。
- `n`: バケット数。[UInt16](../data-types/int-uint.md)。

**返される値**

- [UInt16](../data-types/int-uint.md)データ型のハッシュ値。

**実装の細部**

n <= 32768の場合にのみ効率的です。

**例**

クエリ:

```sql
SELECT kostikConsistentHash(16045690984833335023, 2);
```

```response
┌─kostikConsistentHash(16045690984833335023, 2)─┐
│                                             1 │
└───────────────────────────────────────────────┘
```

## murmurHash2_32, murmurHash2_64

[MurmurHash2](https://github.com/aappleby/smhasher)ハッシュ値を生成します。

```sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**引数**

両方の関数は可変数の入力パラメータを取ります。引数は[サポートされているデータ型](../data-types/index.md)のいずれかであり得ます。異なる型の引数でも、同じ値に対して計算されるハッシュ関数の値が同じになる場合があります（異なるサイズの整数、同じデータを持つ名前付きおよび名前なしの`Tuple`、同じデータを持つ`Map`および対応する`Array(Tuple(key, value))`型）。

**返される値**

- `murmurHash2_32`関数は[UInt32](../data-types/int-uint.md)データ型のハッシュ値を返します。
- `murmurHash2_64`関数は[UInt64](../data-types/int-uint.md)データ型のハッシュ値を返します。

**例**

```sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;
```

```response
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash

[gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191)と同じハッシュシードを使用して64ビットの[MurmurHash2](https://github.com/aappleby/smhasher)ハッシュ値を計算します。ClangとGCCのビルド間で互換性があります。

**構文**

```sql
gccMurmurHash(par1, ...)
```

**引数**

- `par1, ...` — [サポートされているデータ型](../data-types/index.md/#data_types)のいずれかである可変数のパラメータ。

**返される値**

- 計算されたハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

結果:

```response
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## kafkaMurmurHash

[Kafka](https://github.com/apache/kafka/blob/461c5cfe056db0951d9b74f5adc45973670404d7/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L482)と同じハッシュシードを使用して32ビットの[MurmurHash2](https://github.com/aappleby/smhasher)ハッシュ値を計算し、[Default Partitioner](https://github.com/apache/kafka/blob/139f7709bd3f5926901a21e55043388728ccca78/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L328)と互換性があるように最上位ビットを持ちません。

**構文**

```sql
MurmurHash(par1, ...)
```

**引数**

- `par1, ...` — [サポートされているデータ型](../data-types/index.md/#data_types)のいずれかである可変数のパラメータ。

**返される値**

- 計算されたハッシュ値。[UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    kafkaMurmurHash('foobar') AS res1,
    kafkaMurmurHash(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS res2
```

結果:

```response
┌───────res1─┬─────res2─┐
│ 1357151166 │ 85479775 │
└────────────┴──────────┘
```

## murmurHash3_32, murmurHash3_64

[MurmurHash3](https://github.com/aappleby/smhasher)ハッシュ値を生成します。

```sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**引数**

両方の関数は可変数の入力パラメータを取ります。引数は[サポートされているデータ型](../data-types/index.md)のいずれかであり得ます。異なる型の引数でも、同じ値に対して計算されるハッシュ関数の値が同じになる場合があります（異なるサイズの整数、同じデータを持つ名前付きおよび名前なしの`Tuple`、同じデータを持つ`Map`および対応する`Array(Tuple(key, value))`型）。

**返される値**

- `murmurHash3_32`関数は[UInt32](../data-types/int-uint.md)データ型のハッシュ値を返します。
- `murmurHash3_64`関数は[UInt64](../data-types/int-uint.md)データ型のハッシュ値を返します。

**例**

```sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;
```

```response
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3_128

128ビットの[MurmurHash3](https://github.com/aappleby/smhasher)ハッシュ値を生成します。

**構文**

```sql
murmurHash3_128(expr)
```

**引数**

- `expr` — [式](../syntax.md/#syntax-expressions)のリスト。[String](../data-types/string.md)。

**返される値**

128ビットの`MurmurHash3`ハッシュ値。[FixedString(16)](../data-types/fixedstring.md)。

**例**

クエリ:

```sql
SELECT hex(murmurHash3_128('foo', 'foo', 'foo'));
```

結果:

```response
┌─hex(murmurHash3_128('foo', 'foo', 'foo'))─┐
│ F8F7AD9B6CD4CF117A71E277E2EC2931          │
└───────────────────────────────────────────┘
```

## xxh3

64ビットの[xxh3](https://github.com/Cyan4973/xxHash)ハッシュ値を生成します。

**構文**

```sql
xxh3(expr)
```

**引数**

- `expr` — 任意のデータ型の[式](../syntax.md/#syntax-expressions)のリスト。

**返される値**

64ビットの`xxh3`ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT xxh3('Hello', 'world')
```

結果:

```response
┌─xxh3('Hello', 'world')─┐
│    5607458076371731292 │
└────────────────────────┘
```

## xxHash32, xxHash64

文字列から`xxHash`を計算します。32ビットと64ビットの2つのフレーバーがあります。

```sql
SELECT xxHash32('')

OR

SELECT xxHash64('')
```

**返される値**

- ハッシュ値。[UInt32/64](../data-types/int-uint.md)。

:::note
戻り型は`xxHash32`の場合`UInt32`、`xxHash64`の場合`UInt64`になります。
:::

**例**

クエリ:

```sql
SELECT xxHash32('Hello, world!');
```

結果:

```response
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**関連項目**

- [xxHash](http://cyan4973.github.io/xxHash/)。

## ngramSimHash

ASCII文字列を`ngramsize`シンボルのn-gramに分割し、n-gramの`simhash`を返します。大文字小文字を区別します。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
ngramSimHash(string[, ngramsize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT ngramSimHash('ClickHouse') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 1627567969 │
└────────────┘
```

## ngramSimHashCaseInsensitive

ASCII文字列を`ngramsize`シンボルのn-gramに分割し、n-gramの`simhash`を返します。大文字小文字を区別しません。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
ngramSimHashCaseInsensitive(string[, ngramsize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT ngramSimHashCaseInsensitive('ClickHouse') AS Hash;
```

結果:

```response
┌──────Hash─┐
│ 562180645 │
└───────────┘
```

## ngramSimHashUTF8

UTF-8文字列を`ngramsize`シンボルのn-gramに分割し、n-gramの`simhash`を返します。大文字小文字を区別します。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
ngramSimHashUTF8(string[, ngramsize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT ngramSimHashUTF8('ClickHouse') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 1628157797 │
└────────────┘
```

## ngramSimHashCaseInsensitiveUTF8

UTF-8文字列を`ngramsize`シンボルのn-gramに分割し、n-gramの`simhash`を返します。大文字小文字を区別しません。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
ngramSimHashCaseInsensitiveUTF8(string[, ngramsize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT ngramSimHashCaseInsensitiveUTF8('ClickHouse') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 1636742693 │
└────────────┘
```

## wordShingleSimHash

ASCII文字列を`shinglesize`単語の部分（シングル）に分割して、単語シングルの`simhash`を返します。大文字小文字を区別します。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
wordShingleSimHash(string[, shinglesize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT wordShingleSimHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitive

ASCII文字列を`shinglesize`単語の部分（シングル）に分割して、単語シングルの`simhash`を返します。大文字小文字を区別しません。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
wordShingleSimHashCaseInsensitive(string[, shinglesize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT wordShingleSimHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## wordShingleSimHashUTF8

UTF-8文字列を`shinglesize`単語の部分（シングル）に分割して、単語シングルの`simhash`を返します。大文字小文字を区別します。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
wordShingleSimHashUTF8(string[, shinglesize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT wordShingleSimHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitiveUTF8

UTF-8文字列を`shinglesize`単語の部分（シングル）に分割して、単語シングルの`simhash`を返します。大文字小文字を区別しません。

[bitHammingDistance](../functions/bit-functions.md/#bithammingdistance)を使用して、半重複文字列を検出するために使用できます。計算された2つの文字列の`simhashes`の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)が小さいほど、それらの文字列は同じ可能性が高くなります。

**構文**

```sql
wordShingleSimHashCaseInsensitiveUTF8(string[, shinglesize])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT wordShingleSimHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

結果:

```response
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## wyHash64

64ビットの[wyHash64](https://github.com/wangyi-fudan/wyhash)ハッシュ値を生成します。

**構文**

```sql
wyHash64(string)
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT wyHash64('ClickHouse') AS Hash;
```

結果:

```response
┌─────────────────Hash─┐
│ 12336419557878201794 │
└──────────────────────┘
```

## ngramMinHash

ASCII文字列を`ngramsize`シンボルのn-gramに分割し、各n-gramのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別します。

[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列を検出するために使用できます。2つの文字列の場合: 返されるハッシュのいずれかが両方の文字列で同じである場合、それらの文字列は同じであると考えます。

**構文**

```sql
ngramMinHash(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュを持つタプル — 最小と最大。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT ngramMinHash('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,9054248444481805918) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitive

ASCII文字列を`ngramsize`シンボルのn-gramに分割し、各n-gramのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別しません。

[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列を検出するために使用できます。2つの文字列の場合: 返されるハッシュのいずれかが両方の文字列で同じである場合、それらの文字列は同じであると考えます。

**構文**

```sql
ngramMinHashCaseInsensitive(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュを持つタプル — 最小と最大。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT ngramMinHashCaseInsensitive('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────┐
│ (2106263556442004574,13203602793651726206) │
└────────────────────────────────────────────┘
```

## ngramMinHashUTF8

UTF-8文字列を`ngramsize`シンボルのn-gramに分割し、各n-gramのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別します。

[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列を検出するために使用できます。2つの文字列の場合: 返されるハッシュのいずれかが両方の文字列で同じである場合、それらの文字列は同じであると考えます。

**構文**

```sql
ngramMinHashUTF8(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュを持つタプル — 最小と最大。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT ngramMinHashUTF8('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,6742163577938632877) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitiveUTF8

UTF-8文字列を`ngramsize`シンボルのn-gramに分割し、各n-gramのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別しません。

[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列を検出するために使用できます。2つの文字列の場合: 返されるハッシュのいずれかが両方の文字列で同じである場合、それらの文字列は同じであると考えます。

**構文**

```sql
ngramMinHashCaseInsensitiveUTF8(string [, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュを持つタプル — 最小と最大。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT ngramMinHashCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple───────────────────────────────────────┐
│ (12493625717655877135,13203602793651726206) │
└─────────────────────────────────────────────┘
```

## ngramMinHashArg

ASCII文字列を`ngramsize`シンボルのn-gramに分割し、最小および最大ハッシュを計算する[ngramMinHash](#ngramminhash)関数で同じ入力を用いて計算されたn-gramsを返します。大文字小文字を区別します。

**構文**

```sql
ngramMinHashArg(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 各`hashnum`個のn-gramsを持つ2つのタプルを含むタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT ngramMinHashArg('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('Hou','lic','ick','ous','ckH','Cli')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitive

ASCII文字列を`ngramsize`シンボルのn-gramに分割し、最小および最大ハッシュを計算する[ngramMinHashCaseInsensitive](#ngramminhashcaseinsensitive)関数で同じ入力を用いて計算されたn-gramsを返します。大文字小文字を区別しません。

**構文**

```sql
ngramMinHashArgCaseInsensitive(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 各`hashnum`個のn-gramsを持つ2つのタプルを含むタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT ngramMinHashArgCaseInsensitive('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','kHo','use','Cli'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgUTF8

UTF-8文字列を`ngramsize`シンボルのn-gramに分割し、最小および最大ハッシュを計算する[ngramMinHashUTF8](#ngramminhashutf8)関数で同じ入力を用いて計算されたn-gramsを返します。大文字小文字を区別します。

**構文**

```sql
ngramMinHashArgUTF8(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 各`hashnum`個のn-gramsを持つ2つのタプルを含むタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT ngramMinHashArgUTF8('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('kHo','Hou','lic','ick','ous','ckH')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitiveUTF8

UTF-8文字列を`ngramsize`シンボルのn-gramに分割し、最小および最大ハッシュを計算する[ngramMinHashCaseInsensitiveUTF8](#ngramminhashcaseinsensitiveutf8)関数で同じ入力を用いて計算されたn-gramsを返します。大文字小文字を区別しません。

**構文**

```sql
ngramMinHashArgCaseInsensitiveUTF8(string[, ngramsize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `ngramsize` — n-gramのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 各`hashnum`個のn-gramsを持つ2つのタプルを含むタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT ngramMinHashArgCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ckH','ous','ick','lic','kHo','use'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHash

ASCII文字列を`shinglesize`単語の部分（シングル）に分割して、各単語シングルのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別します。

[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列を検出するために使用できます。2つの文字列の場合: 返されるハッシュのいずれかが両方の文字列で同じである場合、それらの文字列は同じであると考えます。

**構文**

```sql
wordShingleMinHash(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュを持つタプル — 最小と最大。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT wordShingleMinHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitive

ASCII文字列を`shinglesize`単語の部分（シングル）に分割して、各単語シングルのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別しません。

[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列を検出するために使用できます。2つの文字列の場合: 返されるハッシュのいずれかが両方の文字列で同じである場合、それらの文字列は同じであると考えます。

**構文**

```sql
wordShingleMinHashCaseInsensitive(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果の計算に使用される最小および最大ハッシュの数。オプション。可能な値は`1`から`25`までの任意の数。デフォルト値: `6`。[UInt8](../data-types/int-uint.md)।

**返される値**

- 2つのハッシュを持つタプル — 最小と最大。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashUTF8

UTF-8文字列を`shinglesize`単語の部分（シングル）に分割して、各単語シングルのハッシュ値を計算します。`hashnum`個の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`個の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別します。
は[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列の検出に使用できます。2つの文字列に対して、返されたハッシュの1つが両方の文字列で同じであれば、それらの文字列は同じであると考えます。

**構文**

```sql
wordShingleMinHashUTF8(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。 [String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`3`。 [UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果を計算するために使用される最小および最大ハッシュの数。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`6`。 [UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュ（最小および最大）のタプル。 [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitiveUTF8

UTF-8文字列を`shinglesize`単語の部分（シングル）に分割し、各単語シングルに対するハッシュ値を計算します。`hashnum`の最小ハッシュを使用して最小ハッシュを計算し、`hashnum`の最大ハッシュを使用して最大ハッシュを計算します。これらのハッシュを含むタプルを返します。大文字小文字を区別しません。

は[tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance)を使用して、半重複文字列の検出に使用できます。2つの文字列に対して、返されたハッシュの1つが両方の文字列で同じであれば、それらの文字列は同じであると考えます。

**構文**

```sql
wordShingleMinHashCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果を計算するために使用される最小および最大ハッシュの数。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- 2つのハッシュ（最小および最大）のタプル。[Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashArg

ASCII文字列を`shinglesize`単語の部分（シングル）に分割し、同じ入力で[wordshingleMinHash](#wordshingleminhash)関数によって計算された最小および最大ワードハッシュを持つシングルを返します。大文字小文字を区別します。

**構文**

```sql
wordShingleMinHashArg(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果を計算するために使用される最小および最大ハッシュの数。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- `hashnum`ワードシングルを含む2つのタプル。 [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashArg('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitive

ASCII文字列を`shinglesize`単語の部分（シングル）に分割し、同じ入力で[wordShingleMinHashCaseInsensitive](#wordshingleminhashcaseinsensitive)関数によって計算された最小および最大ワードハッシュを持つシングルを返します。大文字小文字を区別しません。

**構文**

```sql
wordShingleMinHashArgCaseInsensitive(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果を計算するために使用される最小および最大ハッシュの数。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- `hashnum`ワードシングルを含む2つのタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashArgCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgUTF8

UTF-8文字列を`shinglesize`単語の部分（シングル）に分割し、同じ入力で[wordShingleMinHashUTF8](#wordshingleminhashutf8)関数によって計算された最小および最大ワードハッシュを持つシングルを返します。大文字小文字を区別します。

**構文**

```sql
wordShingleMinHashArgUTF8(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果を計算するために使用される最小および最大ハッシュの数。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- `hashnum`ワードシングルを含む2つのタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashArgUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

結果:

```response
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitiveUTF8

UTF-8文字列を`shinglesize`単語の部分（シングル）に分割し、同じ入力で[wordShingleMinHashCaseInsensitiveUTF8](#wordshingleminhashcaseinsensitiveutf8)関数によって計算された最小および最大ワードハッシュを持つシングルを返します。大文字小文字を区別しません。

**構文**

```sql
wordShingleMinHashArgCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**引数**

- `string` — 文字列。[String](../data-types/string.md)。
- `shinglesize` — 単語シングルのサイズ。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`3`。[UInt8](../data-types/int-uint.md)。
- `hashnum` — 結果を計算するために使用される最小および最大ハッシュの数。省略可能。可能な値：`1`から`25`までの任意の数。デフォルト値：`6`。[UInt8](../data-types/int-uint.md)。

**返される値**

- `hashnum`ワードシングルを含む2つのタプル。[Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md)))。

**例**

クエリ:

```sql
SELECT wordShingleMinHashArgCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

結果:

```response
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```

## sqidEncode

番号を[Sqid](https://sqids.org/)としてエンコードします。これはYouTubeのようなID文字列です。
出力アルファベットは `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789` です。
ハッシュのためにこの関数を使用しないでください - 生成されたIDは元の番号にデコード可能です。

**構文**

```sql
sqidEncode(number1, ...)
```

エイリアス: `sqid`

**引数**

- 可変個数のUInt8, UInt16, UInt32またはUInt64の番号。

**返される値**

sqid [String](../data-types/string.md)。

**例**

```sql
SELECT sqidEncode(1, 2, 3, 4, 5);
```

```response
┌─sqidEncode(1, 2, 3, 4, 5)─┐
│ gXHfJ1C6dN                │
└───────────────────────────┘
```

## sqidDecode

[Sqid](https://sqids.org/)を元の番号にデコードします。入力文字列が有効なsqidでない場合は、空の配列を返します。

**構文**

```sql
sqidDecode(sqid)
```

**引数**

- sqid - [String](../data-types/string.md)

**返される値**

数値に変換されたsqid [Array(UInt64)](../data-types/array.md)。

**例**

```sql
SELECT sqidDecode('gXHfJ1C6dN');
```

```response
┌─sqidDecode('gXHfJ1C6dN')─┐
│ [1,2,3,4,5]              │
└──────────────────────────┘
```
