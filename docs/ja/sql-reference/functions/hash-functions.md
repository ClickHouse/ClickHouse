---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: "\u30CF\u30C3\u30B7\u30E5"
---

# ハッシュ関数 {#hash-functions}

ハッシュ関数は、要素の決定論的pseudo似乱数シャッフルに使用することができます。

## halfMD5 {#hash-functions-halfmd5}

[解釈する](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) すべての入力パラメータを文字列として計算し、 [MD5](https://en.wikipedia.org/wiki/MD5) それぞれのハッシュ値。 次に、ハッシュを結合し、結果の文字列のハッシュの最初の8バイトを取り、それらを次のように解釈します `UInt64` ビッグエンディアンのバイト順。

``` sql
halfMD5(par1, ...)
```

この機能は比較的遅い（プロセッサコアあたり毎秒5万個の短い文字列）。
の使用を検討します。 [sipHash64](#hash_functions-siphash64) 代わりに関数。

**パラメータ**

この関数は、可変数の入力パラメータを受け取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。

**例**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

文字列からMD5を計算し、結果のバイトセットをFixedString(16)として返します。
特にMD5を必要としないが、まともな暗号化128ビットハッシュが必要な場合は、 ‘sipHash128’ 代わりに関数。
Md5sumユーティリティによる出力と同じ結果を得たい場合は、lower(hex(MD5(s)))を使用します。

## sipHash64 {#hash_functions-siphash64}

64ビットを生成する [サイファッシュ](https://131002.net/siphash/) ハッシュ値。

``` sql
sipHash64(par1,...)
```

これは暗号化ハッシュ関数です。 それはより少なくとも三倍速く働きます [MD5](#hash_functions-md5) 機能。

関数 [解釈する](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) すべての入力パラメータを文字列として計算し、それぞれのハッシュ値を計算します。 次のアルゴリズムでハッシュを結合します:

1.  すべての入力パラメータをハッシュした後、関数はハッシュの配列を取得します。
2.  関数は、最初と第二の要素を取り、それらの配列のハッシュを計算します。
3.  次に、関数は、前のステップで計算されたハッシュ値、および最初のハッシュ配列の第三の要素を取り、それらの配列のハッシュを計算します。
4.  前のステップは、初期ハッシュ配列の残りのすべての要素に対して繰り返されます。

**パラメータ**

この関数は、可変数の入力パラメータを受け取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。

**例**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128 {#hash_functions-siphash128}

文字列からSipHashを計算します。
文字列型の引数を受け取ります。 FixedString(16)を返します。
SipHash64とは異なり、最終的なxor折り畳み状態は128ビットまでしか行われない。

## シティハッシュ64 {#cityhash64}

64ビットを生成する [シティハッシュ](https://github.com/google/cityhash) ハッシュ値。

``` sql
cityHash64(par1,...)
```

これは高速な非暗号ハッシュ関数です。 文字列パラメータにはCityHashアルゴリズムを使用し、他のデータ型のパラメータには実装固有の高速非暗号化ハッシュ関数を使用します。 この関数は、最終的な結果を得るためにCityHash combinatorを使用します。

**パラメータ**

この関数は、可変数の入力パラメータを受け取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。

**例**

呼び出し例:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

次の例では、行の順序までの精度でテーブル全体のチェックサムを計算する方法を示します:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

任意のタイプの整数から32ビットハッシュコードを計算します。
これは、数値の平均品質の比較的高速な非暗号ハッシュ関数です。

## intHash64 {#inthash64}

任意のタイプの整数から64ビットハッシュコードを計算します。
それはintHash32よりも速く動作します。 平均品質。

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

文字列からSHA-1、SHA-224、またはSHA-256を計算し、結果のバイトセットをFixedString(20)、FixedString(28)、またはFixedString(32)として返します。
この機能はかなりゆっくりと動作します（SHA-1はプロセッサコア毎秒約5万の短い文字列を処理しますが、SHA-224とSHA-256は約2.2万の短い文字列を処理
この関数は、特定のハッシュ関数が必要で選択できない場合にのみ使用することをお勧めします。
このような場合でも、SELECTに適用するのではなく、関数をオフラインで適用し、テーブルに挿入するときに値を事前に計算することをお勧めします。

## URLHash(url\[,N\]) {#urlhashurl-n}

何らかのタイプの正規化を使用してURLから取得された文字列に対する、高速でまともな品質の非暗号化ハッシュ関数。
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` または `#` 最後に、存在する場合。
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` または `#` 最後に、存在する場合。
レベルはURLHierarchyと同じです。 この機能はYandexに固有のものです。メトリカ

## farnhash64 {#farmhash64}

64ビットを生成する [ファームハッシュ](https://github.com/google/farmhash) ハッシュ値。

``` sql
farmHash64(par1, ...)
```

この関数は `Hash64` すべてからの方法 [利用可能な方法](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**パラメータ**

この関数は、可変数の入力パラメータを受け取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。

**例**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

計算 [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) 文字列から。 このハッシュ関数は高速でも良質でもありません。 これを使用する唯一の理由は、このアルゴリズムが既に別のシステムで使用されており、まったく同じ結果を計算する必要がある場合です。

**構文**

``` sql
SELECT javaHash('');
```

**戻り値**

A `Int32` データ型のハッシュ値。

**例**

クエリ:

``` sql
SELECT javaHash('Hello, world!');
```

結果:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE {#javahashutf16le}

計算 [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) 文字列から、UTF-16LEエンコーディングで文字列を表すバイトが含まれていると仮定します。

**構文**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**パラメータ**

-   `stringUtf16le` — a string in UTF-16LE encoding.

**戻り値**

A `Int32` データ型のハッシュ値。

**例**

UTF-16LEエンコード文字列でクエリを修正します。

クエリ:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))
```

結果:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash {#hash-functions-hivehash}

計算 `HiveHash` 文字列から。

``` sql
SELECT hiveHash('');
```

これはちょうどです [JavaHash](#hash_functions-javahash) ゼロアウト符号ビットを持つ。 この関数は [Apacheハイブ](https://en.wikipedia.org/wiki/Apache_Hive) 3.0より前のバージョンの場合。 このハッシュ関数は高速でも良質でもありません。 これを使用する唯一の理由は、このアルゴリズムが既に別のシステムで使用されており、まったく同じ結果を計算する必要がある場合です。

**戻り値**

A `Int32` データ型のハッシュ値。

タイプ: `hiveHash`.

**例**

クエリ:

``` sql
SELECT hiveHash('Hello, world!');
```

結果:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64 {#metrohash64}

64ビットを生成する [メトロハシュ](http://www.jandrewrogers.com/2015/05/27/metrohash/) ハッシュ値。

``` sql
metroHash64(par1, ...)
```

**パラメータ**

この関数は、可変数の入力パラメータを受け取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。

**例**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash {#jumpconsistenthash}

JumpConsistentHashを計算すると、UInt64を形成します。
UInt64型のキーとバケットの数です。 Int32を返します。
詳細は、リンクを参照してください: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2\_32,murmurHash2\_64 {#murmurhash2-32-murmurhash2-64}

を生成する。 [つぶやき2](https://github.com/aappleby/smhasher) ハッシュ値。

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**パラメータ**

両方の関数は、可変数の入力パラメータを取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

-   その `murmurHash2_32` 関数はハッシュ値を返します。 [UInt32](../../sql-reference/data-types/int-uint.md) データ型。
-   その `murmurHash2_64` 関数はハッシュ値を返します。 [UInt64](../../sql-reference/data-types/int-uint.md) データ型。

**例**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash {#gccmurmurhash}

64ビットの計算 [つぶやき2](https://github.com/aappleby/smhasher) 同じハッシュシードを使用するハッシュ値 [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191). これは、CLangとGCCビルドの間で移植可能です。

**構文**

``` sql
gccMurmurHash(par1, ...);
```

**パラメータ**

-   `par1, ...` — A variable number of parameters that can be any of the [対応するデータ型](../../sql-reference/data-types/index.md#data_types).

**戻り値**

-   計算されたハッシュ値。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

結果:

``` text
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## murmurHash3\_32,murmurHash3\_64 {#murmurhash3-32-murmurhash3-64}

を生成する。 [マムルハシュ3世](https://github.com/aappleby/smhasher) ハッシュ値。

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**パラメータ**

両方の関数は、可変数の入力パラメータを取ります。 パラメータは、以下のいずれかです [対応するデータ型](../../sql-reference/data-types/index.md).

**戻り値**

-   その `murmurHash3_32` 関数はaを返します [UInt32](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。
-   その `murmurHash3_64` 関数はaを返します [UInt64](../../sql-reference/data-types/int-uint.md) データ型のハッシュ値。

**例**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## つぶやき3\_128 {#murmurhash3-128}

128ビットを生成する [マムルハシュ3世](https://github.com/aappleby/smhasher) ハッシュ値。

``` sql
murmurHash3_128( expr )
```

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) aを返す [文字列](../../sql-reference/data-types/string.md)-タイプ値。

**戻り値**

A [FixedString(16)](../../sql-reference/data-types/fixedstring.md) データ型のハッシュ値。

**例**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32,xxHash64 {#hash-functions-xxhash32}

計算 `xxHash` 文字列から。 これは、二つの味、32と64ビットで提案されています。

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**戻り値**

A `Uint32` または `Uint64` データ型のハッシュ値。

タイプ: `xxHash`.

**例**

クエリ:

``` sql
SELECT xxHash32('Hello, world!');
```

結果:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**も参照。**

-   [xxHash](http://cyan4973.github.io/xxHash/).

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/hash_functions/) <!--hide-->
