---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 50
toc_title: "\u30CF\u30C3\u30B7\u30E5"
---

# ハッシュ関数 {#hash-functions}

ハッシュ関数は、要素の決定論的な擬似ランダムシャッフルに使用できます。

## ハーフmd5 {#hash-functions-halfmd5}

[解釈する](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) すべての入力パラメーターを文字列として計算します [MD5](https://en.wikipedia.org/wiki/MD5) それぞれのハッシュ値。 次に、ハッシュを結合し、結果の文字列のハッシュの最初の8バイトを取り、それらを次のように解釈します `UInt64` ビッグエンディアンのバイト順。

``` sql
halfMD5(par1, ...)
```

この関数は比較的遅い（プロセッサコアあたり5万個の短い文字列）。
を使用することを検討 [サイファッシュ64](#hash_functions-siphash64) 代わりに機能。

**パラメータ**

この関数は、可変個の入力パラメータを受け取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。

**例えば**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

文字列からmd5を計算し、結果のバイトセットをfixedstring(16)として返します。
特にmd5を必要としないが、適切な暗号化128ビットハッシュが必要な場合は、 ‘sipHash128’ 代わりに機能。
Md5sumユーティリティによる出力と同じ結果を得たい場合は、lower(hex(MD5(s)))を使用します。

## サイファッシュ64 {#hash_functions-siphash64}

64ビットを生成する [サイファッシュ](https://131002.net/siphash/) ハッシュ値。

``` sql
sipHash64(par1,...)
```

これは暗号化ハッシュ関数です。 それはより速い少なくとも三回働きます [MD5](#hash_functions-md5) 機能。

機能 [解釈する](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) すべての入力パラメータを文字列として計算し、それぞれのハッシュ値を計算します。 その融合ハッシュにより、次のアルゴリズム:

1.  すべての入力パラメータをハッシュした後、関数はハッシュの配列を取得します。
2.  関数は、第一及び第二の要素を取り、それらの配列のためのハッシュを計算します。
3.  次に、関数は、前のステップで計算されたハッシュ値と最初のハッシュ配列の第三の要素を取り、それらの配列のハッシュを計算します。
4.  最初のハッシュ配列の残りのすべての要素について、前の手順が繰り返されます。

**パラメータ**

この関数は、可変個の入力パラメータを受け取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。

**例えば**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## サイファシー128 {#hash_functions-siphash128}

文字列からサイファッシュを計算します。
文字列型引数を受け取ります。 fixedstring(16)を返します。
Siphash64とは異なり、最終的なxor折りたたみ状態は128ビットまでしか行われません。

## cityHash64 {#cityhash64}

64ビットを生成する [CityHash](https://github.com/google/cityhash) ハッシュ値。

``` sql
cityHash64(par1,...)
```

これは高速な非暗号化ハッシュ関数です。 文字列パラメーターにはcityhashアルゴリズムを使用し、他のデータ型のパラメーターには実装固有の高速非暗号化ハッシュ関数を使用します。 この関数は、最終的な結果を得るためにcityhashコンビネータを使用します。

**パラメータ**

この関数は、可変個の入力パラメータを受け取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。

**例**

呼び出しの例:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

次の例は、行順序までの精度でテーブル全体のチェックサムを計算する方法を示しています:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

任意のタイプの整数から32ビットのハッシュコードを計算します。
これは、数値の平均品質の比較的高速な非暗号化ハッシュ関数です。

## intHash64 {#inthash64}

任意のタイプの整数から64ビットのハッシュコードを計算します。
それはinthash32より速く働きます。 平均品質。

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

文字列からsha-1、sha-224、またはsha-256を計算し、結果のバイトセットをfixedstring(20)、fixedstring(28)、またはfixedstring(32)として返します。
この関数はかなりゆっくりと動作します（sha-1はプロセッサコアあたり約5百万個の短い文字列を処理し、sha-224とsha-256は約2.2百万プロセス）。
特定のハッシュ関数が必要で選択できない場合にのみ、この関数を使用することをお勧めします。
このような場合でも、テーブルに値を挿入するときは、selectsに値を適用するのではなく、関数をオフラインで適用して事前計算することをお勧めします。

## URLHash(url\[,N\]) {#urlhashurl-n}

いくつかのタイプの正規化を使用してurlから取得した文字列の高速でまともな品質の非暗号化ハッシュ関数。
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` または `#` 最後に、存在する場合。
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` または `#` 最後に、存在する場合。
レベルはurlhierarchyと同じです。 この機能はyandexに固有です。メトリカ

## farmHash64 {#farmhash64}

64ビットを生成する [FarmHash](https://github.com/google/farmhash) ハッシュ値。

``` sql
farmHash64(par1, ...)
```

この関数は、 `Hash64` すべてからの方法 [利用可能な方法](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**パラメータ**

この関数は、可変個の入力パラメータを受け取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。

**例えば**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

計算 [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) 文字列から。 このハッシュ関数は高速でも良い品質でもありません。 理由はただそれだけに使用する場合もこのアルゴリズムは実用化が始まっており他のシステムとしての計算は、全く同じ働きをします。

**構文**

``` sql
SELECT javaHash('');
```

**戻り値**

A `Int32` データ型ハッシュ値。

**例えば**

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

A `Int32` データ型ハッシュ値。

**例えば**

UTF-16LEでエンコードされた文字列で正しいクエリー。

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

これはちょうど [JavaHash](#hash_functions-javahash) サインビットをゼロにします。 この関数は、 [Apacheハイブ](https://en.wikipedia.org/wiki/Apache_Hive) 3.0より前のバージョンの場合。 このハッシュ関数は高速でも良い品質でもありません。 理由はただそれだけに使用する場合もこのアルゴリズムは実用化が始まっており他のシステムとしての計算は、全く同じ働きをします。

**戻り値**

A `Int32` データ型ハッシュ値。

タイプ: `hiveHash`.

**例えば**

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

64ビットを生成する [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) ハッシュ値。

``` sql
metroHash64(par1, ...)
```

**パラメータ**

この関数は、可変個の入力パラメータを受け取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

A [UInt64](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。

**例えば**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash {#jumpconsistenthash}

JumpConsistentHashフォームUInt64を計算します。
UInt64型のキーとバケットの数です。 Int32を返します。
詳細については、リンク: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2\_32,murmurHash2\_64 {#murmurhash2-32-murmurhash2-64}

を生成する [MurmurHash2](https://github.com/aappleby/smhasher) ハッシュ値。

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**パラメータ**

どちらの関数も、入力パラメータの可変数を取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

-   その `murmurHash2_32` 関数は、ハッシュ値を返します [UInt32](../../sql-reference/data-types/int-uint.md) データ型。
-   その `murmurHash2_64` 関数は、ハッシュ値を返します [UInt64](../../sql-reference/data-types/int-uint.md) データ型。

**例えば**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## murmurHash3\_32,murmurHash3\_64 {#murmurhash3-32-murmurhash3-64}

を生成する [MurmurHash3](https://github.com/aappleby/smhasher) ハッシュ値。

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**パラメータ**

どちらの関数も、入力パラメータの可変数を取ります。 パラメー [対応データ型](../../sql-reference/data-types/index.md).

**戻り値**

-   その `murmurHash3_32` 関数は、 [UInt32](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。
-   その `murmurHash3_64` 関数は、 [UInt64](../../sql-reference/data-types/int-uint.md) データ型ハッシュ値。

**例えば**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3\_128 {#murmurhash3-128}

128ビットを生成する [MurmurHash3](https://github.com/aappleby/smhasher) ハッシュ値。

``` sql
murmurHash3_128( expr )
```

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) を返す [文字列](../../sql-reference/data-types/string.md)-タイプ値。

**戻り値**

A [FixedString(16)](../../sql-reference/data-types/fixedstring.md) データ型ハッシュ値。

**例えば**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32,xxHash64 {#hash-functions-xxhash32}

計算 `xxHash` 文字列から。 これは、二つの味、32および64ビットで提案されています。

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**戻り値**

A `Uint32` または `Uint64` データ型ハッシュ値。

タイプ: `xxHash`.

**例えば**

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

**また見なさい**

-   [xxHash](http://cyan4973.github.io/xxHash/).

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/hash_functions/) <!--hide-->
