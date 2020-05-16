---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u6587\u5B57\u5217\u3092\u691C\u7D22\u3059\u308B\u5834\u5408"
---

# 文字列を検索する関数 {#functions-for-searching-strings}

これらのすべての関数では、デフォルトで大文字と小文字が区別されます。 あるvariantのための大文字と小文字を区別しません。

## 位置（干し草の山、針）、位置（干し草の山、針) {#position}

文字列内で見つかった部分文字列の位置(バイト単位)を1から始めて返します。

作品は、この文字列が含まれるセットを表すバイトの単一のバイトの符号化されます。 この仮定が満たされず、文字を単一バイトで表すことができない場合、関数は例外をスローせず、予期しない結果を返します。 文字を二つのバイトで表現できる場合は、二つのバイトなどを使用します。

大文字と小文字を区別しない検索では、次の関数を使用します [ポジションカースインセンティブ](#positioncaseinsensitive).

**構文**

``` sql
position(haystack, needle)
```

別名: `locate(haystack, needle)`.

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合は、開始位置(1から数える)をバイト単位で指定します。
-   部分文字列が見つからなかった場合は0。

タイプ: `Integer`.

**例**

フレーズ “Hello, world!” を含むの設定を表すバイトの単一のバイトの符号化されます。 この関数は、期待される結果を返します:

クエリ:

``` sql
SELECT position('Hello, world!', '!')
```

結果:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

ロシア語の同じ句には、単一バイトで表現できない文字が含まれています。 この関数は予期しない結果を返します(使用 [positionUTF8](#positionutf8) マルチバイトエンコードテキストの関数):

クエリ:

``` sql
SELECT position('Привет, мир!', '!')
```

結果:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

## ポジションカースインセンティブ {#positioncaseinsensitive}

と同じ [位置](#position) 文字列内で見つかった部分文字列の位置(バイト単位)を1から始めて返します。 大文字と小文字を区別しない検索には、この関数を使用します。

作品は、この文字列が含まれるセットを表すバイトの単一のバイトの符号化されます。 この仮定が満たされず、文字を単一バイトで表すことができない場合、関数は例外をスローせず、予期しない結果を返します。 文字を二つのバイトで表現できる場合は、二つのバイトなどを使用します。

**構文**

``` sql
positionCaseInsensitive(haystack, needle)
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合は、開始位置(1から数える)をバイト単位で指定します。
-   部分文字列が見つからなかった場合は0。

タイプ: `Integer`.

**例**

クエリ:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello')
```

結果:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8 {#positionutf8}

文字列内で見つかった部分文字列の位置(Unicodeポイント)を1から始めて返します。

作品は、この文字列が含まれるセットを表すバイトのUTF-8で符号化されます。 この仮定が満たされない場合、関数は例外をスローせず、予期しない結果を返します。 文字が二つのUnicodeポイントを使用して表すことができる場合、それはように二つを使用します。

大文字と小文字を区別しない検索では、次の関数を使用します [positionCaseInsensitiveUTF8](#positioncaseinsensitiveutf8).

**構文**

``` sql
positionUTF8(haystack, needle)
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合、Unicodeポイントの開始位置（1から数える）。
-   部分文字列が見つからなかった場合は0。

タイプ: `Integer`.

**例**

フレーズ “Hello, world!” ロシア語のUnicodeのポイントを表すシングルポイントで符号化されます。 この関数は、期待される結果を返します:

クエリ:

``` sql
SELECT positionUTF8('Привет, мир!', '!')
```

結果:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

フレーズ “Salut, étudiante!”,ここで文字 `é` 一点を使って表現することができます (`U+00E9`）または二点 (`U+0065U+0301`）関数は予期しない結果を返すことができます:

手紙のクエリ `é` これは一つのUnicodeポイントで表されます `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

結果:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

手紙のクエリ `é` これは二つのUnicodeポイントで表されます `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

結果:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## positionCaseInsensitiveUTF8 {#positioncaseinsensitiveutf8}

と同じ [positionUTF8](#positionutf8) しかし、大文字と小文字は区別されません。 文字列内で見つかった部分文字列の位置(Unicodeポイント)を1から始めて返します。

作品は、この文字列が含まれるセットを表すバイトのUTF-8で符号化されます。 この仮定が満たされない場合、関数は例外をスローせず、予期しない結果を返します。 文字が二つのUnicodeポイントを使用して表すことができる場合、それはように二つを使用します。

**構文**

``` sql
positionCaseInsensitiveUTF8(haystack, needle)
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合、Unicodeポイントの開始位置（1から数える）。
-   部分文字列が見つからなかった場合は0。

タイプ: `Integer`.

**例**

クエリ:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')
```

結果:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## マルチサーチアルポジション {#multisearchallpositions}

と同じ [位置](string-search-functions.md#position) しかし、戻り `Array` 文字列内で見つかった対応する部分文字列の位置(バイト単位)。 位置は1から始まる索引付けです。

の検索を行い、配列のバイトを尊重することなく文字列エンコーディングと照合。

-   大文字と小文字を区別しないASCII検索では、次の関数を使用します `multiSearchAllPositionsCaseInsensitive`.
-   UTF-8で検索するには、次の関数を使用します [multiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   大文字と小文字を区別しないUTF-8検索では、関数multiSearchAllPositionsCaseInsensitiveutf8を使用します。

**構文**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   対応する部分文字列が見つかった場合は1から数えます。

**例**

クエリ:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])
```

結果:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8 {#multiSearchAllPositionsUTF8}

見る `multiSearchAllPositions`.

## マルチサーチファーストポジション(ヘイスタック、\[針<sub>1</sub>、針<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstposition}

と同じ `position` しかし、文字列の左端のオフセットを返します `haystack` それは針のいくつかに一致します。

大文字と小文字を区別しない検索または/およびUTF-8形式の場合は、関数を使用します `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## マルチサーチファーストインデックス(haystack,\[needle<sub>1</sub>、針<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

インデックスを返す `i` 一番左にある針の1から始まります<sub>私は</sub> 文字列の中で `haystack` それ以外の場合は0です。

大文字と小文字を区別しない検索または/およびUTF-8形式の場合は、関数を使用します `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## マルチサーチャニー（干し草、\[針<sub>1</sub>、針<sub>2</sub>, …, needle<sub>n</sub>\]) {#function-multisearchany}

1を返します。<sub>私は</sub> 文字列と一致します `haystack` それ以外の場合は0です。

大文字と小文字を区別しない検索または/およびUTF-8形式の場合は、関数を使用します `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "注"
    すべて `multiSearch*` 機能針の数は2よりより少しべきです<sup>8</sup> 実装仕様のため。

## マッチ(曖昧さ回避) {#matchhaystack-pattern}

文字列が `pattern` 正規表現。 A `re2` 正規表現。 その [構文](https://github.com/google/re2/wiki/Syntax) の `re2` 正規表現は、Perl正規表現の構文よりも制限されています。

一致しない場合は0、一致する場合は1を返します。

バックスラッシュ記号は (`\`)は正規表現でのエスケープに使用されます。 文字列リテラルのエスケープには同じ記号が使用されます。 したがって、正規表現のシンボルをエスケープするには、文字列リテラルに二つの円記号(\\)を記述する必要があります。

正規表現は、文字列がバイトのセットであるかのように動作します。 正規表現にはnullバイトを含めることはできません。
文字列内の部分文字列を検索するパターンの場合は、LIKEまたはを使用する方が良いです ‘position’ 彼らははるかに高速に動作するので。

## マルチマッチャニー(曖昧さ回避<sub>1</sub>,パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

と同じ `match` しかし、正規表現が一致しない場合は0を返し、パターンが一致する場合は1を返します。 それは使用します [hyperscan](https://github.com/intel/hyperscan) 図書館 パターンが文字列内の部分文字列を検索するには、以下を使用する方が良いです `multiSearchAny` それははるかに高速に動作するので。

!!! note "注"
    のいずれかの長さ `haystack` 文字列は2未満でなければなりません<sup>32</sup> それ以外の場合は例外がスローされます。 この制限はhyperscan APIのために行われます。

## multiMatchAnyIndex(ヘイスタック,\[パターン<sub>1</sub>,パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

と同じ `multiMatchAny` しかし、干し草の山に一致する任意のインデックスを返します。

## multiMatchAllIndices(干し草の山、\[パターン<sub>1</sub>,パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

と同じ `multiMatchAny` しかし、干し草の山に一致するすべての指標の配列を任意の順序で返します。

## マルチフジマッチャニー(干し草、距離、\[パターン<sub>1</sub>,パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

と同じ `multiMatchAny` しかし、定数内の干し草に一致するパターンがあれば1を返します [距離を編集](https://en.wikipedia.org/wiki/Edit_distance). この機能は実験モードでもあり、非常に遅くなる可能性があります。 詳細については、 [hyperscanドキュメント](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## multiFuzzyMatchAnyIndex(干し草の山,距離,\[パターン<sub>1</sub>,パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

と同じ `multiFuzzyMatchAny` しかし、一定の編集距離内の干し草の山に一致するインデックスを返します。

## multiFuzzyMatchAllIndices(干し草の山,距離,\[パターン<sub>1</sub>,パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

と同じ `multiFuzzyMatchAny`,butは、一定の編集距離内の干し草の山に一致する任意の順序ですべてのインデックスの配列を返します。

!!! note "注"
    `multiFuzzyMatch*` 関数はUTF-8正規表現をサポートしておらず、ハイパースキャンの制限によりこのような式はバイトとして扱われます。

!!! note "注"
    Hyperscanを使用するすべての機能をオフにするには、設定を使用します `SET allow_hyperscan = 0;`.

## 抽出(干し草、パターン) {#extracthaystack-pattern}

正規表現を使用して文字列の断片を抽出します。 もし ‘haystack’ 一致しない ‘pattern’ 正規表現では、空の文字列が返されます。 正規表現にサブパターンが含まれていない場合は、正規表現全体に一致するフラグメントを取ります。 それ以外の場合は、最初のサブパターンに一致するフラグメントを取ります。

## extractAll(干し草の山、パターン) {#extractallhaystack-pattern}

正規表現を使用して文字列のすべてのフラグメントを抽出します。 もし ‘haystack’ 一致しない ‘pattern’ 正規表現では、空の文字列が返されます。 正規表現に一致するすべての文字列からなる配列を返します。 一般に、動作は ‘extract’ 関数（最初のサブパターンを取り、サブパターンがない場合は式全体を取ります）。

## like(干し草の山,パターン),干し草の山のようなパターン演算子 {#function-like}

文字列が単純な正規表現に一致するかどうかを確認します。
正規表現には、メタシンボルを含めることができます `%` と `_`.

`%` 任意のバイト数(ゼロ文字を含む)を示します。

`_` 任意のバイトを示します。

バックスラッシュを使用する (`\`）メタシンボルを脱出するため。 の説明のエスケープに関する注意を参照してください。 ‘match’ 機能。

次のような正規表現の場合 `%needle%` コードは、より最適であり、同じくらい速く動作します `position` 機能。
他の正規表現の場合、コードは ‘match’ 機能。

## notLike(haystack,pattern),haystack NOT LIKE pattern演算子 {#function-notlike}

同じことと ‘like’ しかし、否定的。

## グラムディスタンス(曖昧さ回避) {#ngramdistancehaystack-needle}

間の4グラムの距離を計算します `haystack` と `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `needle` または `haystack` 32Kbを超える場合は、例外をスローします。 非定数のいくつかの場合 `haystack` または `needle` 文字列は32Kbを超え、距離は常に一つです。

大文字と小文字を区別しない検索や、UTF-8形式の場合は関数を使用します `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## グラムサーチ(曖昧さ回避) {#ngramsearchhaystack-needle}

同じ `ngramDistance` しかし、の間の非対称差を計算します `needle` と `haystack` – the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` nグラム 近いほど、可能性が高くなります `needle` にある `haystack`. あいまい文字列検索に役立ちます。

大文字と小文字を区別しない検索や、UTF-8形式の場合は関数を使用します `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "注"
    For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/string_search_functions/) <!--hide-->
