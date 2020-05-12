---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 41
toc_title: "\u6587\u5B57\u5217\u3092\u691C\u7D22\u3059\u308B\u5834\u5408"
---

# 文字列を検索するための関数 {#functions-for-searching-strings}

これらのすべての機能では、既定で検索では大文字と小文字が区別されます。 あるvariantのための大文字と小文字を区別しません。

## 位置（干し草の山、針）、位置（干し草の山、針) {#position}

1から始まる、文字列内の見つかった部分文字列の位置(バイト)を返します。

作品は、この文字列が含まれるセットを表すバイトの単一のバイトの符号化されます。 この仮定が満たされておらず、単一のバイトを使用して文字を表現できない場合、関数は例外をスローせず、予期しない結果を返します。 文字が二つのバイトを使用して表現できる場合は、二つのバイトなどを使用します。

大文字と小文字を区別しない検索では、次の関数を使用します [positionCaseInsensitive](#positioncaseinsensitive).

**構文**

``` sql
position(haystack, needle)
```

エイリアス: `locate(haystack, needle)`.

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合、バイト単位で開始位置（1から数えます）。
-   0、部分文字列が見つからなかった場合。

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

ロシア語の同じ句には、単一のバイトを使用して表現できない文字が含まれています。 この関数は、予期しない結果を返します [positionUTF8](#positionutf8) マルチバイトエンコードテキストの機能):

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

## positionCaseInsensitive {#positioncaseinsensitive}

同じように [位置](#position) 1から始まる、文字列内の見つかった部分文字列の位置(バイト)を返します。 大文字小文字を区別しない検索には、この関数を使用します。

作品は、この文字列が含まれるセットを表すバイトの単一のバイトの符号化されます。 この仮定が満たされておらず、単一のバイトを使用して文字を表現できない場合、関数は例外をスローせず、予期しない結果を返します。 文字が二つのバイトを使用して表現できる場合は、二つのバイトなどを使用します。

**構文**

``` sql
positionCaseInsensitive(haystack, needle)
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合、バイト単位で開始位置（1から数えます）。
-   0、部分文字列が見つからなかった場合。

タイプ: `Integer`.

**例えば**

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

文字列内の見つかった部分文字列の位置(unicodeポイント単位)を、1から開始して返します。

作品は、この文字列が含まれるセットを表すバイトのutf-8で符号化されます。 この仮定が満たされない場合、関数は例外をスローせず、予期しない結果を返します。 文字が二つのunicodeポイントを使って表現できる場合は、二つのポイントを使います。

大文字と小文字を区別しない検索では、次の関数を使用します [位置caseinsensitiveutf8](#positioncaseinsensitiveutf8).

**構文**

``` sql
positionUTF8(haystack, needle)
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合、unicodeポイントの開始位置（1から数えます）。
-   0、部分文字列が見つからなかった場合。

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

フレーズ “Salut, étudiante!”、どこの文字 `é` 一つの点を使用して表すことができます (`U+00E9`)または二つのポイント (`U+0065U+0301` 関数は、いくつかの予想外の結果を返すことができます:

手紙のためのクエリ `é`、一つのUnicodeポイントを表している `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

結果:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

手紙のためのクエリ `é` これは二つのユニコード点を表します `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

結果:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## 位置caseinsensitiveutf8 {#positioncaseinsensitiveutf8}

同じように [positionUTF8](#positionutf8) ただし、大文字と小文字は区別されません。 文字列内の見つかった部分文字列の位置(Unicodeポイント単位)を、1から開始して返します。

作品は、この文字列が含まれるセットを表すバイトのutf-8で符号化されます。 この仮定が満たされない場合、関数は例外をスローせず、予期しない結果を返します。 文字が二つのunicodeポイントを使って表現できる場合は、二つのポイントを使います。

**構文**

``` sql
positionCaseInsensitiveUTF8(haystack, needle)
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   部分文字列が見つかった場合、unicodeポイントの開始位置（1から数えます）。
-   0、部分文字列が見つからなかった場合。

タイプ: `Integer`.

**例えば**

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

## マルチアーチャルポジション {#multisearchallpositions}

同じように [位置](string-search-functions.md#position) しかし、返す `Array` 文字列内で見つかった対応する部分文字列の位置(バイト単位)。 位置は1から始まる索引付けされます。

検索は、文字列のエンコードおよび照合順序に関係なく、バイトのシーケンスで実行されます。

-   大文字と小文字を区別しないascii検索では、次の関数を使用します `multiSearchAllPositionsCaseInsensitive`.
-   UTF-8で検索する場合は、次の関数を使用します [multiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   大文字と小文字を区別しないutf-8検索の場合は、関数multitsearchallpositionscaseinsensitiveutf8を使用します。

**構文**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**パラメータ**

-   `haystack` — string, in which substring will to be searched. [文字列](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [文字列](../syntax.md#syntax-string-literal).

**戻り値**

-   対応する部分文字列が見つかった場合は1から数え、見つからなかった場合は0のバイト単位の開始位置の配列。

**例えば**

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

## マルチアーチファーストポジション(干し草の山,\[ニードル<sub>1</sub>、針<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstposition}

同じように `position` しかし、文字列の左端のオフセットを返します `haystack` それは針のいくつかに一致します。

大文字と小文字を区別しない検索やutf-8形式の場合は、関数を使用します `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## マルチアーチファーストインデックス(haystack,\[needle<sub>1</sub>、針<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

インデックスを返す `i` （1から始まる）見つかった最も左の針の<sub>私は</sub> 文字列の中で `haystack` それ以外の場合は0。

大文字と小文字を区別しない検索やutf-8形式の場合は、関数を使用します `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-faq<sub>1</sub>、針<sub>2</sub>, …, needle<sub>n</sub>\]) {#function-multisearchany}

少なくとも一つの文字列の針場合、1を返します<sub>私は</sub> 文字列に一致します `haystack` それ以外の場合は0。

大文字と小文字を区別しない検索やutf-8形式の場合は、関数を使用します `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "メモ"
    すべて `multiSearch*` 機能は針の数2よりより少しべきです<sup>8</sup> 実装仕様のため。

## マッチ（干し草の山、パターン) {#matchhaystack-pattern}

その文字列が `pattern` 正規表現。 A `re2` 正規表現。 その [構文](https://github.com/google/re2/wiki/Syntax) の `re2` 正規表現は、Perl正規表現の構文よりも制限されています。

一致しない場合は0、一致する場合は1を返します。

バックスラッシュ記号に注意してください (`\`)は、正規表現でエスケープするために使用されます。 同じ記号が文字列リテラルでエスケープするために使用されます。 したがって、正規表現でシンボルをエスケープするには、文字列リテラルに二つの円記号(\\)を記述する必要があります。

正規表現は、文字列がバイトのセットであるかのように動作します。 正規表現にnullバイトを含めることはできません。
パターンが文字列内の部分文字列を検索するには、likeまたは ‘position’、彼らははるかに高速に動作するので。

## マルチャチャー（干し草の山、\[パターン<sub>1</sub>、パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

同じように `match` ただし、正規表現のどれも一致しない場合は0を返し、パターンのいずれかが一致する場合は1を返します。 それは使用します [hyperscan](https://github.com/intel/hyperscan) ライブラリ。 文字列の部分文字列を検索するパターンの場合は、次のように使用する方がよいでしょう `multiSearchAny` それははるかに速く動作するので。

!!! note "メモ"
    の長さ `haystack` 文字列は2未満でなければなりません<sup>32</sup> それ以外の場合は、例外がスローされます。 この制限は、hyperscan APIのために行われます。

## インデックスを作成します。<sub>1</sub>、パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

同じように `multiMatchAny`、しかし、haystackに一致する任意のインデックスを返します。

## ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-ﾂ篠堕猟ｿﾂ青ｿﾂ仰<sub>1</sub>、パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

同じように `multiMatchAny` しかし、haystackに一致するすべての指標の配列を任意の順序で返します。

## マルチフザイマチャニ（干し草の山、距離、\[パターン<sub>1</sub>、パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

同じように `multiMatchAny` ただし、定数内のhaystackに一致するパターンがある場合は1を返します [距離を編集](https://en.wikipedia.org/wiki/Edit_distance). この機能は実験モードでもあり、非常に遅くなる可能性があります。 詳細については、 [hyperscanマニュアル](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## インデックスを作成します。<sub>1</sub>、パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

同じように `multiFuzzyMatchAny` しかし、一定の編集距離内のhaystackに一致する任意のインデックスを返します。

## multiFuzzyMatchAllIndices(haystack、距離、\[パターン<sub>1</sub>、パターン<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

同じように `multiFuzzyMatchAny` しかし、一定の編集距離内のhaystackに一致する任意の順序ですべてのインデックスの配列を返します。

!!! note "メモ"
    `multiFuzzyMatch*` 関数はUTF-8の正規表現をサポートしておらず、hyperscanの制限により、このような式はバイトとして扱われます。

!!! note "メモ"
    Hyperscanを使用するすべての機能をオフにするには、設定を使用します `SET allow_hyperscan = 0;`.

## エキス（干し草の山、パターン) {#extracthaystack-pattern}

正規表現を使用して文字列の断片を抽出します。 もし ‘haystack’ この ‘pattern’ 正規表現では、空の文字列が返されます。 正規表現にサブパターンが含まれていない場合は、正規表現全体に一致するフラグメントを取ります。 それ以外の場合は、最初のサブパターンに一致するフラグメントを取得します。

## extractAll(干し草の山,パターン) {#extractallhaystack-pattern}

正規表現を使用して、文字列のすべてのフラグメントを抽出します。 もし ‘haystack’ この ‘pattern’ 正規表現では、空の文字列が返されます。 正規表現に対するすべての一致で構成される文字列の配列を返します。 一般に、この動作は、 ‘extract’ 関数（最初のサブパターン、またはサブパターンがない場合は式全体を取ります）。

## like(haystack,pattern),haystack LIKEパターン演算子 {#function-like}

文字列が単純な正規表現に一致するかどうかを調べます。
正規表現には、メタシンボルを含めることができます `%` と `_`.

`%` 任意のバイト数(ゼロ文字を含む)を示します。

`_` 任意のバイトを示します。

バックスラッシュを使う (`\`)メタシンボルをエスケープするため。 の説明でエスケープの注意事項を参照してください。 ‘match’ 機能。

次のような正規表現の場合 `%needle%`、コードはより最適であり、高速として動作します `position` 機能。
その他の正規表現の場合、コードは次のようになります。 ‘match’ 機能。

## ノットライク（干し草、パターン）、干し草は、パターン演算子が好きではありません {#function-notlike}

同じものとして ‘like’、しかし否定的。

## ngramDistance(干し草の山,針) {#ngramdistancehaystack-needle}

間の4グラムの距離を計算します `haystack` と `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `needle` または `haystack` 32Kb以上で、例外をスローします。 いくつかの非定数の場合 `haystack` または `needle` 文字列は32Kb以上で、距離は常に一つです。

大文字と小文字を区別しない検索やutf-8形式の場合は、関数を使用します `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## ngramSearch(干し草の山,針) {#ngramsearchhaystack-needle}

と同じ `ngramDistance` しかし、非対称の違いを計算します `needle` と `haystack` – the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` nグラム。 一つに近いほど、より多くの可能性が高い `needle` にある `haystack`. あいまい文字列検索に便利です。

大文字と小文字を区別しない検索やutf-8形式の場合は、関数を使用します `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "メモ"
    For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/string_search_functions/) <!--hide-->
