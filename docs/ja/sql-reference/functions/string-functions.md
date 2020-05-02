---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 40
toc_title: "\u6587\u5B57\u5217\u306E\u64CD\u4F5C"
---

# 文字列を扱うための関数 {#functions-for-working-with-strings}

## 空 {#empty}

空の文字列の場合は1、空でない文字列の場合は0を返します。
結果の型はuint8です。
文字列が空白またはnullバイトであっても、少なくとも一つのバイトが含まれている場合、文字列は空ではないと見なされます。
この関数は配列に対しても機能します。

## notEmpty {#notempty}

空の文字列の場合は0、空でない文字列の場合は1を返します。
結果の型はuint8です。
この関数は配列に対しても機能します。

## 長さ {#length}

文字列の長さをバイトで返します(コード-ポイントではなく、文字ではありません)。
結果の型はuint64です。
この関数は配列に対しても機能します。

## lengthUTF8 {#lengthutf8}

文字列にutf-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、unicodeコードポイント(文字ではない)の文字列の長さを この仮定が満たされない場合、いくつかの結果が返されます（例外はスローされません）。
結果の型はuint64です。

## char\_length,CHAR\_LENGTH {#char-length}

文字列にutf-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、unicodeコードポイント(文字ではない)の文字列の長さを この仮定が満たされない場合、いくつかの結果が返されます（例外はスローされません）。
結果の型はuint64です。

## character\_length,CHARACTER\_LENGTH {#character-length}

文字列にutf-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、unicodeコードポイント(文字ではない)の文字列の長さを この仮定が満たされない場合、いくつかの結果が返されます（例外はスローされません）。
結果の型はuint64です。

## lower,lcase {#lower}

文字列内のasciiラテン文字記号を小文字に変換します。

## アッパー,ucase {#upper}

文字列内のasciiラテン文字記号を大文字に変換します。

## lowerUTF8 {#lowerutf8}

文字列にutf-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、文字列を小文字に変換します。
それは言語を検出しません。 そのためにトルコに結果が正確に正しい。
コード-ポイントの大文字と小文字でutf-8バイト-シーケンスの長さが異なる場合、このコード-ポイントでは結果が正しくない可能性があります。
文字列にutf-8でないバイトのセットが含まれている場合、その動作は未定義です。

## upperUTF8 {#upperutf8}

文字列にutf-8でエンコードされたテキストを構成するバイトのセットが含まれている場合、文字列を大文字に変換します。
それは言語を検出しません。 そのためにトルコに結果が正確に正しい。
コード-ポイントの大文字と小文字でutf-8バイト-シーケンスの長さが異なる場合、このコード-ポイントでは結果が正しくない可能性があります。
文字列にutf-8でないバイトのセットが含まれている場合、その動作は未定義です。

## isValidUTF8 {#isvalidutf8}

バイトのセットが有効なutf-8エンコードの場合は1を返し、それ以外の場合は0を返します。

## toValidUTF8 {#tovalidutf8}

無効なutf-8文字を `�` (U+FFFD)文字。 すべての行で実行されている無効な文字は、置換文字に折りたたまれています。

``` sql
toValidUTF8( input_string )
```

パラメータ:

-   input\_string — Any set of bytes represented as the [文字列](../../sql-reference/data-types/string.md) データ型オブジェクト。

戻り値:有効なutf-8文字列。

**例えば**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## 繰り返す {#repeat}

指定した回数だけ文字列を繰り返し、レプリケートされた値を単一の文字列として連結します。

**構文**

``` sql
repeat(s, n)
```

**パラメータ**

-   `s` — The string to repeat. [文字列](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [UInt](../../sql-reference/data-types/int-uint.md).

**戻り値**

文字列を含む単一の文字列 `s` 繰り返す `n` 回。 もし `n` \<1、関数は、空の文字列を返します。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT repeat('abc', 10)
```

結果:

``` text
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## 反転 {#reverse}

文字列を逆にします(バイトのシーケンスとして)。

## reverseUTF8 {#reverseutf8}

文字列にutf-8テキストを表すバイトのセットが含まれていると仮定して、unicodeコードポイントのシーケンスを逆にします。 それ以外の場合は、何か他のことをします（例外はスローされません）。

## format(pattern, s0, s1, …) {#format}

引数にリストされている文字列で定数パターンを書式設定する。 `pattern` 単純なPython形式のパターンです。 書式文字列に含まれる “replacement fields” 中括弧で囲まれています `{}`. 中括弧に含まれていないものは、リテラルテキストと見なされ、出力には変更されません。 リテラルテキストに中かっこ文字を含める必要がある場合は、倍にすることでエスケープできます: `{{ '{{' }}` と `{{ '}}' }}`. フィールド名は数字（ゼロから始まる）または空（それらは結果番号として扱われます）。

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

``` text
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

``` text
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat {#concat}

引数にリストされている文字列を区切り文字なしで連結します。

**構文**

``` sql
concat(s1, s2, ...)
```

**パラメータ**

String型またはFixedString型の値。

**戻り値**

引数を連結した結果の文字列を返します。

引数の値のいずれかがある場合 `NULL`, `concat` を返します `NULL`.

**例えば**

クエリ:

``` sql
SELECT concat('Hello, ', 'World!')
```

結果:

``` text
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

## ﾂづﾂつｿﾂづｫﾂづｱﾂ、ﾂ債｡ {#concatassumeinjective}

と同じ [concat](#concat) の差であることを確認する必要があり `concat(s1, s2, ...) → sn` injectiveは、GROUP BYの最適化に使用されます。

関数の名前は次のとおりです “injective” 引数の異なる値に対して常に異なる結果を返す場合。 言い換えれば異なる引数のない利回り同一の結果です。

**構文**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**パラメータ**

String型またはFixedString型の値。

**戻り値**

引数を連結した結果の文字列を返します。

引数の値のいずれかがある場合 `NULL`, `concatAssumeInjective` を返します `NULL`.

**例えば**

入力テーブル:

``` sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

``` text
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

クエリ:

``` sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2)
```

結果:

``` text
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## サブストリング(s,オフセット,長さ)、中(s,オフセット,長さ)、サブストリング(s,オフセット,長さ) {#substring}

からのバイトで始まる部分文字列を返します。 ‘offset’ あるインデックス ‘length’ バイト長。 文字の索引付けは、（標準SQLのように）文字から始まります。 その ‘offset’ と ‘length’ 引数は定数である必要があります。

## substringUTF8(s,オフセット,長さ) {#substringutf8}

同じように ‘substring’ しかし、Unicodeコードポイントの場合。 作品は、この文字列が含まれるセットを表すバイトのUTF-8で符号化されます。 この仮定が満たされない場合、いくつかの結果が返されます（例外はスローされません）。

## appendTrailingCharIfAbsent(s,c) {#appendtrailingcharifabsent}

この ‘s’ 文字列は空ではなく、空の文字列を含みません。 ‘c’ 最後の文字は、 ‘c’ 最後に文字。

## convertCharset(s,from,to) {#convertcharset}

文字列を返します ‘s’ それはエンコーディングから変換された ‘from’ でのエンコーディングに ‘to’.

## base64Encode(s) {#base64encode}

エンコード ‘s’ base64への文字列

## base64Decode(s) {#base64decode}

Base64エンコードされた文字列のデコード ‘s’ 元の文字列に。 失敗した場合には例外を発生させます。

## tryBase64Decode(s) {#trybase64decode}

Base64Decodeに似ていますが、エラーの場合は空の文字列が返されます。

## endsWith(s,suffix) {#endswith}

指定された接尾辞で終了するかどうかを返します。 文字列が指定された接尾辞で終わる場合は1を返し、それ以外の場合は0を返します。

## startsWith(str,プレフィックス) {#startswith}

1を返しますか否かの文字列の開始を、指定された接頭辞、そうでない場合は0を返します。

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

**戻り値**

-   1、文字列が指定された接頭辞で始まる場合。
-   文字列が指定された接頭辞で始まらない場合は0。

**例えば**

クエリ:

``` sql
SELECT startsWith('Hello, world!', 'He');
```

結果:

``` text
┌─startsWith('Hello, world!', 'He')─┐
│                                 1 │
└───────────────────────────────────┘
```

## トリム {#trim}

文字列の先頭または末尾から指定されたすべての文字を削除します。
デフォルトでは、文字列の両端から共通の空白（ascii文字32）が連続して出現するすべてを削除します。

**構文**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**パラメータ**

-   `trim_character` — specified characters for trim. [文字列](../../sql-reference/data-types/string.md).
-   `input_string` — string for trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

先頭および(または)末尾に指定された文字を含まない文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )')
```

結果:

``` text
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

文字列の先頭から、共通の空白文字(ascii文字32)のすべての連続した出現を削除します。 他の種類の空白文字（タブ、改行なしなど）は削除されません。).

**構文**

``` sql
trimLeft(input_string)
```

エイリアス: `ltrim(input_string)`.

**パラメータ**

-   `input_string` — string to trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

共通の空白をリードしない文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT trimLeft('     Hello, world!     ')
```

結果:

``` text
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

文字列の末尾から共通の空白文字(ascii文字32)のすべての連続した出現を削除します。 他の種類の空白文字（タブ、改行なしなど）は削除されません。).

**構文**

``` sql
trimRight(input_string)
```

エイリアス: `rtrim(input_string)`.

**パラメータ**

-   `input_string` — string to trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

共通の空白を末尾に付けない文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT trimRight('     Hello, world!     ')
```

結果:

``` text
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## トリンボスcity in california usa {#trimboth}

文字列の両端から共通の空白文字(ascii文字32)が連続して出現するすべてを削除します。 他の種類の空白文字（タブ、改行なしなど）は削除されません。).

**構文**

``` sql
trimBoth(input_string)
```

エイリアス: `trim(input_string)`.

**パラメータ**

-   `input_string` — string to trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

先頭と末尾の共通の空白を含まない文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT trimBoth('     Hello, world!     ')
```

結果:

``` text
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32(s) {#crc32}

CRC-32-IEEE802.3多項式と初期値を使用して、文字列のCRC32チェックサムを返します `0xffffffff` （zlibの実装）。

結果の型はuint32です。

## CRC32IEEE(s) {#crc32ieee}

CRC-32-IEEE802.3多項式を使用して、文字列のCRC32チェックサムを返します。

結果の型はuint32です。

## CRC64(s) {#crc64}

CRC-64-ECMA多項式を使用して、文字列のCRC64チェックサムを返します。

結果の型はuint64です。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/string_functions/) <!--hide-->
