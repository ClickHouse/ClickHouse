---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u6587\u5B57\u5217\u306E\u64CD\u4F5C"
---

# 文字列を操作するための関数 {#functions-for-working-with-strings}

## 空 {#empty}

空の文字列の場合は1、空でない文字列の場合は0を返します。
結果の型はUInt8です。
文字列が空白またはnullバイトであっても、少なくとも一つのバイトが含まれている場合、文字列は空ではないと見なされます。
この関数は配列に対しても機能します。

## ノーテンプティ {#notempty}

空の文字列の場合は0、空でない文字列の場合は1を返します。
結果の型はUInt8です。
この関数は配列に対しても機能します。

## 長さ {#length}

文字列の長さをバイト単位で返します(文字ではなく、コードポイントではありません)。
結果の型はUInt64です。
この関数は配列に対しても機能します。

## lengthUTF8 {#lengthutf8}

文字列にUTF-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、Unicodeコードポイント(文字ではない)で文字列の長さを返 この仮定が満たされない場合、結果が返されます（例外はスローされません）。
結果の型はUInt64です。

## char_length,CHAR_LENGTH {#char-length}

文字列にUTF-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、Unicodeコードポイント(文字ではない)で文字列の長さを返 この仮定が満たされない場合、結果が返されます（例外はスローされません）。
結果の型はUInt64です。

## character_length,CHARACTER_LENGTH {#character-length}

文字列にUTF-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、Unicodeコードポイント(文字ではない)で文字列の長さを返 この仮定が満たされない場合、結果が返されます（例外はスローされません）。
結果の型はUInt64です。

## lower,lcase {#lower}

文字列内のASCIIラテン文字記号を小文字に変換します。

## アッパー,ucase {#upper}

文字列内のASCIIラテン文字記号を大文字に変換します。

## lowerUTF8 {#lowerutf8}

文字列にUTF-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、文字列を小文字に変換します。
それは言語を検出しません。 そのためにトルコに結果が正確に正しい。
UTF-8バイト-シーケンスの長さがコード-ポイントの大文字と小文字で異なる場合、このコード-ポイントの結果が正しくない可能性があります。
文字列にUTF-8以外のバイトセットが含まれている場合、動作は未定義です。

## upperUTF8 {#upperutf8}

文字列にUTF-8でエンコードされたテキストを構成するバイトのセットが含まれていると仮定して、文字列を大文字に変換します。
それは言語を検出しません。 そのためにトルコに結果が正確に正しい。
UTF-8バイト-シーケンスの長さがコード-ポイントの大文字と小文字で異なる場合、このコード-ポイントの結果が正しくない可能性があります。
文字列にUTF-8以外のバイトセットが含まれている場合、動作は未定義です。

## isValidUTF8 {#isvalidutf8}

バイトセットが有効なUTF-8エンコードの場合は1を返し、それ以外の場合は0を返します。

## toValidUTF8 {#tovalidutf8}

無効なUTF-8文字を `�` (U+FFFD)文字。 行で実行されているすべての無効な文字は、一つの置換文字に折りたたまれます。

``` sql
toValidUTF8( input_string )
```

パラメータ:

-   input_string — Any set of bytes represented as the [文字列](../../sql-reference/data-types/string.md) データ型オブジェクト。

戻り値:有効なUTF-8文字列。

**例**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## 繰り返し {#repeat}

指定した回数だけ文字列を繰り返し、複製された値を単一の文字列として連結します。

**構文**

``` sql
repeat(s, n)
```

**パラメータ**

-   `s` — The string to repeat. [文字列](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [UInt](../../sql-reference/data-types/int-uint.md).

**戻り値**

文字列を含む単一の文字列 `s` 繰り返し `n` タイムズ もし `n` \<1、関数は空の文字列を返します。

タイプ: `String`.

**例**

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

## 逆 {#reverse}

文字列を(バイトのシーケンスとして)反転します。

## reverseUTF8 {#reverseutf8}

文字列にUTF-8テキストを表すバイトのセットが含まれていると仮定して、Unicodeコードポイントのシーケンスを反転します。 それ以外の場合は、何か他のことをします（例外はスローされません）。

## format(pattern, s0, s1, …) {#format}

引数にリストされた文字列で定数パターンを書式設定します。 `pattern` 単純化されたPython形式のパターンです。 書式指定文字列 “replacement fields” 中括弧で囲む `{}`. 中かっこに含まれていないものはリテラルテキストと見なされ、そのまま出力にコピーされます。 リテラルテキストに中かっこ文字を含める必要がある場合は、倍にすることでエスケープできます: `{{ '{{' }}` と `{{ '}}' }}`. フィールド名には、数値(ゼロから始まる)または空(結果の数値として扱われます)を指定できます。

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

## コンカ {#concat}

引数にリストされている文字列を、区切り記号なしで連結します。

**構文**

``` sql
concat(s1, s2, ...)
```

**パラメータ**

String型またはFixedString型の値。

**戻り値**

引数を連結した結果の文字列を返します。

引数値のいずれかが `NULL`, `concat` ﾂづｩﾂ。 `NULL`.

**例**

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

## concatAssumeInjective {#concatassumeinjective}

同じ [コンカ](#concat) 違いは、次のことを確認する必要があるということです `concat(s1, s2, ...) → sn` はinjectiveであり、GROUP BYの最適化に使用されます。

関数の名前は次のとおりです “injective” 引数の異なる値に対して常に異なる結果を返す場合。 言い換えれば異なる引数のない利回り同一の結果です。

**構文**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**パラメータ**

String型またはFixedString型の値。

**戻り値**

引数を連結した結果の文字列を返します。

引数値のいずれかが `NULL`, `concatAssumeInjective` ﾂづｩﾂ。 `NULL`.

**例**

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

## substring(s,offset,length),mid(s,offset,length),substr(s,offset,length) {#substring}

バイトで始まる部分文字列を返します。 ‘offset’ であるインデックス ‘length’ バイト長。 文字割り出し開始から一つとしての標準SQL). その ‘offset’ と ‘length’ 引数は定数である必要があります。

## substringUTF8(s,オフセット,長さ) {#substringutf8}

と同じ ‘substring’ しかし、Unicodeコードポイントの場合。 作品は、この文字列が含まれるセットを表すバイトのUTF-8で符号化されます。 この仮定が満たされない場合、結果が返されます（例外はスローされません）。

## appendTrailingCharIfAbsent(s,c) {#appendtrailingcharifabsent}

もし ‘s’ 文字列は空ではなく、 ‘c’ 最後に文字を追加します。 ‘c’ 最後に文字。

## convertCharset(s,from,to) {#convertcharset}

文字列を返します ‘s’ それはで符号化から変換された ‘from’ のエンコーディングに ‘to’.

## base64Encode(s) {#base64encode}

エンコード ‘s’ base64への文字列

## base64Decode(s) {#base64decode}

Base64エンコードされた文字列をデコード ‘s’ 元の文字列に。 障害が発生した場合には例外を発生させます。

## tryBase64Decode(s) {#trybase64decode}

Base64Decodeに似ていますが、エラーの場合は空の文字列が返されます。

## endsWith(s,接尾辞) {#endswith}

指定した接尾辞で終わるかどうかを返します。 文字列が指定された接尾辞で終わる場合は1を返し、それ以外の場合は0を返します。

## startsWith(str,プレフィックス) {#startswith}

文字列が指定された接頭辞で始まるかどうかは1を返します。

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

**戻り値**

-   文字列が指定された接頭辞で始まる場合は、1。
-   文字列が指定された接頭辞で始まらない場合は0。

**例**

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

指定したすべての文字を文字列の先頭または末尾から削除します。
デフォルトでは、文字列の両端から共通の空白(ASCII文字32)が連続して出現するすべてを削除します。

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

**例**

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

## トリムレフト {#trimleft}

文字列の先頭から連続して出現する共通の空白(ASCII文字32)をすべて削除します。 他の種類の空白文字（タブ、改行なしスペースなど）は削除されません。).

**構文**

``` sql
trimLeft(input_string)
```

別名: `ltrim(input_string)`.

**パラメータ**

-   `input_string` — string to trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

共通の空白の先頭を持たない文字列。

タイプ: `String`.

**例**

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

## トリムライト {#trimright}

文字列の末尾から連続して出現するすべての共通空白(ASCII文字32)を削除します。 他の種類の空白文字（タブ、改行なしスペースなど）は削除されません。).

**構文**

``` sql
trimRight(input_string)
```

別名: `rtrim(input_string)`.

**パラメータ**

-   `input_string` — string to trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

共通の空白を末尾に付けない文字列。

タイプ: `String`.

**例**

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

## トリンボス {#trimboth}

文字列の両端から共通の空白(ASCII文字32)が連続して出現するすべてを削除します。 他の種類の空白文字（タブ、改行なしスペースなど）は削除されません。).

**構文**

``` sql
trimBoth(input_string)
```

別名: `trim(input_string)`.

**パラメータ**

-   `input_string` — string to trim. [文字列](../../sql-reference/data-types/string.md).

**戻り値**

共通の空白文字の先頭と末尾を持たない文字列。

タイプ: `String`.

**例**

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

結果の型はUInt32です。

## CRC32IEEE(s) {#crc32ieee}

CRC-32-IEEE802.3多項式を使用して、文字列のCRC32チェックサムを返します。

結果の型はUInt32です。

## CRC64(s) {#crc64}

CRC-64-ECMA多項式を使用して、文字列のCRC64チェックサムを返します。

結果の型はUInt64です。

[元の記事](https://clickhouse.com/docs/en/query_language/functions/string_functions/) <!--hide-->
