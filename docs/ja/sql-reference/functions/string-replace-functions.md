---
slug: /ja/sql-reference/functions/string-replace-functions
sidebar_position: 150
sidebar_label: 文字列の置換関数
---

# 文字列の置換関数

[一般的な文字列関数](string-functions.md)および[文字列の検索関数](string-search-functions.md)は別々に説明されています。

## overlay

1から始まるインデックス`offset`で、文字列`input`の一部を別の文字列`replace`で置換します。

**構文**

```sql
overlay(s, replace, offset[, length])
```

**パラメータ**

- `s`: 文字列型 [String](../data-types/string.md)。
- `replace`: 文字列型 [String](../data-types/string.md)。
- `offset`: 整数型 [Int](../data-types/int-uint.md) (1-based)。`offset` が負の場合、それは文字列 `s` の末尾から数えます。
- `length`: オプション。整数型 [Int](../data-types/int-uint.md)。`length` は、入力文字列 `s` 内で置換される部分の長さを指定します。`length` が指定されていない場合、`s` から削除されるバイト数は `replace` の長さと同じです。指定されている場合は、 `length` バイトが削除されます。

**返される値**

- [String](../data-types/string.md) データ型の値。

**例**

```sql
SELECT overlay('My father is from Mexico.', 'mother', 4) AS res;
```

結果:

```text
┌─res──────────────────────┐
│ My mother is from Mexico.│
└──────────────────────────┘
```

```sql
SELECT overlay('My father is from Mexico.', 'dad', 4, 6) AS res;
```

結果:

```text
┌─res───────────────────┐
│ My dad is from Mexico.│
└───────────────────────┘
```

## overlayUTF8

1から始まるインデックス`offset`で、文字列`input`の一部を別の文字列`replace`で置換します。

文字列が有効なUTF-8でエンコードされたテキストを含むことを前提としています。この前提が破られた場合、例外は投げられず、結果は未定義です。

**構文**

```sql
overlayUTF8(s, replace, offset[, length])
```

**パラメータ**

- `s`: 文字列型 [String](../data-types/string.md)。
- `replace`: 文字列型 [String](../data-types/string.md)。
- `offset`: 整数型 [Int](../data-types/int-uint.md) (1-based)。`offset` が負の場合、それは入力文字列 `s` の末尾から数えます。
- `length`: オプション。整数型 [Int](../data-types/int-uint.md)。`length` は、入力文字列 `s` 内で置換される部分の長さを指定します。`length` が指定されていない場合、`s` から削除される文字数は `replace` の長さと同じです。指定されている場合は、 `length` 文字が削除されます。

**返される値**

- [String](../data-types/string.md) データ型の値。

**例**

```sql
SELECT overlay('Mein Vater ist aus Österreich.', 'der Türkei', 20) AS res;
```

結果:

```text
┌─res───────────────────────────┐
│ Mein Vater ist aus der Türkei.│
└───────────────────────────────┘
```

## replaceOne

`haystack` 中の最初のサブストリング `pattern` を `replacement` 文字列で置換します。

**構文**

```sql
replaceOne(haystack, pattern, replacement)
```

## replaceAll

`haystack` 中のすべてのサブストリング `pattern` を `replacement` 文字列で置換します。

**構文**

```sql
replaceAll(haystack, pattern, replacement)
```

別名: `replace`.

## replaceRegexpOne

正規表現 `pattern` ( [re2 syntax](https://github.com/google/re2/wiki/Syntax) で) に一致する最初のサブストリングを `haystack` から `replacement` 文字列で置換します。

`replacement` には置換 `\0-\9` を含めることができます。
置換 `\1-\9` は1から9までのキャプチャグループ (サブマッチ) に対応し、置換 `\0` は全体のマッチに対応します。

`pattern` または `replacement` 文字列に逐語的な `\` 文字を使用するには、 `\` を使用してエスケープします。
文字列リテラルは追加のエスケープが必要であることにも注意してください。

**構文**

```sql
replaceRegexpOne(haystack, pattern, replacement)
```

**例**

ISO形式の日付をアメリカの形式に変換する:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

結果:

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

文字列を10回コピーする:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

結果:

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll

`replaceRegexpOne` と似ていますが、パターンのすべての出現を置換します。

別名: `REGEXP_REPLACE`.

**例**

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

結果:

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

例外として、正規表現が空のサブストリングで機能した場合、置換は一度だけ行われ、次のようになります:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

結果:

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## regexpQuoteMeta

正規表現で特別な意味を持つ次の文字の前にバックスラッシュを追加します: `\0`, `\\`, `|`, `(`, `)`, `^`, `$`, `.`, `[`, `]`, `?`, `*`, `+`, `{`, `:`, `-`.

この実装は re2::RE2::QuoteMeta とは若干異なります。ゼロバイトを `\x00` ではなく `\0` としてエスケープし、必要な文字のみエスケープします。
詳細については、[RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473) を参照してください。

**構文**

```sql
regexpQuoteMeta(s)
```

## format

`pattern` 文字列を引数にリストされた値（文字列、整数など）でフォーマットします。Pythonのフォーマットに似ています。`pattern` 文字列には、中括弧 `{}` で囲まれた置換フィールドを含めることができます。中括弧に含まれていないものは、リテラルテキストと見なされ、出力にそのままコピーされます。リテラル中括弧は二重中括弧 `{{ '{{' }}` および `{{ '}}' }}` でエスケープできます。フィールド名は0から始まる数字または空白にでき、空の場合は単調に増加する番号が暗黙的に与えられます。

**構文**

```sql
format(pattern, s0, s1, ...)
```

**例**

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

```result
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

暗黙の番号を使用した場合:

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

```result
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## translate

文字列 `s` 内の文字を、`from` および `to` 文字列で定義された一対一の文字マッピングを使用して置換します。`from` および `to` は同じサイズの定数ASCII文字列でなければなりません。元の文字列の非ASCII文字は変更されません。

**構文**

```sql
translate(s, from, to)
```

**例**

``` sql
SELECT translate('Hello, World!', 'delor', 'DELOR') AS res
```

結果:

``` text
┌─res───────────┐
│ HELLO, WORLD! │
└───────────────┘
```

## translateUTF8

[translate](#translate) と同様ですが、`s`, `from`, `to` がUTF-8でエンコードされた文字列であることを前提としています。

**構文**

``` sql
translateUTF8(s, from, to)
```

**パラメータ**

- `s`: 文字列型 [String](../data-types/string.md)。
- `from`: 文字列型 [String](../data-types/string.md)。
- `to`: 文字列型 [String](../data-types/string.md)。

**返される値**

- [String](../data-types/string.md) データ型の値。

**例**

クエリ:

``` sql
SELECT translateUTF8('Münchener Straße', 'üß', 'us') AS res;
```

``` response
┌─res──────────────┐
│ Munchener Strase │
└──────────────────┘
```

## printf

`printf` 関数は、与えられた文字列を引数にリストされた値（文字列、整数、浮動小数点数など）でフォーマットし、C++ の printf 関数に似ています。フォーマット文字列には `%` 文字から始まるフォーマット指定子を含めることができます。`%` およびその後のフォーマット指定子に含まれていないものはリテラルテキストと見なされ、出力にそのままコピーされます。リテラル `%` 文字は `%%` でエスケープできます。

**構文**

``` sql
printf(format, arg1, arg2, ...)
```

**例**

クエリ:

``` sql
select printf('%%%s %s %d', 'Hello', 'World', 2024);
```

``` response
┌─printf('%%%s %s %d', 'Hello', 'World', 2024)─┐
│ %Hello World 2024                            │
└──────────────────────────────────────────────┘
```
