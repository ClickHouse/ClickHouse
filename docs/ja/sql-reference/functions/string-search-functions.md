---
slug: /ja/sql-reference/functions/string-search-functions
sidebar_position: 160
sidebar_label: 文字列の検索
---

# 文字列検索のための関数

このセクションのすべての関数はデフォルトでケースセンシティブに検索します。ケースインセンシティブな検索は通常、別の関数バリアントで提供されます。

:::note
ケースインセンシティブな検索は、英語の小文字-大文字のルールに従います。たとえば、英語では大文字の `i` は `I` ですが、トルコ語では `İ` です。したがって、英語以外の言語の結果は予期せぬものになるかもしれません。
:::

このセクションの関数は、検索される文字列（このセクションでは `haystack` と呼びます）および検索文字列（このセクションでは `needle` と呼びます）がシングルバイトでエンコードされたテキストであることを前提としています。この前提が満たされない場合、例外はスローされず、結果は未定義です。UTF-8 エンコードされた文字列での検索は通常、別の関数バリアントで提供されます。同様に、UTF-8 関数バリアントを使用していて入力文字列が UTF-8 エンコードされたテキストでない場合も、例外はスローされず、結果は未定義です。自動的な Unicode 正規化は行われませんが、[normalizeUTF8*()](https://clickhouse.com../functions/string-functions/) 関数を使用してそれを実行できます。

[一般的な文字列関数](string-functions.md)および[文字列の置換関数](string-replace-functions.md)は別途説明されています。

## position

文字列 `haystack` 内のサブ文字列 `needle` の位置（バイト単位、1から開始）を返します。

**構文**

``` sql
position(haystack, needle[, start_pos])
```

エイリアス:
- `position(needle IN haystack)`

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `start_pos` – 検索開始位置 (`haystack` の1ベースの位置)。[UInt](../data-types/int-uint.md)。オプション。

**返される値**

- サブ文字列が見つかった場合の開始位置をバイト単位で返し、1からカウントします。[UInt64](../data-types/int-uint.md)。
- サブ文字列が見つからなかった場合は0を返します。[UInt64](../data-types/int-uint.md)。

サブ文字列 `needle` が空の場合、次のルールが適用されます:
- `start_pos` が指定されていない場合：`1` を返す
- `start_pos = 0` の場合：`1` を返す
- `start_pos >= 1` かつ `start_pos <= length(haystack) + 1` の場合：`start_pos` を返す
- それ以外の場合：`0` を返す

同じルールは、関数 `locate`、`positionCaseInsensitive`、`positionUTF8`、および `positionCaseInsensitiveUTF8` にも適用されます。

**例**

クエリ:

``` sql
SELECT position('Hello, world!', '!');
```

結果:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

`start_pos` 引数を使用した例:

クエリ:

``` sql
SELECT
    position('Hello, world!', 'o', 1),
    position('Hello, world!', 'o', 7)
```

結果:

``` text
┌─position('Hello, world!', 'o', 1)─┬─position('Hello, world!', 'o', 7)─┐
│                                 5 │                                 9 │
└───────────────────────────────────┴───────────────────────────────────┘
```

`needle IN haystack` 構文の例:

クエリ:

```sql
SELECT 6 = position('/' IN s) FROM (SELECT 'Hello/World' AS s);
```

結果:

```text
┌─equals(6, position(s, '/'))─┐
│                           1 │
└─────────────────────────────┘
```

空の `needle` サブ文字列を使用した例:

クエリ:

``` sql
SELECT
    position('abc', ''),
    position('abc', '', 0),
    position('abc', '', 1),
    position('abc', '', 2),
    position('abc', '', 3),
    position('abc', '', 4),
    position('abc', '', 5)
```

結果:

``` text
┌─position('abc', '')─┬─position('abc', '', 0)─┬─position('abc', '', 1)─┬─position('abc', '', 2)─┬─position('abc', '', 3)─┬─position('abc', '', 4)─┬─position('abc', '', 5)─┐
│                   1 │                      1 │                      1 │                      2 │                      3 │                      4 │                      0 │
└─────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┘
```

## locate

[位置](#position) と同様ですが、引数 `haystack` と `needle` が入れ替わっています。

この関数の挙動は ClickHouse のバージョンに依存します:
- バージョン < v24.3 では、`locate` は関数 `position` のエイリアスであり、引数 `(haystack, needle[, start_pos])` を受け付けていました。
- バージョン >= 24.3 では、`locate` は個別の関数であり (MySQL との互換性を向上させるため)、引数 `(needle, haystack[, start_pos])` を受け付けます。以前の挙動は、設定 [function_locate_has_mysql_compatible_argument_order = false](../../operations/settings/settings.md#function-locate-has-mysql-compatible-argument-order) で復元できます。

**構文**

``` sql
locate(needle, haystack[, start_pos])
```

## positionCaseInsensitive

[位置](#position) のケースインセンシティブなバージョン。

**例**

クエリ:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello');
```

結果:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8

[位置](#position) と同様ですが、`haystack` と `needle` が UTF-8 エンコードされた文字列であることを前提とします。

**例**

関数 `positionUTF8` は、文字 `ö`（2 ポイントで表される）を正しく単一の Unicode コードポイントとしてカウントします。

クエリ:

``` sql
SELECT positionUTF8('Motörhead', 'r');
```

結果:

``` text
┌─position('Motörhead', 'r')─┐
│                          5 │
└────────────────────────────┘
```

## positionCaseInsensitiveUTF8

[位置UTF8](#positionutf8) と同様ですが、ケースインセンシティブに検索します。

## multiSearchAllPositions

[位置](#position) と同様ですが、`haystack` 文字列の複数の `needle` サブ文字列の位置をバイト単位で配列として返します。

:::note
すべての `multiSearch*()` 関数は最大 2<sup>8</sup> 個の選択肢しかサポートしていません。
:::

**構文**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needleN])
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- サブ文字列が見つかった場合の開始位置をバイト単位で配列として返し、1からカウントします。
- サブ文字列が見つからなかった場合は0を返します。

**例**

クエリ:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world']);
```

結果:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```
## multiSearchAllPositionsCaseInsensitive

[multiSearchAllPositions](#multisearchallpositions) と同様ですが、ケースを無視します。

**構文**

```sql
multiSearchAllPositionsCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- サブ文字列が見つかった場合の開始位置をバイト単位で配列として返し、1からカウントします。
- サブ文字列が見つからなかった場合は0を返します。

**例**

クエリ:

```sql
SELECT multiSearchAllPositionsCaseInsensitive('ClickHouse',['c','h']);
```

結果:

```response
["1","6"]
```

## multiSearchAllPositionsUTF8

[multiSearchAllPositions](#multisearchallpositions) と同様ですが、`haystack` と `needle` サブ文字列が UTF-8 エンコードされた文字列であることを前提とします。

**構文**

```sql
multiSearchAllPositionsUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- サブ文字列が見つかった場合の開始位置をバイト単位で配列として返し、1からカウントします。
- サブ文字列が見つからなかった場合は0を返します。

**例**

与えられた `ClickHouse` を UTF-8 文字列とし、`C` (`\x43`) と `H` (`\x48`) の位置を見つける。

クエリ:

```sql
SELECT multiSearchAllPositionsUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x43','\x48']);
```

結果:

```response
["1","6"]
```

## multiSearchAllPositionsCaseInsensitiveUTF8

[multiSearchAllPositionsUTF8](#multisearchallpositionsutf8) と同様ですが、ケースを無視します。

**構文**

```sql
multiSearchAllPositionsCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- サブ文字列が見つかった場合の開始位置をバイト単位で配列として返し、1からカウントします。
- サブ文字列が見つからなかった場合は0を返します。

**例**

与えられた `ClickHouse` を UTF-8 文字列とし、`h` (`\x68`) の文字が単語に含まれているかチェックします。

クエリ:

```sql
SELECT multiSearchAllPositionsCaseInsensitiveUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x68']);
```

結果:

```response
["1","6"]
```

## multiSearchFirstPosition

[`position`](#position) と同様ですが、`haystack` 文字列で複数の `needle` 文字列のいずれかに一致する最も左のオフセットを返します。

関数 [`multiSearchFirstPositionCaseInsensitive`](#multisearchfirstpositioncaseinsensitive)、[`multiSearchFirstPositionUTF8`](#multisearchfirstpositionutf8)、および [`multiSearchFirstPositionCaseInsensitiveUTF8`](#multisearchfirstpositioncaseinsensitiveutf8) は、この関数のケースインセンシティブおよび/または UTF-8 バリアントを提供します。

**構文**

```sql
multiSearchFirstPosition(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- `haystack` 文字列で複数の `needle` 文字列のいずれかに一致する最も左のオフセット。
- 一致がなかった場合は0。

**例**

クエリ:

```sql
SELECT multiSearchFirstPosition('Hello World',['llo', 'Wor', 'ld']);
```

結果:

```response
3
```

## multiSearchFirstPositionCaseInsensitive

[`multiSearchFirstPosition`](#multisearchfirstposition) と同様ですが、ケースを無視します。

**構文**

```sql
multiSearchFirstPositionCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- `haystack` 文字列で複数の `needle` 文字列のいずれかに一致する最も左のオフセット。
- 一致がなかった場合は0。

**例**

クエリ:

```sql
SELECT multiSearchFirstPositionCaseInsensitive('HELLO WORLD',['wor', 'ld', 'ello']);
```

結果:

```response
2
```

## multiSearchFirstPositionUTF8

[`multiSearchFirstPosition`](#multisearchfirstposition) と同様ですが、`haystack` と `needle` が UTF-8 エンコードされた文字列であることを前提とします。

**構文**

```sql
multiSearchFirstPositionUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- `haystack` 文字列で複数の `needle` 文字列のいずれかに一致する最も左のオフセット。
- 一致がなかった場合は0。

**例**

UTF-8 文字列 `hello world` において、与えられた選択肢のいずれかに一致する左端のオフセットを見つけます。

クエリ:

```sql
SELECT multiSearchFirstPositionUTF8('\x68\x65\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64',['wor', 'ld', 'ello']);
```

結果:

```response
2
```

## multiSearchFirstPositionCaseInsensitiveUTF8

[`multiSearchFirstPosition`](#multisearchfirstposition) と同様ですが、`haystack` と `needle` が UTF-8 エンコードされた文字列であり、ケースを無視します。

**構文**

```sql
multiSearchFirstPositionCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- ケースを無視し、`haystack` 文字列で複数の `needle` 文字列のいずれかに一致する最も左のオフセット。
- 一致がなかった場合は0。

**例**

UTF-8 文字列 `HELLO WORLD` において、与えられた選択肢のいずれかに一致する左端のオフセットを見つけます。

クエリ:

```sql
SELECT multiSearchFirstPositionCaseInsensitiveUTF8('\x48\x45\x4c\x4c\x4f\x20\x57\x4f\x52\x4c\x44',['wor', 'ld', 'ello']);
```

結果:

```response
2
```

## multiSearchFirstIndex

文字列 `haystack` 内で、最も左端に見つかった `needle<sub>i</sub>` のインデックス `i`（1から開始）を返し、それ以外の場合は0を返します。

関数 [`multiSearchFirstIndexCaseInsensitive`](#multisearchfirstindexcaseinsensitive)、[`multiSearchFirstIndexUTF8`](#multisearchfirstindexutf8)、および [`multiSearchFirstIndexCaseInsensitiveUTF8`](#multisearchfirstindexcaseinsensitiveutf8) は、この関数のケースインセンシティブおよび/または UTF-8 バリアントを提供します。

**構文**

```sql
multiSearchFirstIndex(haystack, [needle1, needle2, ..., needleN])
```
**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- 最も左端に見つかった選択肢のインデックス（1から開始）。一致がなかった場合は0。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT multiSearchFirstIndex('Hello World',['World','Hello']);
```

結果:

```response
1
```

## multiSearchFirstIndexCaseInsensitive

文字列 `haystack` 内で、最も左端に見つかった `needle<sub>i</sub>` のインデックス `i`（1から開始）を返し、それ以外の場合は0を返します。ケースを無視します。

**構文**

```sql
multiSearchFirstIndexCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- 最も左端に見つかった選択肢のインデックス（1から開始）。一致がなかった場合は0。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT multiSearchFirstIndexCaseInsensitive('hElLo WoRlD',['World','Hello']);
```

結果:

```response
1
```

## multiSearchFirstIndexUTF8

文字列 `haystack` 内で、最も左端に見つかった `needle<sub>i</sub>` のインデックス `i`（1から開始）を返し、それ以外の場合は0を返します。`haystack` と `needle` が UTF-8 エンコードされた文字列であることを前提とします。

**構文**

```sql
multiSearchFirstIndexUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- 最も左端に見つかった選択肢のインデックス（1から開始）。一致がなかった場合は0。[UInt8](../data-types/int-uint.md)。

**例**

UTF-8 文字列 `Hello World` を与え、UTF-8 文字列 `Hello` と `World` の最初のインデックスを見つけます。

クエリ:

```sql
SELECT multiSearchFirstIndexUTF8('\x48\x65\x6c\x6c\x6f\x20\x57\x6f\x72\x6c\x64',['\x57\x6f\x72\x6c\x64','\x48\x65\x6c\x6c\x6f']);
```

結果:

```response
1
```

## multiSearchFirstIndexCaseInsensitiveUTF8

文字列 `haystack` 内で、最も左端に見つかった `needle<sub>i</sub>` のインデックス `i`（1から開始）を返し、それ以外の場合は0を返します。`haystack` と `needle` が UTF-8 エンコードされた文字列であり、ケースを無視します。

**構文**

```sql
multiSearchFirstIndexCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- 最も左端に見つかった選択肢のインデックス（1から開始）。一致がなかった場合は0。[UInt8](../data-types/int-uint.md)。

**例**

UTF-8 文字列 `HELLO WORLD` を与え、UTF-8 文字列 `hello` と `world` の最初のインデックスを見つけます。

クエリ:

```sql
SELECT multiSearchFirstIndexCaseInsensitiveUTF8('\x48\x45\x4c\x4c\x4f\x20\x57\x4f\x52\x4c\x44',['\x68\x65\x6c\x6c\x6f','\x77\x6f\x72\x6c\x64']);
```

結果:

```response
1
```

## multiSearchAny

少なくとも1つの文字列 `needle<sub>i</sub>` が文字列 `haystack` に一致する場合は1を返し、それ以外の場合は0を返します。

関数 [`multiSearchAnyCaseInsensitive`](#multisearchanycaseinsensitive)、[`multiSearchAnyUTF8`](#multisearchanyutf8)、および [`multiSearchAnyCaseInsensitiveUTF8`](#multisearchanycaseinsensitiveutf8) は、この関数のケースインセンシティブおよび/または UTF-8 バリアントを提供します。

**構文**

```sql
multiSearchAny(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- 少なくとも1つの一致があった場合は1。
- 少なくとも1つの一致がなかった場合は0。

**例**

クエリ:

```sql
SELECT multiSearchAny('ClickHouse',['C','H']);
```

結果:

```response
1
```

## multiSearchAnyCaseInsensitive

[multiSearchAny](#multisearchany) と同様ですが、ケースを無視します。

**構文**

```sql
multiSearchAnyCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[Array](../data-types/array.md)。

**返される値**

- 少なくとも1つのケースインセンシティブな一致があった場合は1。
- 少なくとも1つのケースインセンシティブな一致がなかった場合は0。

**例**

クエリ:

```sql
SELECT multiSearchAnyCaseInsensitive('ClickHouse',['c','h']);
```

結果:

```response
1
```

## multiSearchAnyUTF8

[multiSearchAny](#multisearchany) と同様ですが、`haystack` と `needle` サブ文字列が UTF-8 エンコードされた文字列であることを前提とします。

**構文**

```sql
multiSearchAnyUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- 少なくとも1つの一致があった場合は1。
- 少なくとも1つの一致がなかった場合は0。

**例**

与えられた `ClickHouse` を UTF-8 文字列とし、`C` (`\x43`) や `H` (`\x48`) の文字が単語に含まれているかチェックします。

クエリ:

```sql
SELECT multiSearchAnyUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x43','\x48']);
```

結果:

```response
1
```

## multiSearchAnyCaseInsensitiveUTF8

[multiSearchAnyUTF8](#multisearchanyutf8) と同様ですが、ケースを無視します。

**構文**

```sql
multiSearchAnyCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**パラメータ**

- `haystack` — 検索が行われる UTF-8 エンコードされた文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される UTF-8 サブ文字列。[Array](../data-types/array.md)。

**返される値**

- 少なくとも1つのケースインセンシティブな一致があった場合は1。
- 少なくとも1つのケースインセンシティブな一致がなかった場合は0。

**例**

与えられた `ClickHouse` を UTF-8 文字列とし、`h` (`\x68`) の文字が単語に含まれているかケースを無視してチェックします。

クエリ:

```sql
SELECT multiSearchAnyCaseInsensitiveUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x68']);
```

結果:

```response
1
```

## match {#match}

文字列 `haystack` が [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax) の正規表現 `pattern` に一致するかどうかを返します。

一致は UTF-8 に基づいて行われます。たとえば、`.` は Unicode コードポイント `¥` に一致し、このコードポイントは UTF-8 では2バイトで表されます。正規表現には null バイトを含めることはできません。`haystack` または `pattern` が有効な UTF-8 でない場合、動作は未定義です。

re2 のデフォルト動作とは異なり、`.` は改行を含むすべての文字に一致します。これを無効にするには、パターンの先頭に `(?-s)` を追加してください。

文字列の部分文字列を検索したいだけなら、[like](#like) や [position](#position) 関数を使用できます。これらの関数はこの関数よりもはるかに高速に動作します。

**構文**

```sql
match(haystack, pattern)
```

エイリアス: `haystack REGEXP pattern` 演算子

## multiMatchAny

`match` と同様ですが、少なくとも1つのパターンが一致する場合は1を返し、そうでない場合は0を返します。

:::note
`multi[Fuzzy]Match*()` ファミリーの関数は、(Vectorscan)[https://github.com/VectorCamp/vectorscan] ライブラリを使用します。そのため、vectorscan をサポートするように ClickHouse がコンパイルされている場合にのみ有効になります。

すべての hyperscan を使用する関数をオフにするには、設定 `SET allow_hyperscan = 0;` を使用します。

vectorscan の制限により、`haystack` 文字列の長さは 2<sup>32</sup> バイト未満である必要があります。

Hyperscan は一般的に正規表現拒否サービス（ReDoS）攻撃に対して脆弱です（例: ここ(here)[https://www.usenix.org/conference/usenixsecurity22/presentation/turonova]やここ(here)[https://doi.org/10.1007/s10664-021-10033-1]およびここ(here)[https://doi.org/10.1145/3236024.3236027]を参照）。提供されるパターンを慎重に確認することをユーザーに推奨します。
:::

もし複数の部分文字列を文字列で検索したいだけなら、[multiSearchAny](#multisearchany) 関数を使用することができます。この関数はこの関数よりもはるかに高速に動作します。

**構文**

```sql
multiMatchAny(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiMatchAnyIndex

`multiMatchAny` と同様ですが、haystack に一致するインデックスのいずれかを返します。

**構文**

```sql
multiMatchAnyIndex(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiMatchAllIndices

`multiMatchAny` と同様ですが、任意の順序ですべてのインデックスが haystack に一致する配列を返します。

**構文**

```sql
multiMatchAllIndices(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAny

`multiMatchAny` と同様ですが、定数の[編集距離](https://en.wikipedia.org/wiki/Edit_distance)で haystack を満たすパターンがある場合に1を返します。この関数は [hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) ライブラリのエクスペリメンタルな機能に依存しており、いくつかのコーナーケースでは遅くなることがあります。パフォーマンスは編集距離の値と使用されたパターンに依存しますが、常に非ファジーバリアントよりも高いコストがかかります。

:::note
`multiFuzzyMatch*()` 関数ファミリーは UTF-8 正規表現をサポートしていません（それらをバイトのシーケンスとして処理します）これは hyperscan の制限によるものです。
:::

**構文**

```sql
multiFuzzyMatchAny(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAnyIndex

`multiFuzzyMatchAny` と同様ですが、固定の編集距離内で haystack に一致するインデックスのいずれかを返します。

**構文**

```sql
multiFuzzyMatchAnyIndex(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAllIndices

`multiFuzzyMatchAny` と同様ですが、定数の編集距離内で一致するすべてのインデックスを任意の順序で返す配列を返します。

**構文**

```sql
multiFuzzyMatchAllIndices(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## extract

文字列の正規表現に一致する最初の部分文字列を返します。
`haystack` が `pattern` 正規表現に一致しない場合、空の文字列が返されます。

正規表現にキャプチャグループがある場合、この関数は入力文字列を最初のキャプチャグループに対して一致させます。

**構文**

```sql
extract(haystack, pattern)
```

**引数**

- `haystack` — 入力文字列。[String](../data-types/string.md)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax)を使用した正規表現。

**返される値**

- haystack 文字列の正規表現の最初の一致。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT extract('number: 1, number: 2, number: 3', '\\d+') AS result;
```

結果:

```response
┌─result─┐
│ 1      │
└────────┘
```

## extractAll

文字列の正規表現のすべての一致を含む配列を返します。`haystack` が `pattern` 正規表現に一致しない場合、空の文字列が返されます。

サブパターンに関する動作は、関数[`extract`](#extract)と同じです。

**構文**

```sql
extractAll(haystack, pattern)
```

**引数**

- `haystack` — 入力文字列。[String](../data-types/string.md)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax)を使用した正規表現。

**返される値**

- haystack 文字列の正規表現の一致の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

**例**

クエリ:

```sql
SELECT extractAll('number: 1, number: 2, number: 3', '\\d+') AS result;
```

結果:

```response
┌─result────────┐
│ ['1','2','3'] │
└───────────────┘
```

## extractAllGroupsHorizontal

`haystack` 文字列のすべてのグループを正規表現 `pattern` を使用してマッチングし、すべてのグループに一致するフラグメントを配列で返します。配列の最初の要素は、最初のグループのすべての一致を含み、二つ目の配列は二番目のグループが一致したフラグメントを、それぞれのグループのフラグメントが含まれている。

この関数は [extractAllGroupsVertical](#extractallgroupsvertical) よりも遅いです。

**構文**

``` sql
extractAllGroupsHorizontal(haystack, pattern)
```

**引数**

- `haystack` — 入力文字列。[String](../data-types/string.md)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax)を使用した正規表現。括弧で囲まれたグループを含む必要があります。`pattern` にグループが含まれていない場合、例外がスローされます。[String](../data-types/string.md)。

**返される値**

- 一致の配列の配列。[Array](../data-types/array.md)。

:::note
`haystack` が `pattern` 正規表現に一致しない場合、空の配列の配列が返されます。
:::

**例**

``` sql
SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

結果:

``` text
┌─extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','def','ghi'],['111','222','333']]                                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

## extractGroups

指定された入力文字列を与えられた正規表現でマッチングし、一致する配列の配列を返します。

**構文**

``` sql
extractGroups(haystack, pattern)
```

**引数**

- `haystack` — 入力文字列。[String](../data-types/string.md)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax)を使用した正規表現。括弧で囲まれたグループを含む必要があります。`pattern` にグループが含まれていない場合、例外がスローされます。[String](../data-types/string.md)。

**返される値**

- 一致の配列の配列。[Array](../data-types/array.md)。

**例**

``` sql
SELECT extractGroups('hello abc=111 world', '("[^"]+"|\\w+)=("[^"]+"|\\w+)') AS result;
```

結果:

``` text
┌─result────────┐
│ ['abc','111'] │
└───────────────┘
```

## extractAllGroupsVertical

`haystack` 文字列のすべてのグループを正規表現 `pattern` を使用してマッチングし、配列で返します。この配列の各要素には、`haystack` に出現順が含まれている各グループに由来するフラグメントが含まれています。

**構文**

``` sql
extractAllGroupsVertical(haystack, pattern)
```

**引数**

- `haystack` — 入力文字列。[String](../data-types/string.md)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax)を使用した正規表現。括弧で囲まれたグループを含む必要があります。`pattern` にグループが含まれていない場合、例外がスローされます。[String](../data-types/string.md)。

**返される値**

- 一致の配列の配列。[Array](../data-types/array.md)。

:::note
`haystack` が `pattern` 正規表現に一致しない場合、空の配列が返されます。
:::

**例**

``` sql
SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

結果:

``` text
┌─extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','111'],['def','222'],['ghi','333']]                                            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## like

文字列 `haystack` が LIKE 式 `pattern` に一致するかどうかを返します。

LIKE 式は、通常の文字と次のメタシンボルを含むことができます。

- `%` は任意の数のおよび任意の文字を示します（0文字を含む）。
- `_` は1つの任意の文字を示します。
- `\` はリテラル `%`、`_` および `\` をエスケープします。

一致は UTF-8 に基づいて行われます。たとえば、`_` は Unicode コードポイント `¥` に一致し、このコードポイントは UTF-8 では2バイトで表されます。

`haystack` または LIKE 式が有効な UTF-8 でない場合、動作は未定義です。

自動的な Unicode 正規化は行われません、[normalizeUTF8*()](https://clickhouse.com../functions/string-functions/) 関数を使用してください。

リテラル `%`、`_` および `\`（LIKE メタ文字）に対しては、バックスラッシュ：`\%`、`\_` および `\\` を前置します。バックスラッシュは、`%`、`_` または `\` 以外の文字に前置される場合、その特別な意味を失います（つまり、文字通りに解釈されます）。クリックハウスでは、[文字列の](../syntax.md#string)バックスラッシュを引用する必要があることに注意してください、そのため実際に `\\%`、`\\_` および `\\` を書く必要があります。

LIKE 式が `%needle%` の形式の場合、関数は `position` 関数と同じスピードで動作します。他のすべての LIKE 式は、内部的に正規表現に変換され、`match` 関数と同様の速度で実行されます。

**構文**

```sql
like(haystack, pattern)
```

エイリアス: `haystack LIKE pattern`（演算子）

## notLike {#notlike}

`like` と同様ですが、結果を否定します。

エイリアス: `haystack NOT LIKE pattern`（演算子）

## ilike

`like` と同様ですが、ケースを無視します。

エイリアス: `haystack ILIKE pattern`（演算子）

## notILike

`ilike` と同様ですが、結果を否定します。

エイリアス: `haystack NOT ILIKE pattern`（演算子）

## ngramDistance

`haystack` 文字列と `needle` 文字列の 4-gram 距離を計算します。これにより、二つの4-gram マルチセットの対称差をカウントし、それをそれらの組の和で正規化します。[Float32](../data-types/float.md/#float32-float64) を返します。値は0から1までです。結果が小さければ小さいほど、文字列は互いに似ています。

関数 [`ngramDistanceCaseInsensitive`](#ngramdistancecaseinsensitive)、[`ngramDistanceUTF8`](#ngramdistanceutf8)、[`ngramDistanceCaseInsensitiveUTF8`](#ngramdistancecaseinsensitiveutf8) は、この関数のケースインセンシティブおよび/または UTF-8 バリアントを提供します。

**構文**

```sql
ngramDistance(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の比較文字列。[String literal](../syntax#string)
- `needle`: 第二の比較文字列。[String literal](../syntax#string)

**返される値**

- 二つの文字列間の類似度を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

**実装の詳細**

この関数は、定数 `needle` または `haystack` の引数が32Kbを超える場合、例外をスローします。非定数 `haystack` または `needle` の引数が32Kbを超える場合、距離は常に1になります。

**例**

二つの文字列が互いに似ていれば似ているほど、結果は0に近くなります（同一の場合）。

クエリ:

```sql
SELECT ngramDistance('ClickHouse','ClickHouse!');
```

結果:

```response
0.06666667
```

二つの文字列が互いに似ていなければ似ていないほど、結果は大きくなります。

クエリ:

```sql
SELECT ngramDistance('ClickHouse','House');
```

結果:

```response
0.5555556
```

## ngramDistanceCaseInsensitive

[ngramDistance](#ngramdistance) のケースインセンシティブバージョンを提供します。

**構文**

```sql
ngramDistanceCaseInsensitive(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の比較文字列。[String literal](../syntax#string)
- `needle`: 第二の比較文字列。[String literal](../syntax#string)

**返される値**

- 二つの文字列間の類似度を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

**例**

[ngramDistance](#ngramdistance) を使用すると、ケースの違いが類似度の値に影響します:

クエリ:

```sql
SELECT ngramDistance('ClickHouse','clickhouse');
```

結果:

```response
0.71428573
```

[ngramDistanceCaseInsensitive](#ngramdistancecaseinsensitive) を使用すると、ケースは無視されるため、ケースだけが異なる同一の文字列は低い類似度値を返します:

クエリ:

```sql
SELECT ngramDistanceCaseInsensitive('ClickHouse','clickhouse');
```

結果:

```response
0
```

## ngramDistanceUTF8

[ngramDistance](#ngramdistance) の UTF-8 バリアントを提供します。`needle` と `haystack` 文字列が UTF-8 エンコードされた文字列であることを前提としています。

**構文**

```sql
ngramDistanceUTF8(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)
- `needle`: 第二の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)

**返される値**

- 二つの文字列間の類似度を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

**例**

クエリ:

```sql
SELECT ngramDistanceUTF8('abcde','cde');
```

結果:

```response
0.5
```

## ngramDistanceCaseInsensitiveUTF8

[ngramDistanceUTF8](#ngramdistanceutf8) のケースインセンシティブバージョンを提供します。

**構文**

```sql
ngramDistanceCaseInsensitiveUTF8(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)
- `needle`: 第二の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)

**返される値**

- 二つの文字列間の類似度を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

**例**

クエリ:

```sql
SELECT ngramDistanceCaseInsensitiveUTF8('abcde','CDE');
```

結果:

```response
0.5
```

## ngramSearch

`ngramDistance` に似ていますが、`needle` 文字列と `haystack` 文字列の非対称差を計算します。すなわち、needle の n-gram の数から共通の n-gram の数を引き、`needle` n-gram の数で正規化します。[Float32](../data-types/float.md/#float32-float64) を返します。値は0から1の間です。結果が大きいほど、`needle` が `haystack` に含まれている可能性が高くなります。この関数はファジー文字列検索に便利です。また、[`soundex`](../../sql-reference/functions/string-functions#soundex) 関数も参照してください。

関数 [`ngramSearchCaseInsensitive`](#ngramsearchcaseinsensitive)、[`ngramSearchUTF8`](#ngramsearchutf8)、[`ngramSearchCaseInsensitiveUTF8`](#ngramsearchcaseinsensitiveutf8) は、この関数のケースインセンシティブおよび/または UTF-8 バリアントを提供します。

**構文**

```sql
ngramSearch(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の比較文字列。[String literal](../syntax#string)
- `needle`: 第二の比較文字列。[String literal](../syntax#string)

**返される値**

- `needle` が `haystack` に含まれている可能性を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

**実装の詳細**

:::note
UTF-8 バリアントは3-gram距離を使用します。これらは完全に公平なn-gram距離ではありません。私たちは n-gram をハッシュするために2バイト長のハッシュを使い、それからこれらのハッシュテーブル間の（非）対称差を計算します - 衝突が発生する可能性があります。UTF-8ケースインセンシティブフォーマットでは、`tolower` 関数を公正に使用していません - 各コードポイントバイトの5番目ビット（0からスタート）を、バイトが1つ以上ある場合、0から始まるビットをゼロにします - これはラテン語とほとんどすべてのキリル文字に対して機能します。
:::

**例**

クエリ:

```sql
SELECT ngramSearch('Hello World','World Hello');
```

結果:

```response
0.5
```

## ngramSearchCaseInsensitive

[ngramSearch](#ngramsearch) のケースインセンシティブバージョンを提供します。

**構文**

```sql
ngramSearchCaseInsensitive(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の比較文字列。[String literal](../syntax#string)
- `needle`: 第二の比較文字列。[String literal](../syntax#string)

**返される値**

- `needle` が `haystack` に含まれている可能性を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

The bigger the result is, the more likely `needle` is in the `haystack`.

**例**

クエリ:

```sql
SELECT ngramSearchCaseInsensitive('Hello World','hello');
```

結果:

```response
1
```

## ngramSearchUTF8

[ngramSearch](#ngramsearch) の UTF-8 バリアントを提供し、`needle` と `haystack` が UTF-8 エンコードされた文字列であることを前提としています。

**構文**

```sql
ngramSearchUTF8(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)
- `needle`: 第二の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)

**返される値**

- `needle` が `haystack` に含まれている可能性を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

The bigger the result is, the more likely `needle` is in the `haystack`.

**例**

クエリ:

```sql
SELECT ngramSearchUTF8('абвгдеёжз', 'гдеёзд');
```

結果:

```response
0.5
```

## ngramSearchCaseInsensitiveUTF8

[ngramSearchUTF8](#ngramsearchutf8) のケースインセンシティブバージョンを提供し、`needle` と `haystack` が UTF-8 エンコードされた文字列であることを前提としています。

**構文**

```sql
ngramSearchCaseInsensitiveUTF8(haystack, needle)
```

**パラメータ**

- `haystack`: 第一の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)
- `needle`: 第二の UTF-8 エンコードされた比較文字列。[String literal](../syntax#string)

**返される値**

- `needle` が `haystack` に含まれている可能性を表す値、0から1の間。[Float32](../data-types/float.md/#float32-float64)

The bigger the result is, the more likely `needle` is in the `haystack`.

**例**

クエリ:

```sql
SELECT ngramSearchCaseInsensitiveUTF8('абвГДЕёжз', 'АбвгдЕЁжз');
```

結果:

```response
0.57142854
```

## countSubstrings

サブ文字列 `needle` が文字列 `haystack` にどれほど頻繁に出現するかを返します。

関数 [`countSubstringsCaseInsensitive`](#countsubstringscaseinsensitive) および [`countSubstringsCaseInsensitiveUTF8`](#countsubstringscaseinsensitiveutf8) は、それぞれケースインセンシティブおよびケースインセンシティブ + UTF-8 バリアントを提供します。

**構文**

``` sql
countSubstrings(haystack, needle[, start_pos])
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `start_pos` – 検索開始位置 (`haystack` の1ベースの位置)。[UInt](../data-types/int-uint.md)。オプション。

**返される値**

- 出現回数。[UInt64](../data-types/int-uint.md)。

**例**

``` sql
SELECT countSubstrings('aaaa', 'aa');
```

結果:

``` text
┌─countSubstrings('aaaa', 'aa')─┐
│                             2 │
└───────────────────────────────┘
```

`start_pos` 引数を使用した例:

```sql
SELECT countSubstrings('abc___abc', 'abc', 4);
```

結果:

``` text
┌─countSubstrings('abc___abc', 'abc', 4)─┐
│                                      1 │
└────────────────────────────────────────┘
```
## countSubstringsCaseInsensitive

サブ文字列 `needle` が文字列 `haystack` にどれほど頻繁に出現するかを返します。ケースを無視します。

**構文**

``` sql
countSubstringsCaseInsensitive(haystack, needle[, start_pos])
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `start_pos` – 検索開始位置 (`haystack` の1ベースの位置)。[UInt](../data-types/int-uint.md)。オプション。

**返される値**

- 出現回数。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT countSubstringsCaseInsensitive('AAAA', 'aa');
```

結果:

``` text
┌─countSubstringsCaseInsensitive('AAAA', 'aa')─┐
│                                            2 │
└──────────────────────────────────────────────┘
```

`start_pos` 引数を使用した例:

クエリ:

```sql
SELECT countSubstringsCaseInsensitive('abc___ABC___abc', 'abc', 4);
```

結果:

``` text
┌─countSubstringsCaseInsensitive('abc___ABC___abc', 'abc', 4)─┐
│                                                           2 │
└─────────────────────────────────────────────────────────────┘
```

## countSubstringsCaseInsensitiveUTF8

サブ文字列 `needle` が文字列 `haystack` にどれほど頻繁に出現するかを返します。ケースを無視し、`haystack` が UTF-8 文字列であることを前提とします。

**構文**

``` sql
countSubstringsCaseInsensitiveUTF8(haystack, needle[, start_pos])
```

**引数**

- `haystack` — UTF-8 文字列で行われる検索。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索されるサブ文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `start_pos` – 検索開始位置 (`haystack` の1ベースの位置)。[UInt](../data-types/int-uint.md)。オプション。

**返される値**

- 出現回数。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА');
```

結果:

``` text
┌─countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА')─┐
│                                                                  4 │
└────────────────────────────────────────────────────────────────────┘
```

`start_pos` 引数を使用した例:

クエリ:

```sql
SELECT countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА', 13);
```

結果:

``` text
┌─countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА', 13)─┐
│                                                                      2 │
└────────────────────────────────────────────────────────────────────────┘
```

## countMatches

`haystack` で正規表現 `pattern` が一致する回数を返します。

**構文**

``` sql
countMatches(haystack, pattern)
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax) を使用した正規表現。[String](../data-types/string.md)。

**返される値**

- 一致の回数。[UInt64](../data-types/int-uint.md)。

**例**

``` sql
SELECT countMatches('foobar.com', 'o+');
```

結果:

``` text
┌─countMatches('foobar.com', 'o+')─┐
│                                2 │
└──────────────────────────────────┘
```

``` sql
SELECT countMatches('aaaa', 'aa');
```

結果:

``` text
┌─countMatches('aaaa', 'aa')────┐
│                             2 │
└───────────────────────────────┘
```

## countMatchesCaseInsensitive

正規表現 `pattern` が `haystack` に一致する回数を返します。大文字小文字の区別はしません。

**構文**

``` sql
countMatchesCaseInsensitive(haystack, pattern)
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `pattern` — [re2 正規表現構文](https://github.com/google/re2/wiki/Syntax) を使用した正規表現。[String](../data-types/string.md)。

**返される値**

- 一致の回数。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT countMatchesCaseInsensitive('AAAA', 'aa');
```

結果:

``` text
┌─countMatchesCaseInsensitive('AAAA', 'aa')────┐
│                                            2 │
└──────────────────────────────────────────────┘
```

## regexpExtract

`haystack` で正規表現パターンに一致する最初の文字列を抽出し、正規表現グループインデックスに対応します。

**構文**

``` sql
regexpExtract(haystack, pattern[, index])
```

エイリアス: `REGEXP_EXTRACT(haystack, pattern[, index])`。

**引数**

- `haystack` — 正規表現パターンが一致する文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `pattern` — 正規表現式の文字列、定数でなければならない。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `index` – 0以上の整数でデフォルトは1です。抽出する正規表現グループを表します。[UInt または Int](../data-types/int-uint.md)。オプション。

**返される値**

`pattern` は複数の正規表現グループを含むことができ、`index` は抽出する正規表現グループを示します。インデックスが0の場合、正規表現全体に一致します。[String](../data-types/string.md)。

**例**

``` sql
SELECT
    regexpExtract('100-200', '(\\d+)-(\\d+)', 1),
    regexpExtract('100-200', '(\\d+)-(\\d+)', 2),
    regexpExtract('100-200', '(\\d+)-(\\d+)', 0),
    regexpExtract('100-200', '(\\d+)-(\\d+)');
```

結果:

``` text
┌─regexpExtract('100-200', '(\\d+)-(\\d+)', 1)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)', 2)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)', 0)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)')─┐
│ 100                                          │ 200                                          │ 100-200                                      │ 100                                       │
└──────────────────────────────────────────────┴──────────────────────────────────────────────┴──────────────────────────────────────────────┴───────────────────────────────────────────┘
```

## hasSubsequence

`needle` が `haystack` の部分列である場合は1を、それ以外の場合は0を返します。
文字列の部分列は、与えられた文字列から0個以上の要素を削除することで導き出されるシーケンスです。

**構文**

``` sql
hasSubsequence(haystack, needle)
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される部分列。[String](../../sql-reference/syntax.md#syntax-string-literal)。

**返される値**

- 1が部分列である場合は `haystack`、そうでない場合は `needle`。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT hasSubsequence('garbage', 'arg');
```

結果:

``` text
┌─hasSubsequence('garbage', 'arg')─┐
│                                1 │
└──────────────────────────────────┘
```

## hasSubsequenceCaseInsensitive

[hasSubsequence](#hassubsequence) と同様ですが、大文字小文字の区別を無視して検索します。

**構文**

``` sql
hasSubsequenceCaseInsensitive(haystack, needle)
```

**引数**

- `haystack` — 検索が行われる文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される部分列。[String](../../sql-reference/syntax.md#syntax-string-literal)。

**返される値**

- 1が部分列であれば `haystack`、さもなくば `needle`。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT hasSubsequenceCaseInsensitive('garbage', 'ARG');
```

結果:

``` text
┌─hasSubsequenceCaseInsensitive('garbage', 'ARG')─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## hasSubsequenceUTF8

[hasSubsequence](#hassubsequence) と同様ですが、`haystack` と `needle` が UTF-8 エンコードされた文字列であることを前提とします。

**構文**

``` sql
hasSubsequenceUTF8(haystack, needle)
```

**引数**

- `haystack` — 検索が行われる文字列。UTF-8 エンコードされた [String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される部分列。UTF-8 エンコードされた [String](../../sql-reference/syntax.md#syntax-string-literal)。

**返される値**

- 1が部分列であれば `haystack`、さもなくば `needle`。[UInt8](../data-types/int-uint.md)。

クエリ:

**例**

``` sql
select hasSubsequenceUTF8('ClickHouse - столбцовая система управления базами данных', 'система');
```

結果:

``` text
┌─hasSubsequenceUTF8('ClickHouse - столбцовая система управления базами данных', 'система')─┐
│                                                                                         1 │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

## hasSubsequenceCaseInsensitiveUTF8

[hasSubsequenceUTF8](#hassubsequenceutf8) と同様ですが、大文字小文字を区別しません。

**構文**

``` sql
hasSubsequenceCaseInsensitiveUTF8(haystack, needle)
```

**引数**

- `haystack` — 検索が行われる文字列。UTF-8 エンコードされた [String](../../sql-reference/syntax.md#syntax-string-literal)。
- `needle` — 検索される部分列。UTF-8 エンコードされた [String](../../sql-reference/syntax.md#syntax-string-literal)。

**返される値**

- 1が部分列であれば `haystack`、さもなくば `needle`。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
select hasSubsequenceCaseInsensitiveUTF8('ClickHouse - столбцовая система управления базами данных', 'СИСТЕМА');
```

結果:

``` text
┌─hasSubsequenceCaseInsensitiveUTF8('ClickHouse - столбцовая система управления базами данных', 'СИСТЕМА')─┐
│                                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## hasToken

指定されたトークンが haystack に存在する場合は1を、それ以外の場合は0を返します。

**構文**

```sql
hasToken(haystack, token)
```


**パラメータ**

- `haystack`: 検索を行う文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `token`: 2つの非英数字ASCII文字（またはhaystackの境界）の間の最大長サブストリング。

**返される値**

- tokenがhaystackに存在する場合は1、そうでない場合は0。[UInt8](../data-types/int-uint.md)。

**実装の詳細**

tokenは定数文字列でなければなりません。tokenbf_v1 インデックスの特殊化によってサポートされています。

**例**

クエリ:

```sql
SELECT hasToken('Hello World','Hello');
```

```response
1
```

## hasTokenOrNull

指定されたtokenが存在する場合は1を返し、存在しない場合は0を返し、tokenが不正な形式の場合はnullを返します。

**構文**

```sql
hasTokenOrNull(haystack, token)
```

**パラメータ**

- `haystack`: 検索を行う文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `token`: 2つの非英数字ASCII文字（またはhaystackの境界）の間の最大長サブストリング。

**返される値**

- tokenがhaystackに存在する場合は1、存在しない場合は0、不正な形式のtokenの場合はnull。

**実装の詳細**

tokenは定数文字列でなければなりません。tokenbf_v1 インデックスの特殊化によってサポートされています。

**例**

`hasToken`は不正な形式のtokenに対してエラーを投げますが、`hasTokenOrNull`は不正な形式のtokenに対して`null`を返します。

クエリ:

```sql
SELECT hasTokenOrNull('Hello World','Hello,World');
```

```response
null
```

## hasTokenCaseInsensitive

指定されたtokenがhaystackに存在する場合は1を返し、そうでない場合は0を返します。大文字小文字を無視します。

**構文**

```sql
hasTokenCaseInsensitive(haystack, token)
```

**パラメータ**

- `haystack`: 検索を行う文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `token`: 2つの非英数字ASCII文字（またはhaystackの境界）の間の最大長サブストリング。

**返される値**

- tokenがhaystackに存在する場合は1、そうでない場合は0。[UInt8](../data-types/int-uint.md)。

**実装の詳細**

tokenは定数文字列でなければなりません。tokenbf_v1 インデックスの特殊化によってサポートされています。

**例**

クエリ:

```sql
SELECT hasTokenCaseInsensitive('Hello World','hello');
```

```response
1
```

## hasTokenCaseInsensitiveOrNull

指定されたtokenがhaystackに存在する場合は1を返し、そうでない場合は0を返します。大文字小文字を無視し、不正な形式のtokenの場合はnullを返します。

**構文**

```sql
hasTokenCaseInsensitiveOrNull(haystack, token)
```

**パラメータ**

- `haystack`: 検索を行う文字列。[String](../../sql-reference/syntax.md#syntax-string-literal)。
- `token`: 2つの非英数字ASCII文字（またはhaystackの境界）の間の最大長サブストリング。

**返される値**

- tokenがhaystackに存在する場合は1、存在しない場合は0、不正な形式のtokenの場合は[`null`](../data-types/nullable.md)。[UInt8](../data-types/int-uint.md)。

**実装の詳細**

tokenは定数文字列でなければなりません。tokenbf_v1 インデックスの特殊化によってサポートされています。

**例**

`hasTokenCaseInsensitive`は不正な形式のtokenに対してエラーを投げますが、`hasTokenCaseInsensitiveOrNull`は不正な形式のtokenに対して`null`を返します。

クエリ:

```sql
SELECT hasTokenCaseInsensitiveOrNull('Hello World','hello,world');
```

```response
null
```
