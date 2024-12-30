---
slug: /ja/sql-reference/functions/string-functions
sidebar_position: 170
sidebar_label: Strings
---

import VersionBadge from '@theme/badges/VersionBadge';

# 文字列を操作するための関数

文字列の[検索](string-search-functions.md)や[置換](string-replace-functions.md)の関数は別途説明されています。

## empty

入力文字列が空であるかどうかをチェックします。文字列は、たとえこのバイトがスペースやヌルバイトであっても、少なくとも1バイト含んでいる場合は空ではないと見なされます。

この関数は[配列](array-functions.md#function-empty)や[UUIDs](uuid-functions.md#empty)でも使用できます。

**構文**

``` sql
empty(x)
```

**引数**

- `x` — 入力値。[String](../data-types/string.md)。

**返される値**

- 空文字列の場合は `1`、空でない文字列の場合は `0` を返します。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT empty('');
```

結果：

```result
┌─empty('')─┐
│         1 │
└───────────┘
```

## notEmpty

入力文字列が空でないかどうかをチェックします。文字列は、たとえこのバイトがスペースやヌルバイトであっても、少なくとも1バイト含んでいる場合は空ではないと見なされます。

この関数は[配列](array-functions.md#function-notempty)や[UUIDs](uuid-functions.md#notempty)でも使用できます。

**構文**

``` sql
notEmpty(x)
```

**引数**

- `x` — 入力値。[String](../data-types/string.md)。

**返される値**

- 空でない文字列の場合は `1`、空文字列の場合は `0` を返します。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT notEmpty('text');
```

結果：

```result
┌─notEmpty('text')─┐
│                1 │
└──────────────────┘
```

## length

文字列の長さを文字やUnicodeコードポイントではなくバイトで返します。この関数は配列にも使用できます。

別名: `OCTET_LENGTH`

**構文**

```sql
length(s)
```

**パラメータ**

- `s` — 入力文字列または配列。[String](../data-types/string)/[Array](../data-types/array).

**返される値**

- 文字列または配列 `s` の長さをバイトで返します。[UInt64](../data-types/int-uint).

**例**

クエリ：

```sql
SELECT length('Hello, world!');
```

結果:

```response
┌─length('Hello, world!')─┐
│                      13 │
└─────────────────────────┘
```

クエリ：

```sql
SELECT length([1, 2, 3, 4]);
```

結果:

```response
┌─length([1, 2, 3, 4])─┐
│                    4 │
└──────────────────────┘
```

## lengthUTF8

文字列の長さをUnicodeコードポイントで返します。UTF-8エンコードされたテキストであることを前提にしています。この前提が誤っている場合、例外はスローされず、結果は未定義です。

別名:
- `CHAR_LENGTH`
- `CHARACTER_LENGTH`

**構文**

```sql
lengthUTF8(s)
```

**パラメータ**

- `s` — 有効なUTF-8エンコードされたテキストを含む文字列。[String](../data-types/string).

**返される値**

- 文字列 `s` の長さをUnicodeコードポイントで返します。[UInt64](../data-types/int-uint.md).

**例**

クエリ：

```sql
SELECT lengthUTF8('Здравствуй, мир!');
```

結果:

```response
┌─lengthUTF8('Здравствуй, мир!')─┐
│                             16 │
└────────────────────────────────┘
```

## left

左から指定された `offset` の位置から始まる文字列 `s` の部分文字列を返します。

**構文**

``` sql
left(s, offset)
```

**パラメータ**

- `s` — 部分文字列を計算する文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md).
- `offset` — オフセットのバイト数。[UInt*](../data-types/int-uint).

**返される値**

- 正の `offset` の場合：文字列の左から始まる、`offset` バイト数の部分文字列。
- 負の `offset` の場合：文字列の左から始まる、`length(s) - |offset|` バイト数の部分文字列。
- `length` が0の場合は空文字列。

**例**

クエリ：

```sql
SELECT left('Hello', 3);
```

結果:

```response
Hel
```

クエリ：

```sql
SELECT left('Hello', -3);
```

結果:

```response
He
```

## leftUTF8

左から指定された `offset` の位置から始まるUTF-8エンコードされた文字列 `s` の部分文字列を返します。

**構文**

``` sql
leftUTF8(s, offset)
```

**パラメータ**

- `s` — 部分文字列を計算するUTF-8エンコードされた文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md).
- `offset` — オフセットのバイト数。[UInt*](../data-types/int-uint).

**返される値**

- 正の `offset` の場合：文字列の左から始まる、`offset` バイト数の部分文字列。
- 負の `offset` の場合：文字列の左から始まる、`length(s) - |offset|` バイト数の部分文字列。
- `length` が0の場合は空文字列。

**例**

クエリ：

```sql
SELECT leftUTF8('Привет', 4);
```

結果:

```response
Прив
```

クエリ：

```sql
SELECT leftUTF8('Привет', -4);
```

結果:

```response
Пр
```

## leftPad

文字列を左からスペースまたは指定された文字列（必要であれば複数回）で、結果の文字列が指定された `length` に達するまで埋めます。

**構文**

``` sql
leftPad(string, length[, pad_string])
```

別名: `LPAD`

**引数**

- `string` — 埋めるべき入力文字列。[String](../data-types/string.md).
- `length` — 結果の文字列の長さ。[UInt または Int](../data-types/int-uint.md)。入力文字列の長さよりも小さい場合、入力文字列は `length` 文字に短縮されます。
- `pad_string` — 入力文字列を埋めるための文字列。[String](../data-types/string.md). オプション。指定しない場合、入力文字列はスペースで埋められます。

**返される値**

- 指定された長さの左寄せされた文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT leftPad('abc', 7, '*'), leftPad('def', 7);
```

結果:

```result
┌─leftPad('abc', 7, '*')─┬─leftPad('def', 7)─┐
│ ****abc                │     def           │
└────────────────────────┴───────────────────┘
```

## leftPadUTF8

UTF-8文字列を左からスペースまたは指定した文字列で埋めます（必要であれば複数回）、結果の文字列が指定した長さに達するまで。文字列の長さはバイトではなくコードポイントで測定されます。

**構文**

``` sql
leftPadUTF8(string, length[, pad_string])
```

**引数**

- `string` — 埋めるべき入力文字列。[String](../data-types/string.md).
- `length` — 結果の文字列の長さ。[UInt または Int](../data-types/int-uint.md)。入力文字列の長さよりも小さい場合、入力文字列は `length` 文字に短縮されます。
- `pad_string` — 入力文字列を埋めるための文字列。[String](../data-types/string.md). オプション。指定しない場合、入力文字列はスペースで埋められます。

**返される値**

- 指定された長さの左寄せされた文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT leftPadUTF8('абвг', 7, '*'), leftPadUTF8('дежз', 7);
```

結果:

```result
┌─leftPadUTF8('абвг', 7, '*')─┬─leftPadUTF8('дежз', 7)─┐
│ ***абвг                     │    дежз                │
└─────────────────────────────┴────────────────────────┘
```

## right

右から指定された `offset` の位置から始まる文字列 `s` の部分文字列を返します。

**構文**

``` sql
right(s, offset)
```

**パラメータ**

- `s` — 部分文字列を計算する文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md).
- `offset` — オフセットのバイト数。[UInt*](../data-types/int-uint).

**返される値**

- 正の `offset` の場合：文字列の右から始まる、`offset` バイト数の部分文字列。
- 負の `offset` の場合：文字列の右から始まる、`length(s) - |offset|` バイト数の部分文字列。
- `length` が0の場合は空文字列。

**例**

クエリ：

```sql
SELECT right('Hello', 3);
```

結果:

```response
llo
```

クエリ：

```sql
SELECT right('Hello', -3);
```

結果:

```response
lo
```

## rightUTF8

右から指定された `offset` の位置から始まるUTF-8エンコードされた文字列 `s` の部分文字列を返します。

**構文**

``` sql
rightUTF8(s, offset)
```

**パラメータ**

- `s` — 部分文字列を計算するUTF-8エンコードされた文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md).
- `offset` — オフセットのバイト数。[UInt*](../data-types/int-uint).

**返される値**

- 正の `offset` の場合：文字列の右から始まる、`offset` バイト数の部分文字列。
- 負の `offset` の場合：文字列の右から始まる、`length(s) - |offset|` バイト数の部分文字列。
- `length` が0の場合は空文字列。

**例**

クエリ：

```sql
SELECT rightUTF8('Привет', 4);
```

結果:

```response
ивет
```

クエリ：

```sql
SELECT rightUTF8('Привет', -4);
```

結果:

```response
ет
```

## rightPad

文字列を右からスペースまたは指定された文字列（必要であれば複数回）で、結果の文字列が指定された `length` に達するまで埋めます。

**構文**

``` sql
rightPad(string, length[, pad_string])
```

別名: `RPAD`

**引数**

- `string` — 埋めるべき入力文字列。[String](../data-types/string.md).
- `length` — 結果の文字列の長さ。[UInt または Int](../data-types/int-uint.md)。入力文字列の長さよりも小さい場合、入力文字列は `length` 文字に短縮されます。
- `pad_string` — 入力文字列を埋めるための文字列。[String](../data-types/string.md). オプション。指定しない場合、入力文字列はスペースで埋められます。

**返される値**

- 指定された長さの右寄せされた文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT rightPad('abc', 7, '*'), rightPad('abc', 7);
```

結果:

```result
┌─rightPad('abc', 7, '*')─┬─rightPad('abc', 7)─┐
│ abc****                 │ abc                │
└─────────────────────────┴────────────────────┘
```

## rightPadUTF8

UTF-8文字列を右からスペースまたは指定した文字列で埋めます（必要であれば複数回）、結果の文字列が指定した長さに達するまで。文字列の長さはバイトではなくコードポイントで測定されます。

**構文**

``` sql
rightPadUTF8(string, length[, pad_string])
```

**引数**

- `string` — 埋めるべき入力文字列。[String](../data-types/string.md).
- `length` — 結果の文字列の長さ。[UInt または Int](../data-types/int-uint.md)。入力文字列の長さよりも小さい場合、入力文字列は `length` 文字に短縮されます。
- `pad_string` — 入力文字列を埋めるための文字列。[String](../data-types/string.md). オプション。指定しない場合、入力文字列はスペースで埋められます。

**返される値**

- 指定され長さの絵文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT rightPadUTF8('абвг', 7, '*'), rightPadUTF8('абвг', 7);
```

結果:

```result
┌─rightPadUTF8('абвг', 7, '*')─┬─rightPadUTF8('абвг', 7)─┐
│ абвг***                      │ абвг                    │
└──────────────────────────────┴─────────────────────────┘
```

## lower

文字列内のASCIIラテンシンボルを小文字に変換します。

**構文**

``` sql
lower(input)
```

別名: `lcase`

**パラメータ**

- `input`: 文字列のタイプ [String](../data-types/string.md)。

**返される値**

- [String](../data-types/string.md) データタイプの値。

**例**

クエリ：

```sql
SELECT lower('CLICKHOUSE');
```

```response
┌─lower('CLICKHOUSE')─┐
│ clickhouse          │
└─────────────────────┘
```

## upper

文字列内のASCIIラテンシンボルを大文字に変換します。

**構文**

``` sql
upper(input)
```

別名: `ucase`

**パラメータ**

- `input` — 文字列のタイプ [String](../data-types/string.md)。

**返される値**

- [String](../data-types/string.md) データタイプの値。

**例**

クエリ：

``` sql
SELECT upper('clickhouse');
```

``` response
┌─upper('clickhouse')─┐
│ CLICKHOUSE          │
└─────────────────────┘
```

## lowerUTF8

文字列を小文字に変換します。文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

:::note
言語を検出しません。たとえば、トルコ語の場合、結果は完全に正確ではない可能性があります (i/İ vs. i/I)。あるコードポイントのUTF-8バイトシーケンスの長さが大文字と小文字で異なる場合（例： `ẞ` と `ß`）、このコードポイントに対する結果は正しくない可能性があります。
:::

**構文**

```sql
lowerUTF8(input)
```

**パラメータ**

- `input` — 文字列のタイプ [String](../data-types/string.md)。

**返される値**

- [String](../data-types/string.md) データタイプの値。

**例**

クエリ：

``` sql
SELECT lowerUTF8('MÜNCHEN') as Lowerutf8;
```

結果:

``` response
┌─Lowerutf8─┐
│ münchen   │
└───────────┘
```

## upperUTF8

文字列を大文字に変換します。文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

:::note
言語を検出しません。たとえば、トルコ語の場合、結果は完全に正確ではない可能性があります (i/İ vs. i/I)。あるコードポイントのUTF-8バイトシーケンスの長さが大文字と小文字で異なる場合（例： `ẞ` と `ß`）、このコードポイントに対する結果は正しくない可能性があります。
:::

**構文**

```sql
upperUTF8(input)
```

**パラメータ**

- `input` — 文字列のタイプ [String](../data-types/string.md)。

**返される値**

- [String](../data-types/string.md) データタイプの値。

**例**

クエリ：

``` sql
SELECT upperUTF8('München') as Upperutf8;
```

結果:

``` response
┌─Upperutf8─┐
│ MÜNCHEN   │
└───────────┘
```

## isValidUTF8

バイトセットが有効なUTF-8でエンコードされたテキストを構成している場合に1を返し、そうでない場合は0を返します。

**構文**

``` sql
isValidUTF8(input)
```

**パラメータ**

- `input` — 文字列のタイプ [String](../data-types/string.md)。

**返される値**

- 有効なUTF-8でエンコードされたテキストを構成している場合は `1` を、それ以外の場合は `0` を返します。

クエリ：

``` sql
SELECT isValidUTF8('\xc3\xb1') AS valid, isValidUTF8('\xc3\x28') AS invalid;
```

結果:

``` response
┌─valid─┬─invalid─┐
│     1 │       0 │
└───────┴─────────┘
```

## toValidUTF8

無効なUTF-8文字を `�` (U+FFFD) 文字に置き換えます。連続する無効な文字は、1つの置換文字にまとめられます。

**構文**

``` sql
toValidUTF8(input_string)
```

**引数**

- `input_string` — バイトセットとして表現された[String](../data-types/string.md)データタイプのオブジェクト。

**返される値**

- 有効なUTF-8文字列。

**例**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b');
```

```result
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## repeat

指定された回数だけ文字列を自分自身と連結します。

**構文**

``` sql
repeat(s, n)
```

別名: `REPEAT`

**引数**

- `s` — 繰り返す文字列。[String](../data-types/string.md)。
- `n` — 文字列を繰り返す回数。[UInt* または Int*](../data-types/int-uint.md) 。

**返される値**

`n` 回繰り返された文字列 `s` を含む文字列。`n` ≤ 0の場合、関数は空文字列を返します。[String](../data-types/string.md)。

**例**

``` sql
SELECT repeat('abc', 10);
```

結果:

```result
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## space

スペース（` `）を指定された回数だけ自分自身と連結します。

**構文**

``` sql
space(n)
```

別名: `SPACE`.

**引数**

- `n` — スペースを繰り返す回数。[UInt* または Int*](../data-types/int-uint.md)。

**返される値**

`n` 回繰り返された文字列 ` ` を含む文字列。`n` ≤ 0の場合、関数は空文字列を返します。[String](../data-types/string.md)。

**例**

クエリ：

``` sql
SELECT space(3);
```

結果:

```text
┌─space(3) ────┐
│              │
└──────────────┘
```

## reverse

文字列内のバイトの順序を反転します。

## reverseUTF8

文字列内のUnicodeコードポイントの順序を反転します。文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

## concat

指定された引数を連結します。

**構文**

``` sql
concat(s1, s2, ...)
```

**引数**

任意の型の値。

[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md) でない引数は、デフォルトのシリアライゼーションを使用して文字列に変換されます。これはパフォーマンスを低下させるため、非String/FixedString引数の使用は推奨されません。

**返される値**

引数を連結して作成された文字列。

引数のいずれかが `NULL` の場合、関数は `NULL` を返します。

**例**

クエリ：

``` sql
SELECT concat('Hello, ', 'World!');
```

結果:

```result
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

クエリ：

```sql
SELECT concat(42, 144);
```

結果:

```result
┌─concat(42, 144)─┐
│ 42144           │
└─────────────────┘
```

## concatAssumeInjective

[concat](#concat) のようですが、`concat(s1, s2, ...) → sn` が単射であると仮定しています。GROUP BYの最適化に使用できます。

ある関数が単射と呼ばれるのは、それが異なる引数に対して異なる結果を返す場合です。別の言い方をすると：異なる引数が同一の結果を決して生成しないことです。

**構文**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**引数**

StringまたはFixedString型の値。

**返される値**

引数を連結して作成された文字列。

引数のいずれかの値が `NULL` の場合、関数は `NULL` を返します。

**例**

入力テーブル：

``` sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

```result
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

``` sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2);
```

結果:

```result
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## concatWithSeparator

指定された区切り文字で、与えられた文字列を連結します。

**構文**

``` sql
concatWithSeparator(sep, expr1, expr2, expr3...)
```

別名: `concat_ws`

**引数**

- sep — 区切り文字。定数[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- exprN — 連結の対象となる式。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md) 型ではない引数は、デフォルトのシリアライゼーションを使用して文字列に変換されます。これはパフォーマンスを低下させるため、非String/FixedString引数の使用は推奨されません。

**返される値**

引数を連結して作成された文字列。

引数のいずれかの値が `NULL` の場合、関数は `NULL` を返します。

**例**

``` sql
SELECT concatWithSeparator('a', '1', '2', '3', '4')
```

結果:

```result
┌─concatWithSeparator('a', '1', '2', '3', '4')─┐
│ 1a2a3a4                                      │
└──────────────────────────────────────────────┘
```

## concatWithSeparatorAssumeInjective

`concatWithSeparator` のようですが、`concatWithSeparator(sep, expr1, expr2, expr3...) → result` が単射であると仮定しています。GROUP BYの最適化に使用できます。

ある関数が単射と呼ばれるのは、それが異なる引数に対して異なる結果を返す場合です。別の言い方をすると：異なる引数が同一の結果を決して生成しないことです。

## substring

指定されたバイトインデックス `offset` から始まる文字列 `s` の部分文字列を返します。バイトカウントは1から始まります。`offset` が0の場合、空文字列を返します。`offset` が負の場合、文字列の始めからではなく、文字列の終わりから `pos` 文字から始まります。オプションの引数 `length` は、返される部分文字列の最大バイト数を指定します。

**構文**

```sql
substring(s, offset[, length])
```

別名:
- `substr`
- `mid`
- `byteSlice`

**引数**

- `s` — 部分文字列を計算する文字列。[String](../data-types/string.md), [FixedString](../data-types/fixedstring.md) または [Enum](../data-types/enum.md)
- `offset` — 部分文字列の `s` 内の開始位置。[(U)Int*](../data-types/int-uint.md)。
- `length` — 部分文字列の最大長。[(U)Int*](../data-types/int-uint.md)。オプション。

**返される値**

`offset` の位置から始まる `length` バイトの部分文字列。 [String](../data-types/string.md)。

**例**

``` sql
SELECT 'database' AS db, substr(db, 5), substr(db, 5, 1)
```

結果:

```result
┌─db───────┬─substring('database', 5)─┬─substring('database', 5, 1)─┐
│ database │ base                     │ b                           │
└──────────┴──────────────────────────┴─────────────────────────────┘
```

## substringUTF8

指定されたバイトインデックス `offset` に基づいてUnicodeコードポイントの文字列 `s` の部分文字列を返します。バイトカウントは `1` から始まります。`offset` が `0` の場合、空文字列を返します。`offset` が負の場合、文字列の始めからではなく、文字列の終わりから `pos` 文字から始まります。オプションの引数 `length` は、返される部分文字列の最大バイト数を指定します。

文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

**構文**

```sql
substringUTF8(s, offset[, length])
```

**引数**

- `s` — 部分文字列を計算する文字列。[String](../data-types/string.md), [FixedString](../data-types/fixedstring.md) または [Enum](../data-types/enum.md)
- `offset` — 部分文字列の `s` 内の開始位置。[(U)Int*](../data-types/int-uint.md)。
- `length` — 部分文字列の最大長。[(U)Int*](../data-types/int-uint.md)。オプション。

**返される値**

`offset` の位置から始まる `length` バイトの部分文字列。

**実装の詳細**

文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

**例**

```sql
SELECT 'Täglich grüßt das Murmeltier.' AS str,
       substringUTF8(str, 9),
       substringUTF8(str, 9, 5)
```

```response
Täglich grüßt das Murmeltier.	grüßt das Murmeltier.	grüßt
```

## substringIndex

SparkまたはMySQLのように、`delim` の `count` 回の出現の前の `s` の部分文字列を返します。

**構文**

```sql
substringIndex(s, delim, count)
```
別名: `SUBSTRING_INDEX`

**引数**

- s — 部分文字列を抽出する文字列。[String](../data-types/string.md)。
- delim — 分割する文字。[String](../data-types/string.md)。
- count — 部分文字列を抽出する前にカウントするデリミタの出現回数。count が正の場合、（左からカウントして）最後のデリミタの左側のすべてが返されます。count が負の場合、（右からカウントして）最後のデリミタの右側のすべてが返されます。[UInt または Int](../data-types/int-uint.md)

**例**

``` sql
SELECT substringIndex('www.clickhouse.com', '.', 2)
```

結果:
```
┌─substringIndex('www.clickhouse.com', '.', 2)─┐
│ www.clickhouse                               │
└──────────────────────────────────────────────┘
```

## substringIndexUTF8

Unicodeコードポイント専用に、`delim` の `count` 回の出現の前の `s` の部分文字列を返します。

文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

**構文**

```sql
substringIndexUTF8(s, delim, count)
```

**引数**

- `s` — 部分文字列を抽出する文字列。[String](../data-types/string.md)。
- `delim` — 分割する文字。[String](../data-types/string.md)。
- `count` — 部分文字列を抽出する前にカウントするデリミタの出現回数。count が正の場合、（左からカウントして）最後のデリミタの左側のすべてが返されます。count が負の場合、（右からカウントして）最後のデリミタの右側のすべてが返されます。[UInt または Int](../data-types/int-uint.md)

**返される値**

`delim` の `count` 回の出現の前の `s` の部分文字列 [String](../data-types/string.md)。

**実装の詳細**

文字列が有効なUTF-8エンコードされたテキストを含むと仮定しています。この仮定が誤っている場合、例外はスローされず、結果は未定義です。

**例**

```sql
SELECT substringIndexUTF8('www.straßen-in-europa.de', '.', 2)
```

```response
www.straßen-in-europa
```

## appendTrailingCharIfAbsent

文字列 `s` が空でなく、文字 `c` で終わらない場合、文字 `c` を末尾に追加します。

**構文**

```sql
appendTrailingCharIfAbsent(s, c)
```

## convertCharset

エンコーディング `from` から `to` に変換された文字列 `s` を返します。

**構文**

```sql
convertCharset(s, from, to)
```

## base58Encode

"Bitcoin" アルファベットで [Base58](https://datatracker.ietf.org/doc/html/draft-msporny-base58) を使用して文字列をエンコードします。

**構文**

```sql
base58Encode(plaintext)
```

**引数**

- `plaintext` — [String](../data-types/string.md) カラムまたは定数。

**返される値**

- 引数のエンコードされた値を含む文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**例**

``` sql
SELECT base58Encode('Encoded');
```

結果:

```result
┌─base58Encode('Encoded')─┐
│ 3dc8KtHrwM              │
└─────────────────────────┘
```

## base58Decode

"Bitcoin" アルファベットを使用して [Base58](https://datatracker.ietf.org/doc/html/draft-msporny-base58) エンコーディングスキームを使用して文字列をデコードします。

**構文**

```sql
base58Decode(encoded)
```

**引数**

- `encoded` — [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。文字列が有効なBase58でエンコードされた値でない場合、例外がスローされます。

**返される値**

- 引数のデコードされた値を含む文字列。[String](../data-types/string.md)。

**例**

``` sql
SELECT base58Decode('3dc8KtHrwM');
```

結果:

```result
┌─base58Decode('3dc8KtHrwM')─┐
│ Encoded                    │
└────────────────────────────┘
```

## tryBase58Decode

`base58Decode` のようですが、エラーの場合は空の文字列を返します。

**構文**

```sql
tryBase58Decode(encoded)
```

**パラメータ**

- `encoded`: [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。文字列が有効なBase58でエンコードされた値でない場合、エラー時に空の文字列を返します。

**返される値**

- 引数のデコードされた値を含む文字列。

**例**

クエリ：

```sql
SELECT tryBase58Decode('3dc8KtHrwM') as res, tryBase58Decode('invalid') as res_invalid;
```

```response
┌─res─────┬─res_invalid─┐
│ Encoded │             │
└─────────┴─────────────┘
```

## base64Encode

[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md) を[RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-4) に従ってbase64でエンコードします。

別名: `TO_BASE64`.

**構文**

```sql
base64Encode(plaintext)
```

**引数**

- `plaintext` — [String](../data-types/string.md) カラムまたは定数。

**返される値**

- 引数のエンコードされた値を含む文字列。

**例**

``` sql
SELECT base64Encode('clickhouse');
```

結果:

```result
┌─base64Encode('clickhouse')─┐
│ Y2xpY2tob3VzZQ==           │
└────────────────────────────┘
```

## base64URLEncode

[RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-5)に従って、URL (String または FixedString) をURL専用の変更でbase64でエンコードします。

**構文**

```sql
base64URLEncode(url)
```

**引数**

- `url` — [String](../data-types/string.md) カラムまたは定数。

**返される値**

- 引数のエンコードされた値を含む文字列。

**例**

``` sql
SELECT base64URLEncode('https://clickhouse.com');
```

結果:

```result
┌─base64URLEncode('https://clickhouse.com')─┐
│ aHR0cDovL2NsaWNraG91c2UuY29t              │
└───────────────────────────────────────────┘
```

## base64Decode

[RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-4)に従い、Stringをbase64からデコードします。エラーの場合は例外がスローされます。

別名: `FROM_BASE64`.

**構文**

```sql
base64Decode(encoded)
```

**引数**

- `encoded` — [String](../data-types/string.md) カラムまたは定数。文字列が有効なBase64でエンコードされた値でない場合、例外がスローされます。

**返される値**

- 引数のデコードされた値を含む文字列。

**例**

``` sql
SELECT base64Decode('Y2xpY2tob3VzZQ==');
```

結果:

```result
┌─base64Decode('Y2xpY2tob3VzZQ==')─┐
│ clickhouse                       │
└──────────────────────────────────┘
```

## base64URLDecode

[RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-5)に従い、URL専用の変更を伴うbase64からURLをデコードします。エラーの場合は例外がスローされます。

**構文**

```sql
base64URLDecode(encodedUrl)
```

**引数**

- `encodedURL` — [String](../data-types/string.md) カラムまたは定数。文字列が有効なBase64でエンコードされた、URL専用の変更を伴う値でない場合、例外がスローされます。

**返される値**

- 引数のデコードされた値を含む文字列。

**例**

``` sql
SELECT base64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t');
```

結果:

```result
┌─base64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t')─┐
│ https://clickhouse.com                          │
└─────────────────────────────────────────────────┘
```

## tryBase64Decode

`base64Decode` と似ていますが、エラーの場合は空の文字列を返します。

**構文**

```sql
tryBase64Decode(encoded)
```

**引数**

- `encoded` — [String](../data-types/string.md) カラムまたは定数。文字列が有効なBase64でエンコードされた値でない場合、空の文字列を返します。

**返される値**

- 引数のデコードされた値を含む文字列。

**例**

クエリ：

```sql
SELECT tryBase64Decode('RW5jb2RlZA==') as res, tryBase64Decode('invalid') as res_invalid;
```

```response
┌─res────────┬─res_invalid─┐
│ clickhouse │             │
└────────────┴─────────────┘
```

## tryBase64URLDecode

`base64URLDecode` と似ていますが、エラーの場合は空の文字列を返します。

**構文**

```sql
tryBase64URLDecode(encodedUrl)
```

**パラメータ**

- `encodedURL` — [String](../data-types/string.md) カラムまたは定数。文字列が有効なBase64でエンコードされた、URL専用の変更を伴う値でない場合、空の文字列を返します。

**返される値**

- 引数のデコードされた値を含む文字列。

**例**

クエリ：

```sql
SELECT tryBase64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t') as res, tryBase64Decode('aHR0cHM6Ly9jbGlja') as res_invalid;
```

```response
┌─res────────────────────┬─res_invalid─┐
│ https://clickhouse.com │             │
└────────────────────────┴─────────────┘
```

## endsWith {#endswith}

文字列 `str` が `suffix` で終わるかどうかを返します。

**構文**

```sql
endsWith(str, suffix)
```

## endsWithUTF8

文字列 `str` が `suffix` で終わるかを返します。`endsWithUTF8` と `endsWith` の違いは、`endsWithUTF8` が `str` と `suffix` をUTF-8文字で一致させることです。

**構文**

```sql
endsWithUTF8(str, suffix)
```

**例**

``` sql
SELECT endsWithUTF8('中国', '\xbd'), endsWith('中国', '\xbd')
```

結果:

```result
┌─endsWithUTF8('中国', '½')─┬─endsWith('中国', '½')─┐
│                        0 │                    1 │
└──────────────────────────┴──────────────────────┘
```

## startsWith {#startswith}

文字列 `str` が `prefix` で始まるかどうかを返します。

**構文**

```sql
startsWith(str, prefix)
```

**例**

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

## startsWithUTF8

<VersionBadge minVersion='23.8' />

文字列 `str` が `prefix` で始まるかどうかを返します。`startsWithUTF8` と `startsWith` の違いは、`startsWithUTF8` が `str` と `suffix` をUTF-8文字で一致させることです。

**例**

``` sql
SELECT startsWithUTF8('中国', '\xe4'), startsWith('中国', '\xe4')
```

結果:

```result
┌─startsWithUTF8('中国', '⥩─┬─startsWith('中国', '⥩─┐
│                          0 │                      1 │
└────────────────────────────┴────────────────────────┘
```

## trim

文字列の始めまたは終わりから指定された文字を削除します。特に指定がない限り、関数はホワイトスペース（ASCII文字32）を削除します。

**構文**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**引数**

- `trim_character` — トリムするために指定された文字。[String](../data-types/string.md).
- `input_string` — トリムする文字列。[String](../data-types/string.md).

**返される値**

先頭および/または末尾に指定された文字がない文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )');
```

結果:

```result
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft

文字列の始めから連続するホワイトスペース (ASCII文字32) を削除します。

**構文**

``` sql
trimLeft(input_string)
```

別名: `ltrim(input_string)`.

**引数**

- `input_string` — トリムする文字列。[String](../data-types/string.md).

**返される値**

先頭の一般的なホワイトスペースがない文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT trimLeft('     Hello, world!     ');
```

結果:

```result
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight

文字列の終わりから連続するホワイトスペース (ASCII文字32) を削除します。

**構文**

``` sql
trimRight(input_string)
```

別名: `rtrim(input_string)`.

**引数**

- `input_string` — トリムする文字列。[String](../data-types/string.md).

**返される値**

末尾の一般的なホワイトスペースがない文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT trimRight('     Hello, world!     ');
```

結果:

```result
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth

文字列の両端から連続するホワイトスペース (ASCII文字32) を削除します。

**構文**

``` sql
trimBoth(input_string)
```

別名: `trim(input_string)`.

**引数**

- `input_string` — トリムする文字列。[String](../data-types/string.md).

**返される値**

先頭および末尾の一般的なホワイトスペースがない文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT trimBoth('     Hello, world!     ');
```

結果:

```result
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32

CRC-32-IEEE 802.3 の多項式と初期値 `0xffffffff`（zlibの実装）を使用して文字列のCRC32チェックサムを返します。

結果の型は UInt32 です。

## CRC32IEEE

CRC-32-IEEE 802.3 の多項式を使用して文字列のCRC32チェックサムを返します。

結果の型は UInt32 です。

## CRC64

CRC-64-ECMA の多項式を使用して文字列のCRC64チェックサムを返します。

結果の型は UInt64 です。

## normalizeQuery

リテラル、リテラルの連続、複雑なエイリアス（ホワイトスペースを含むもの、2桁以上または36バイト以上の長さのものであるUUIDなど）をプレースホルダー `?` に置き換えます。

**構文**

``` sql
normalizeQuery(x)
```

**引数**

- `x` — 文字のシーケンス。[String](../data-types/string.md).

**返される値**

- プレースホルダーを持つ文字のシーケンス。[String](../data-types/string.md).

**例**

クエリ：

``` sql
SELECT normalizeQuery('[1, 2, 3, x]') AS query;
```

結果:

```result
┌─query────┐
│ [?.., x] │
└──────────┘
```

## normalizeQueryKeepNames

リテラル、リテラルの連続をプレースホルダー `?` に置き換えますが、複雑なエイリアス（ホワイトスペースを含むもの、2桁以上または36バイト以上のもの、例えばUUID）は置き換えません。これにより、複雑なクエリログをより良く分析できます。

**構文**

``` sql
normalizeQueryKeepNames(x)
```

**引数**

- `x` — 文字のシーケンス。[String](../data-types/string.md).

**返される値**

- プレースホルダーを持つ文字のシーケンス。[String](../data-types/string.md).

**例**

クエリ：

``` sql
SELECT normalizeQuery('SELECT 1 AS aComplexName123'), normalizeQueryKeepNames('SELECT 1 AS aComplexName123');
```

結果:

```result
┌─normalizeQuery('SELECT 1 AS aComplexName123')─┬─normalizeQueryKeepNames('SELECT 1 AS aComplexName123')─┐
│ SELECT ? AS `?`                               │ SELECT ? AS aComplexName123                            │
└───────────────────────────────────────────────┴────────────────────────────────────────────────────────┘
```

## normalizedQueryHash

リテラルの値を持たない、類似したクエリの同じ64ビットのハッシュ値を返します。クエリログを分析するのに役立ちます。

**構文**

``` sql
normalizedQueryHash(x)
```

**引数**

- `x` — 文字のシーケンス。[String](../data-types/string.md).

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md#uint-ranges).

**例**

クエリ：

``` sql
SELECT normalizedQueryHash('SELECT 1 AS `xyz`') != normalizedQueryHash('SELECT 1 AS `abc`') AS res;
```

結果:

```result
┌─res─┐
│   1 │
└─────┘
```

## normalizedQueryHashKeepNames

[normalizedQueryHash](#normalizedqueryhash) のようですが、複雑なエイリアス（ホワイトスペースを含む、2桁以上あるいは36バイト以上のもの、例えばUUID）を置き換えずに同じ64ビットのハッシュ値を返します。クエリログを分析するのに役立ちます。

**構文**

``` sql
normalizedQueryHashKeepNames(x)
```

**引数**

- `x` — 文字のシーケンス。[String](../data-types/string.md).

**返される値**

- ハッシュ値。[UInt64](../data-types/int-uint.md#uint-ranges).

**例**

``` sql
SELECT normalizedQueryHash('SELECT 1 AS `xyz123`') != normalizedQueryHash('SELECT 1 AS `abc123`') AS normalizedQueryHash;
SELECT normalizedQueryHashKeepNames('SELECT 1 AS `xyz123`') != normalizedQueryHashKeepNames('SELECT 1 AS `abc123`') AS normalizedQueryHashKeepNames;
```

結果:

```result
┌─normalizedQueryHash─┐
│                   0 │
└─────────────────────┘
┌─normalizedQueryHashKeepNames─┐
│                            1 │
└──────────────────────────────┘
```

## normalizeUTF8NFC

[NFC標準化形式](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms)の文字列に変換します。文字列が有効なUTF8-エンコードされたテキストを含むと仮定しています。

**構文**

``` sql
normalizeUTF8NFC(words)
```

**引数**

- `words` — UTF8-エンコードされた入力文字列。[String](../data-types/string.md).

**返される値**

- NFC標準化形式に変換された文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT length('â'), normalizeUTF8NFC('â') AS nfc, length(nfc) AS nfc_len;
```

結果:

```result
┌─length('â')─┬─nfc─┬─nfc_len─┐
│           2 │ â   │       2 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFD

[NFD標準化形式](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms)の文字列に変換します。文字列が有効なUTF8-エンコードされたテキストを含むと仮定しています。

**構文**

``` sql
normalizeUTF8NFD(words)
```

**引数**

- `words` — UTF8-エンコードされた入力文字列。[String](../data-types/string.md).

**返される値**

- NFD標準化形式に変換された文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT length('â'), normalizeUTF8NFD('â') AS nfd, length(nfd) AS nfd_len;
```

結果:

```result
┌─length('â')─┬─nfd─┬─nfd_len─┐
│           2 │ â   │       3 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFKC

[NFKC標準化形式](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms)の文字列に変換します。文字列が有効なUTF8-エンコードされたテキストを含むと仮定しています。

**構文**

``` sql
normalizeUTF8NFKC(words)
```

**引数**

- `words` — UTF8-エンコードされた入力文字列。[String](../data-types/string.md).

**返される値**

- NFKC標準化形式に変換された文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT length('â'), normalizeUTF8NFKC('â') AS nfkc, length(nfkc) AS nfkc_len;
```

結果:

```result
┌─length('â')─┬─nfkc─┬─nfkc_len─┐
│           2 │ â    │        2 │
└─────────────┴──────┴──────────┘
```

## normalizeUTF8NFKD

[NFKD標準化形式](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms)の文字列に変換します。文字列が有効なUTF8-エンコードされたテキストを含むと仮定しています。

**構文**

``` sql
normalizeUTF8NFKD(words)
```

**引数**

- `words` — UTF8-エンコードされた入力文字列。[String](../data-types/string.md).

**返される値**

- NFKD標準化形式に変換された文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT length('â'), normalizeUTF8NFKD('â') AS nfkd, length(nfkd) AS nfkd_len;
```

結果:

```result
┌─length('â')─┬─nfkd─┬─nfkd_len─┐
│           2 │ â    │        3 │
└─────────────┴──────┴──────────┘
```

## encodeXMLComponent

XMLのテキストノードまたは属性にその後配置できるように、XMLで特別な意味を持つ文字をエスケープします。

次の文字が置き換えられます： `<`, `&`, `>`, `"`, `'`.
[XMLおよびHTMLの文字エンティティ参照のリスト](https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references)も参照してください。

**構文**

``` sql
encodeXMLComponent(x)
```

**引数**

- `x` — 入力文字列。[String](../data-types/string.md).

**返される値**

- エスケープされた文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT encodeXMLComponent('Hello, "world"!');
SELECT encodeXMLComponent('<123>');
SELECT encodeXMLComponent('&clickhouse');
SELECT encodeXMLComponent('\'foo\'');
```

結果:

```result
Hello, &quot;world&quot;!
&lt;123&gt;
&amp;clickhouse
&apos;foo&apos;
```

## decodeXMLComponent

XMLで特別な意味を持つ部分文字列をエスケープ解除します。これらの部分文字列は： `&quot;` `&amp;` `&apos;` `&gt;` `&lt;`

この関数は、数値文字参照もUnicode文字に置き換えます。10進数（`&#10003;` のようなもの）と16進数（`&#x2713;` のようなもの）の両方の形式がサポートされています。

**構文**

``` sql
decodeXMLComponent(x)
```

**引数**

- `x` — 入力文字列。[String](../data-types/string.md).

**返される値**

- エスケープ解除された文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT decodeXMLComponent('&apos;foo&apos;');
SELECT decodeXMLComponent('&lt; &#x3A3; &gt;');
```

結果:

```result
'foo'
< Σ >
```

## decodeHTMLComponent

HTMLで特別な意味を持つ部分文字列をエスケープ解除します。例えば： `&hbar;` `&gt;` `&diamondsuit;` `&heartsuit;` `&lt;` など。

この関数は、数値文字参照もUnicode文字に置き換えます。10進数（`&#10003;` のようなもの）と16進数（`&#x2713;` のようなもの）の両方の形式がサポートされています。

**構文**

``` sql
decodeHTMLComponent(x)
```

**引数**

- `x` — 入力文字列。[String](../data-types/string.md).

**返される値**

- エスケープ解除された文字列。[String](../data-types/string.md).

**例**

``` sql
SELECT decodeHTMLComponent(''CH');
SELECT decodeHTMLComponent('I&heartsuit;ClickHouse');
```

結果:

```result
'CH'
I♥ClickHouse'
```

## extractTextFromHTML

この関数は、HTMLまたはXHTMLからプレーンテキストを抽出します。

HTML、XMLまたはXHTML仕様に100％準拠していませんが、実装は合理的に正確で高速です。ルールは次の通りです：

1. コメントはスキップされます。例： `<!-- test -->`。コメントは `-->` で終了しなければなりません。ネストされたコメントは禁止されています。
注意: `<!-->` や `<!--->` のような構造はHTMLでは有効なコメントではありませんが、他のルールによってスキップされます。
2. CDATAはそのまま貼り付けられます。注意: CDATAはXML/XHTML固有のものであり、「ベストエフォート」ベースで処理されます。
3. `script` および `style` 要素は、そのすべての内容と共に削除されます。注意: 閉じタグは内容内に現れることはできません。例えば、JS文字列リテラルは `"<\/script>"` のようにエスケープする必要があります。
注意: コメントおよびCDATAは `script` または `style` 内でも可能であり、CDATA内では閉じタグは検索されません。例： `<script><![CDATA[</script>]]></script>` 。しかし、それらはコメント内では依然として検索されます。時にはこれが複雑になります： `<script>var x = "<!--"; </script> var y = "-->"; alert(x + y);</script>`
注意: `script` および `style` はXML名前空間の名前になることがあり、その場合、通常の`script`または`style`要素として扱われません。例： `<script:a>Hello</script:a>`。
注意: タグ名の後にホワイトスペースがある可能性があります： `</script >` しかし、前にはありません： `< / script>`。
4. その他のタグまたはタグのような要素は、内部内容なしでスキップされます。例： `<a>.</a>`
注意: このHTMLは不正と期待されます： `<a test=">"></a>`
注意: タグのようなものもスキップされます： `<>`、`<!>` など。
注意: 終端のないタグは入力の最後までスキップされます：`<hello   `
5. HTMLおよびXMLエンティティはデコードされません。別の関数で処理する必要があります。
6. テキスト内のホワイトスペースは特定の規則に基づいて折り畳まれるか挿入されます。
    - 最初と最後のホワイトスペースは削除されます。
    - 連続するホワイトスペースは折り畳まれます。
    - しかし、他の要素によって分割され、ホワイトスペースがない場合、それが挿入されます。
    - 不自然な例が生じる可能性があります： `Hello<b>world</b>`、`Hello<!-- -->world` - HTMLにはホワイトスペースがありませんが、関数はこれを挿入します。また、`Hello<p>world</p>`、`Hello<br>world`も考えてください。この振る舞いは、HTMLを単語のバッグに変換するようなデータ解析にとって合理的です。
7. ホワイトスペースの正しい処理には、`<pre></pre>`およびCSSの`display`および`white-space`プロパティのサポートが必要です。

**構文**

``` sql
extractTextFromHTML(x)
```

**引数**

- `x` — 入力テキスト。[String](../data-types/string.md).

**返される値**

- 抽出されたテキスト。[String](../data-types/string.md).

**例**

最初の例にはいくつかのタグとコメントが含まれており、ホワイトスペース処理も示しています。
2つ目の例は、CDATAとscriptタグの処理を示しています。
3つ目の例では、[url](../../sql-reference/table-functions/url.md) 関数で受け取った完全なHTMLレスポンスからテキストを抽出しています。

``` sql
SELECT extractTextFromHTML(' <p> A text <i>with</i><b>tags</b>. <!-- comments --> </p> ');
SELECT extractTextFromHTML('<![CDATA[The content within <b>CDATA</b>]]> <script>alert("Script");</script>');
SELECT extractTextFromHTML(html) FROM url('http://www.donothingfor2minutes.com/', RawBLOB, 'html String');
```

結果:

```result
A text with tags .
The content within <b>CDATA</b>
Do Nothing for 2 Minutes 2:00 &nbsp;
```

## ascii {#ascii}

文字列 `s` の最初の文字のASCIIコードポイント（Int32として）を返します。

`s` が空の場合、結果は0です。最初の文字が非ASCII文字またはUTF-16のLatin-1の補集合範囲の一部でない場合、結果は未定義です。

**構文**

```sql
ascii(s)
```

## soundex

文字列の[Soundexコード](https://en.wikipedia.org/wiki/Soundex)を返します。

**構文**

``` sql
soundex(val)
```

**引数**

- `val` — 入力値。[String](../data-types/string.md)

**返される値**

- 入力値のSoundexコード。[String](../data-types/string.md)

**例**

``` sql
select soundex('aksel');
```

結果:

```result
┌─soundex('aksel')─┐
│ A240             │
└──────────────────┘
```

## punycodeEncode

文字列の[Punycode](https://en.wikipedia.org/wiki/Punycode)表現を返します。
文字列はUTF8でエンコードされている必要があります。そうでない場合、動作は未定義です。

**構文**

``` sql
punycodeEncode(val)
```

**引数**

- `val` — 入力値。[String](../data-types/string.md)

**返される値**

- 入力値のPunycode表現。[String](../data-types/string.md)

**例**

``` sql
select punycodeEncode('München');
```

結果:

```result
┌─punycodeEncode('München')─┐
│ Mnchen-3ya                │
└───────────────────────────┘
```

## punycodeDecode

[Punycode](https://en.wikipedia.org/wiki/Punycode)でエンコードされた文字列のUTF8でエンコードされたプレーンテキストを返します。
有効なPunycodeでエンコードされた文字列が与えられない場合、例外がスローされます。

**構文**

``` sql
punycodeEncode(val)
```

**引数**

- `val` — Punycodeでエンコードされた文字列。[String](../data-types/string.md)

**返される値**

- 入力値のプレーンテキスト。[String](../data-types/string.md)

**例**

``` sql
select punycodeDecode('Mnchen-3ya');
```

結果:

```result
┌─punycodeDecode('Mnchen-3ya')─┐
│ München                      │
└──────────────────────────────┘
```

## tryPunycodeDecode

`punycodeDecode` のようですが、無効なPunycodeエンコードされた文字列が与えられた場合、空の文字列を返します。

## idnaEncode

[Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) のメカニズムに従って、ドメイン名のASCII表現（ToASCIIアルゴリズム）を返します。
入力文字列はUTFでエンコードされ、ASCII文字列に変換可能でなければなりません。そうでない場合、例外がスローされます。
注意: パーセントデコードやタブ、スペース、制御文字のトリミングは行いません。

**構文**

```sql
idnaEncode(val)
```

**引数**

- `val` — 入力値。[String](../data-types/string.md)

**返される値**

- 入力値のIDNAメカニズムに従ったASCII表現。[String](../data-types/string.md)

**例**

``` sql
select idnaEncode('straße.münchen.de');
```

結果:

```result
┌─idnaEncode('straße.münchen.de')─────┐
│ xn--strae-oqa.xn--mnchen-3ya.de     │
└─────────────────────────────────────┘
```

## tryIdnaEncode

`idnaEncode` と似ていますが、例外をスローする代わりにエラーの場合は空の文字列を返します。

## idnaDecode

[Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) のメカニズムに従ったドメイン名のUnicode (UTF-8)表現（ToUnicodeアルゴリズム）を返します。
エラーの場合（例えば入力が無効な場合）、入力文字列が返されます。
`idnaEncode()`と`idnaDecode()`を繰り返し適用すると、ケースの正規化により元の文字列が必ずしも戻るとは限らないことに注意してください。

**構文**

```sql
idnaDecode(val)
```

**引数**

- `val` — 入力値。 [String](../data-types/string.md)

**返される値**

- 入力値のIDNAメカニズムによるUnicode (UTF-8)表現。[String](../data-types/string.md)

**例**

``` sql
select idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de');
```

結果:

```result
┌─idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de')─┐
│ straße.münchen.de                             │
└───────────────────────────────────────────────┘
```

## byteHammingDistance

2つのバイト文字列間の[hamming distance](https://en.wikipedia.org/wiki/Hamming_distance)を計算します。

**構文**

```sql
byteHammingDistance(string1, string2)
```

**例**

``` sql
SELECT byteHammingDistance('karolin', 'kathrin');
```

結果:

``` text
┌─byteHammingDistance('karolin', 'kathrin')─┐
│                                         3 │
└───────────────────────────────────────────┘
```

別名: `mismatches`

## stringJaccardIndex

2つのバイト文字列間の[Jaccard類似度指数](https://en.wikipedia.org/wiki/Jaccard_index)を計算します。

**構文**

```sql
stringJaccardIndex(string1, string2)
```

**例**

``` sql
SELECT stringJaccardIndex('clickhouse', 'mouse');
```

結果:

``` text
┌─stringJaccardIndex('clickhouse', 'mouse')─┐
│                                       0.4 │
└───────────────────────────────────────────┘
```

## stringJaccardIndexUTF8

[stringJaccardIndex](#stringjaccardindex)と同様ですが、UTF8エンコードされた文字列用です。

## editDistance

2つのバイト文字列間の[編集距離](https://en.wikipedia.org/wiki/Edit_distance)を計算します。

**構文**

```sql
editDistance(string1, string2)
```

**例**

``` sql
SELECT editDistance('clickhouse', 'mouse');
```

結果:

``` text
┌─editDistance('clickhouse', 'mouse')─┐
│                                   6 │
└─────────────────────────────────────┘
```

別名: `levenshteinDistance`

## editDistanceUTF8

2つのUTF8文字列間の[編集距離](https://en.wikipedia.org/wiki/Edit_distance)を計算します。

**構文**

```sql
editDistanceUTF8(string1, string2)
```

**例**

``` sql
SELECT editDistanceUTF8('我是谁', '我是我');
```

結果:

``` text
┌─editDistanceUTF8('我是谁', '我是我')──┐
│                                   1 │
└─────────────────────────────────────┘
```

別名: `levenshteinDistanceUTF8`

## damerauLevenshteinDistance

2つのバイト文字列間の[Damerau-Levenshtein距離](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance)を計算します。

**構文**

```sql
damerauLevenshteinDistance(string1, string2)
```

**例**

``` sql
SELECT damerauLevenshteinDistance('clickhouse', 'mouse');
```

結果:

``` text
┌─damerauLevenshteinDistance('clickhouse', 'mouse')─┐
│                                                 6 │
└───────────────────────────────────────────────────┘
```

## jaroSimilarity

2つのバイト文字列間の[Jaro類似度](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance#Jaro_similarity)を計算します。

**構文**

```sql
jaroSimilarity(string1, string2)
```

**例**

``` sql
SELECT jaroSimilarity('clickhouse', 'click');
```

結果:

``` text
┌─jaroSimilarity('clickhouse', 'click')─┐
│                    0.8333333333333333 │
└───────────────────────────────────────┘
```

## jaroWinklerSimilarity

2つのバイト文字列間の[Jaro-Winkler類似度](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance#Jaro%E2%80%93Winkler_similarity)を計算します。

**構文**

```sql
jaroWinklerSimilarity(string1, string2)
```

**例**

``` sql
SELECT jaroWinklerSimilarity('clickhouse', 'click');
```

結果:

``` text
┌─jaroWinklerSimilarity('clickhouse', 'click')─┐
│                           0.8999999999999999 │
└──────────────────────────────────────────────┘
```

## initcap

各単語の最初の文字を大文字に変換し、残りを小文字に変換します。単語は非英数字によって区切られた英数字の連続です。

:::note
`initCap`は単語の最初の文字のみを大文字に変換するため、アポストロフィや大文字を含む単語で予期しない動作をすることがあります。例えば:

```sql
SELECT initCap('mother''s daughter'), initCap('joe McAdam');
```

は以下のように返されます

```response
┌─initCap('mother\'s daughter')─┬─initCap('joe McAdam')─┐
│ Mother'S Daughter             │ Joe Mcadam            │
└───────────────────────────────┴───────────────────────┘
```

これは既知の動作であり、現在のところ修正予定はありません。
:::

**構文**

```sql
initcap(val)
```

**引数**

- `val` — 入力値。[String](../data-types/string.md).

**返される値**

- 各単語の最初の文字が大文字に変換された`val`。[String](../data-types/string.md).

**例**

クエリ:

```sql
SELECT initcap('building for fast');
```

結果:

```text
┌─initcap('building for fast')─┐
│ Building For Fast            │
└──────────────────────────────┘
```

## initcapUTF8

[initcap](#initcap)と同様に、`initcapUTF8`は各単語の最初の文字を大文字に変換し、残りを小文字に変換します。
文字列が有効なUTF-8エンコードされたテキストを含むことを前提としています。この前提が違反された場合、例外はスローされず、結果は未定義です。

:::note
この関数は言語を検出しません。例えばトルコ語の場合、結果は正確ではないかもしれません (i/İ vs. i/I)。コードポイントの大文字と小文字のUTF-8バイトシーケンスの長さが異なる場合、このコードポイントに対する結果は正しくない可能性があります。
:::

**構文**

```sql
initcapUTF8(val)
```

**引数**

- `val` — 入力値。[String](../data-types/string.md).

**返される値**

- 各単語の最初の文字が大文字に変換された`val`。[String](../data-types/string.md).

**例**

クエリ:

```sql
SELECT initcapUTF8('не тормозит');
```

結果:

```text
┌─initcapUTF8('не тормозит')─┐
│ Не Тормозит                │
└────────────────────────────┘
```

## firstLine

複数行の文字列から最初の行を返します。

**構文**

```sql
firstLine(val)
```

**引数**

- `val` — 入力値。[String](../data-types/string.md)

**返される値**

- 入力値の最初の行または改行がない場合は全体。[String](../data-types/string.md)

**例**

```sql
select firstLine('foo\nbar\nbaz');
```

結果:

```result
┌─firstLine('foo\nbar\nbaz')─┐
│ foo                        │
└────────────────────────────┘
```
