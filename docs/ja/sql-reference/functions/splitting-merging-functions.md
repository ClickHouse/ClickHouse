---
slug: /ja/sql-reference/functions/splitting-merging-functions
sidebar_position: 165
sidebar_label: 文字列の分割
---

# 文字列を分割するための関数

## splitByChar

特定の文字で文字列を分割してサブ文字列を生成します。1文字からなる定数文字列 `separator` を使用します。選択されたサブ文字列の配列を返します。文字列の先頭や末尾に区切り文字がある場合や、連続して区切り文字が存在する場合は、空のサブ文字列も選択されることがあります。

**構文**

``` sql
splitByChar(separator, s[, max_substrings]))
```

**引数**

- `separator` — 1文字からなる区切り文字。[String](../data-types/string.md)。
- `s` — 分割する文字列。[String](../data-types/string.md)。
- `max_substrings` — 任意の `Int64`。デフォルトは0です。`max_substrings` > 0 の場合、返される配列には最大で `max_substrings` のサブ文字列が含まれます。そうでない場合、可能な限り多くのサブ文字列が返されます。

**返される値**

- 選択されたサブ文字列の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

空のサブ文字列が選択される状況:

- 文字列の先頭または末尾に区切り文字がある場合;
- 連続して複数の区切り文字がある場合;
- 元の文字列 `s` が空である場合。

:::note
ClickHouse v22.11 以降、パラメータ `max_substrings` の動作が変更されました。それ以前のバージョンでは、`max_substrings > 0` は `max_substring` 回の分割が行われ、文字列の残りはリストの最後の要素として返されていました。
例えば、
- v22.10 では: `SELECT splitByChar('=', 'a=b=c=d', 2);` は `['a','b','c=d']` を返しました
- v22.11 では: `SELECT splitByChar('=', 'a=b=c=d', 2);` は `['a','b']` を返しました

ClickHouseのv22.11以前のような動作を得るには、[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) を設定してください。
`SELECT splitByChar('=', 'a=b=c=d', 2) SETTINGS splitby_max_substrings_includes_remaining_string = 1 -- ['a', 'b=c=d']`
:::

**例**

``` sql
SELECT splitByChar(',', '1,2,3,abcde');
```

結果:

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString

文字列を特定の文字列で分割します。複数文字からなる定数文字列 `separator` を区切り文字として使用します。文字列 `separator` が空の場合、文字列 `s` を単一文字の配列に分割します。

**構文**

``` sql
splitByString(separator, s[, max_substrings]))
```

**引数**

- `separator` — 区切り文字。[String](../data-types/string.md)。
- `s` — 分割する文字列。[String](../data-types/string.md)。
- `max_substrings` — 任意の `Int64`。デフォルトは0です。`max_substrings` > 0 の場合、返されるサブ文字列は最大で `max_substrings` になり、それ以外は可能な限り多くのサブ文字列が返されます。

**返される値**

- 選択されたサブ文字列の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

空のサブ文字列が選択される状況:

- 非空の区切り文字が文字列の先頭や末尾にある場合;
- 非空の区切り文字が連続している場合;
- 元の文字列 `s` が空で区切り文字が空ではない場合。

:::note
[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) を設定すると、`max_substrings` > 0 の場合に残りの文字列が結果配列の最後の要素に含まれるかどうかを制御します（デフォルト: 0）。
:::

**例**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde');
```

結果:

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde');
```

結果:

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByRegexp

正規表現で指定したパターンを区切りとして文字列を分割します。正規表現文字列 `regexp` を区切り文字として使用します。`regexp` が空の場合、文字列 `s` を1文字ずつの配列に分割します。この正規表現と一致する部分が見つからない場合、文字列 `s` は分割されません。

**構文**

``` sql
splitByRegexp(regexp, s[, max_substrings]))
```

**引数**

- `regexp` — 正規表現。定数。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- `s` — 分割する文字列。[String](../data-types/string.md)。
- `max_substrings` — 任意の `Int64`。デフォルトは0です。`max_substrings` > 0 の場合、返されるサブ文字列は最大で `max_substrings` になります。それ以外は、可能な限り多くのサブ文字列が返されます。

**返される値**

- 選択されたサブ文字列の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

空のサブ文字列が選択される状況:

- 非空の正規表現の一致が文字列の先頭または末尾にある場合;
- 非空の正規表現の一致が連続している場合;
- 元の文字列 `s` が空で正規表現が空ではない場合。

:::note
[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) を設定すると、`max_substrings` > 0 の場合に残りの文字列が結果配列の最後の要素に含まれるかどうかを制御します（デフォルト: 0）。
:::

**例**

``` sql
SELECT splitByRegexp('\\d+', 'a12bc23de345f');
```

結果:

``` text
┌─splitByRegexp('\\d+', 'a12bc23de345f')─┐
│ ['a','bc','de','f']                    │
└────────────────────────────────────────┘
```

``` sql
SELECT splitByRegexp('', 'abcde');
```

結果:

``` text
┌─splitByRegexp('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByWhitespace

空白文字で文字列を分割します。選択されたサブ文字列の配列を返します。

**構文**

``` sql
splitByWhitespace(s[, max_substrings]))
```

**引数**

- `s` — 分割する文字列。[String](../data-types/string.md)。
- `max_substrings` — 任意の `Int64`。デフォルトは0です。`max_substrings` > 0 の場合、返されるサブ文字列は最大で `max_substrings` になります。それ以外は、可能な限り多くのサブ文字列が返されます。

**返される値**

- 選択されたサブ文字列の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

:::note
[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) を設定すると、`max_substrings` > 0 の場合に残りの文字列が結果配列の最後の要素に含まれるかどうかを制御します（デフォルト: 0）。
:::

**例**

``` sql
SELECT splitByWhitespace('  1!  a,  b.  ');
```

結果:

``` text
┌─splitByWhitespace('  1!  a,  b.  ')─┐
│ ['1!','a,','b.']                    │
└─────────────────────────────────────┘
```

## splitByNonAlpha

空白または句読点で文字列を分割します。選択されたサブ文字列の配列を返します。

**構文**

``` sql
splitByNonAlpha(s[, max_substrings]))
```

**引数**

- `s` — 分割する文字列。[String](../data-types/string.md)。
- `max_substrings` — 任意の `Int64`。デフォルトは0です。`max_substrings` > 0 の場合、返されるサブ文字列は最大で `max_substrings` になります。それ以外は、可能な限り多くのサブ文字列が返されます。

**返される値**

- 選択されたサブ文字列の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

:::note
[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) を設定すると、`max_substrings` > 0 の場合に残りの文字列が結果配列の最後の要素に含まれるかどうかを制御します（デフォルト: 0）。
:::

**例**

``` sql
SELECT splitByNonAlpha('  1!  a,  b.  ');
```

結果:

``` text
┌─splitByNonAlpha('  1!  a,  b.  ')─┐
│ ['1','a','b']                     │
└───────────────────────────────────┘
```

## arrayStringConcat

配列内の値を区切り文字で結合して文字列を返します。`separator` はオプションのパラメータであり、デフォルトでは空文字列として設定されています。

**構文**

```sql
arrayStringConcat(arr\[, separator\])
```

**例**

``` sql
SELECT arrayStringConcat(['12/05/2021', '12:50:00'], ' ') AS DateString;
```

結果:

```text
┌─DateString──────────┐
│ 12/05/2021 12:50:00 │
└─────────────────────┘
```

## alphaTokens

a-z および A-Z の範囲の連続したバイト列のサブ文字列を選択します。サブ文字列の配列を返します。

**構文**

``` sql
alphaTokens(s[, max_substrings]))
```

エイリアス: `splitByAlpha`

**引数**

- `s` — 分割する文字列。[String](../data-types/string.md)。
- `max_substrings` — 任意の `Int64`。デフォルトは0です。`max_substrings` > 0 の場合、返されるサブ文字列は最大で `max_substrings` になります。それ以外は、可能な限り多くのサブ文字列が返されます。

**返される値**

- 選択されたサブ文字列の配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

:::note
[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) を設定すると、`max_substrings` > 0 の場合に残りの文字列が結果配列の最後の要素に含まれるかどうかを制御します（デフォルト: 0）。
:::

**例**

``` sql
SELECT alphaTokens('abca1abc');
```

結果:

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

## extractAllGroups

正規表現で一致する非重複サブ文字列からすべてのグループを抽出します。

**構文**

``` sql
extractAllGroups(text, regexp)
```

**引数**

- `text` — [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- `regexp` — 正規表現。定数。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- 少なくとも1つの一致するグループが見つかった場合、`Array(Array(String))` をグループID（1からN、`regexp`内のキャプチャグループの数N）でクラスタリングして返します。一致するグループがない場合、空の配列を返します。[Array](../data-types/array.md) 。

**例**

``` sql
SELECT extractAllGroups('abc=123, 8="hkl"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

結果:

``` text
┌─extractAllGroups('abc=123, 8="hkl"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','123'],['8','"hkl"']]                                         │
└───────────────────────────────────────────────────────────────────────┘
```

## ngrams

UTF-8文字列を `ngramsize` 記号のn-gramに分割します。

**構文**

``` sql
ngrams(string, ngramsize)
```

**引数**

- `string` — 文字列。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。
- `ngramsize` — n-gramのサイズ。[UInt](../data-types/int-uint.md)。

**返される値**

- n-gramの配列。[Array](../data-types/array.md)([String](../data-types/string.md))。

**例**

``` sql
SELECT ngrams('ClickHouse', 3);
```

結果:

``` text
┌─ngrams('ClickHouse', 3)───────────────────────────┐
│ ['Cli','lic','ick','ckH','kHo','Hou','ous','use'] │
└───────────────────────────────────────────────────┘
```

## tokens

ASCIIの非英数字文字をセパレータとして使用して文字列をトークンに分割します。

**引数**

- `input_string` — [String](../data-types/string.md)データ型オブジェクトで表された任意のバイトセット。

**返される値**

- 入力文字列からのトークン配列。[Array](../data-types/array.md)。

**例**

``` sql
SELECT tokens('test1,;\\ test2,;\\ test3,;\\   test4') AS tokens;
```

結果:

``` text
┌─tokens────────────────────────────┐
│ ['test1','test2','test3','test4'] │
└───────────────────────────────────┘
```
