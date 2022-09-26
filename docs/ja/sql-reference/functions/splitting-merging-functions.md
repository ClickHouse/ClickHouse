---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "\u6587\u5B57\u5217\u3068\u914D\u5217\u306E\u5206\u5272\u3068\u30DE\u30FC\
  \u30B8"
---

# 文字列と配列を分割および結合するための関数 {#functions-for-splitting-and-merging-strings-and-arrays}

## splitByChar(セパレーター、s) {#splitbycharseparator-s}

文字列を、指定した文字で区切られた部分文字列に分割します。 定数文字列を使用します `separator` これは正確に一つの文字で構成されます。
選択した部分文字列の配列を返します。 文字列の先頭または末尾に区切り文字がある場合、または複数の連続した区切り文字がある場合は、空の部分文字列を選択できます。

**構文**

``` sql
splitByChar(<separator>, <s>)
```

**パラメータ**

-   `separator` — The separator which should contain exactly one character. [文字列](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [文字列](../../sql-reference/data-types/string.md).

**戻り値)**

選択した部分文字列の配列を返します。 空の部分文字列は、次の場合に選択できます:

-   区切り文字は、文字列の先頭または末尾にあります;
-   複数の連続した区切り文字があります;
-   元の文字列 `s` 空です。

タイプ: [配列](../../sql-reference/data-types/array.md) の [文字列](../../sql-reference/data-types/string.md).

**例**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString(セパレーター、s) {#splitbystringseparator-s}

文字列を文字列で区切られた部分文字列に分割します。 定数文字列を使用します `separator` 区切り文字として複数の文字を指定します。 文字列の場合 `separator` 空の場合は、文字列を分割します `s` 単一の文字の配列に。

**構文**

``` sql
splitByString(<separator>, <s>)
```

**パラメータ**

-   `separator` — The separator. [文字列](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [文字列](../../sql-reference/data-types/string.md).

**戻り値)**

選択した部分文字列の配列を返します。 空の部分文字列は、次の場合に選択できます:

タイプ: [配列](../../sql-reference/data-types/array.md) の [文字列](../../sql-reference/data-types/string.md).

-   空でない区切り文字は、文字列の先頭または末尾にあります;
-   連続した空でない区切り文字が複数あります;
-   元の文字列 `s` 区切り文字が空でない間は空です。

**例**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde')
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde')
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## arrayStringConcat(arr\[,区切り記号\]) {#arraystringconcatarr-separator}

配列にリストされている文字列を区切り文字で連結します。'separator'はオプションのパラメータです:定数文字列で、デフォルトでは空の文字列に設定されます。
文字列を返します。

## アルファトケンス(曖昧さ回避) {#alphatokenss}

A-zおよびA-Zの範囲から連続したバイトの部分文字列を選択します。

**例**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[元の記事](https://clickhouse.com/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
