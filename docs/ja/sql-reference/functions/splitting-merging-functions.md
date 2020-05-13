---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 47
toc_title: "\u6587\u5B57\u5217\u3068\u914D\u5217\u306E\u5206\u5272\u3068\u30DE\u30FC\
  \u30B8"
---

# 文字列と配列の分割とマージのための関数 {#functions-for-splitting-and-merging-strings-and-arrays}

## splitByChar(セパレータ,s) {#splitbycharseparator-s}

文字列を、指定した文字で区切った部分文字列に分割します。 定数文字列を使用します `separator` その正確に一つの文字からなる。
選択した部分文字列の配列を返します。 空の部分文字列は、文字列の先頭または末尾にセパレータがある場合、または複数の連続するセパレータがある場合に選択できます。

**構文**

``` sql
splitByChar(<separator>, <s>)
```

**パラメータ**

-   `separator` — The separator which should contain exactly one character. [文字列](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [文字列](../../sql-reference/data-types/string.md).

**戻り値(s)**

選択した部分文字列の配列を返します。 空の部分文字列は、次の場合に選択できます:

-   区切り記号は、文字列の先頭または末尾に表示されます;
-   複数の連続した区切り文字があります;
-   元の文字列 `s` 空です。

タイプ: [配列](../../sql-reference/data-types/array.md) の [文字列](../../sql-reference/data-types/string.md).

**例えば**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString(separator,s) {#splitbystringseparator-s}

文字列を文字列で区切られた部分文字列に分割します。 定数文字列を使用します `separator` 区切り文字として複数の文字が使用されます。 文字列の場合 `separator` 空である場合は、文字列を分割します `s` 単一の文字の配列に変換します。

**構文**

``` sql
splitByString(<separator>, <s>)
```

**パラメータ**

-   `separator` — The separator. [文字列](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [文字列](../../sql-reference/data-types/string.md).

**戻り値(s)**

選択した部分文字列の配列を返します。 空の部分文字列は、次の場合に選択できます:

タイプ: [配列](../../sql-reference/data-types/array.md) の [文字列](../../sql-reference/data-types/string.md).

-   空でない区切り文字は、文字列の先頭または末尾に作成されます;
-   複数の連続する空でない区切り記号があります;
-   元の文字列 `s` 区切り記号が空でない間は空です。

**例えば**

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

## arrayStringConcat(arr\[,separator\]) {#arraystringconcatarr-separator}

配列にリストされている文字列を区切り文字と連結します。デフォルトでは空の文字列に設定されています。
文字列を返します。

## alphaTokens(s) {#alphatokenss}

範囲a-zおよびa-zから連続するバイトの部分文字列を選択します。

**例えば**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
