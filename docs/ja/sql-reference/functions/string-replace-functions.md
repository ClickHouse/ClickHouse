---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "\u6587\u5B57\u5217\u3067\u7F6E\u63DB\u3059\u308B\u5834\u5408"
---

# 文字列の検索と置換のための関数 {#functions-for-searching-and-replacing-in-strings}

## replaceOne(干し草、パターン、置換) {#replaceonehaystack-pattern-replacement}

それが存在する場合は、最初の出現を置き換えます。 ‘pattern’ サブストリング ‘haystack’ と ‘replacement’ 部分文字列。
以降, ‘pattern’ と ‘replacement’ 定数である必要があります。

## replaceAll(干し草,パターン,置換),replace(干し草,パターン,置換) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

すべての出現を置き換えます。 ‘pattern’ サブストリング ‘haystack’ と ‘replacement’ 部分文字列。

## replaceRegexpOne(干し草、パターン、置換) {#replaceregexponehaystack-pattern-replacement}

を使用して交換 ‘pattern’ 正規表現。 Re2正規表現。
存在する場合は、最初の出現のみを置き換えます。
パターンは次のように指定できます ‘replacement’. このパター `\0-\9`.
置換 `\0` 正規表現全体を含みます。 置換 `\1-\9` サブパターンに対応するnumbers.To 使用する `\` テンプレー `\`.
また、文字列リテラルには余分なエスケープが必要です。

例1 日付をアメリカ形式に変換する:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

例2。 文字列を十回コピーする:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll(干し草の山,パターン,置換) {#replaceregexpallhaystack-pattern-replacement}

これは同じことを行いますが、すべての出現を置き換えます。 例:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

例外として、正規表現が空の部分文字列に対して機能した場合、置換は複数回行われません。
例:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## regexpQuoteMeta(s) {#regexpquotemetas}

関数は、文字列内のいくつかの定義済み文字の前に円記号を追加します。
定義済み文字: ‘0’, ‘\\’, ‘\|’, ‘(’, ‘)’, ‘^’, ‘$’, ‘.’, ‘\[’, '\]', ‘?’, '\*‘,’+‘,’{‘,’:‘,’-'.
この実装はre2::RE2::QuoteMetaとは若干異なります。 ゼロバイトは00の代わりに\\0としてエスケープされ、必要な文字のみがエスケープされます。
詳細は、リンクを参照してください: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/string_replace_functions/) <!--hide-->
