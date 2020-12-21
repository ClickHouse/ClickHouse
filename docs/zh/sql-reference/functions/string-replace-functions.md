# 字符串替换函数 {#zi-fu-chuan-ti-huan-han-shu}

## replaceOne（大海捞针，模式，更换) {#replaceonehaystack-pattern-replacement}

用’replacement’子串替换’haystack’中与’pattern’子串第一个匹配的匹配项（如果存在）。
’pattern’和’replacement’必须是常量。

## replaceAll（大海捞针，模式，替换），替换（大海捞针，模式，替换) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

用’replacement’子串替换’haystack’中出现的所有’pattern’子串。

## replaceRegexpOne（大海捞针，模式，更换) {#replaceregexponehaystack-pattern-replacement}

使用’pattern’正则表达式替换。 ‘pattern’可以是任意一个有效的re2正则表达式。
如果存在与正则表达式匹配的匹配项，仅替换第一个匹配项。
同时‘replacement’可以指定为正则表达式中的捕获组。可以包含`\0-\9`。
在这种情况下，函数将使用正则表达式的整个匹配项替换‘\\0’。使用其他与之对应的子模式替换对应的’\\1-\\9’。要在模版中使用’‘字符，请使用’’将其转义。
另外还请记住，字符串文字需要额外的转义。

示例1.将日期转换为美国格式：

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

    2014-03-17      03/17/2014
    2014-03-18      03/18/2014
    2014-03-19      03/19/2014
    2014-03-20      03/20/2014
    2014-03-21      03/21/2014
    2014-03-22      03/22/2014
    2014-03-23      03/23/2014

示例2.复制字符串十次：

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

    ┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
    │ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
    └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

## replaceRegexpAll（大海捞针，模式，替换) {#replaceregexpallhaystack-pattern-replacement}

与replaceRegexpOne相同，但会替换所有出现的匹配项。例如：

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

    ┌─res────────────────────────┐
    │ HHeelllloo,,  WWoorrlldd!! │
    └────────────────────────────┘

例外的是，如果使用正则表达式捕获空白子串，则仅会进行一次替换。
示例:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

    ┌─res─────────────────┐
    │ here: Hello, World! │
    └─────────────────────┘

## regexpQuoteMeta(s) {#regexpquotemetas}

该函数用于在字符串中的某些预定义字符之前添加反斜杠。
预定义字符：‘0’，‘\\’，‘\|’，‘(’，‘)’，‘^’，‘$’，‘。’，‘\[’，‘\]’，‘？’，‘\*’，‘+’，‘{’，‘：’，’ - ’。
这个实现与re2 :: RE2 :: QuoteMeta略有不同。它以\\0而不是00转义零字节，它只转义所需的字符。
有关详细信息，请参阅链接：\[RE2\]（https://github.com/google/re2/blob/master/re2/re2.cc\#L473）

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/string_replace_functions/) <!--hide-->
