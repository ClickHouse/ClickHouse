# 字符串拆分合并函数 {#zi-fu-chuan-chai-fen-he-bing-han-shu}

## splitByChar（分隔符，s) {#splitbycharseparator-s}

将字符串以’separator’拆分成多个子串。’separator’必须为仅包含一个字符的字符串常量。
返回拆分后的子串的数组。 如果分隔符出现在字符串的开头或结尾，或者如果有多个连续的分隔符，则将在对应位置填充空的子串。

## splitByString(分隔符，s) {#splitbystringseparator-s}

与上面相同，但它使用多个字符的字符串作为分隔符。 该字符串必须为非空。

## arrayStringConcat(arr\[,分隔符\]) {#arraystringconcatarr-separator}

使用separator将数组中列出的字符串拼接起来。’separator’是一个可选参数：一个常量字符串，默认情况下设置为空字符串。
返回拼接后的字符串。

## alphaTokens(s) {#alphatokenss}

从范围a-z和A-Z中选择连续字节的子字符串。返回子字符串数组。

**示例：**

    SELECT alphaTokens('abca1abc')

    ┌─alphaTokens('abca1abc')─┐
    │ ['abca','abc']          │
    └─────────────────────────┘

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
