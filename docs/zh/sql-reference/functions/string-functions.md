---
slug: /zh/sql-reference/functions/string-functions
---
# 字符串函数 {#zi-fu-chuan-han-shu}

## empty {#string-functions-empty}

对于空字符串返回1，对于非空字符串返回0。
结果类型是UInt8。
如果字符串包含至少一个字节，则该字符串被视为非空字符串，即使这是一个空格或空字符。
该函数也适用于数组。

## notEmpty {#notempty}

对于空字符串返回0，对于非空字符串返回1。
结果类型是UInt8。
该函数也适用于数组。

## length {#length}

返回字符串的字节长度。
结果类型是UInt64。
该函数也适用于数组。

## lengthUTF8 {#lengthutf8}

假定字符串以UTF-8编码组成的文本，返回此字符串的Unicode字符长度。如果传入的字符串不是UTF-8编码，则函数可能返回一个预期外的值（不会抛出异常）。
结果类型是UInt64。

## char_length,CHAR_LENGTH {#char-length-char-length}

假定字符串以UTF-8编码组成的文本，返回此字符串的Unicode字符长度。如果传入的字符串不是UTF-8编码，则函数可能返回一个预期外的值（不会抛出异常）。
结果类型是UInt64。

## character_length,CHARACTER_LENGTH {#character-length-character-length}

假定字符串以UTF-8编码组成的文本，返回此字符串的Unicode字符长度。如果传入的字符串不是UTF-8编码，则函数可能返回一个预期外的值（不会抛出异常）。
结果类型是UInt64。

## lower, lcase {#lower-lcase}

将字符串中的ASCII转换为小写。

## upper, ucase {#upper-ucase}

将字符串中的ASCII转换为大写。

## lowerUTF8 {#lowerutf8}

将字符串转换为小写，函数假设字符串是以UTF-8编码文本的字符集。
同时函数不检测语言。因此对土耳其人来说，结果可能不完全正确。
如果UTF-8字节序列的长度对于代码点的大写和小写不同，则该代码点的结果可能不正确。
如果字符串包含一组非UTF-8的字节，则将引发未定义行为。

## upperUTF8 {#upperutf8}

将字符串转换为大写，函数假设字符串是以UTF-8编码文本的字符集。
同时函数不检测语言。因此对土耳其人来说，结果可能不完全正确。
如果UTF-8字节序列的长度对于代码点的大写和小写不同，则该代码点的结果可能不正确。
如果字符串包含一组非UTF-8的字节，则将引发未定义行为。

## isValidUTF8 {#isvalidutf8}

检查字符串是否为有效的UTF-8编码，是则返回1，否则返回0。

## toValidUTF8 {#tovalidutf8}

用`�`（U+FFFD）字符替换无效的UTF-8字符。所有连续的无效字符都会被替换为一个替换字符。

    toValidUTF8( input_string )

参数：

-   input_string — 任何一个[字符串](../../sql-reference/functions/string-functions.md)类型的对象。

返回值： 有效的UTF-8字符串。

### 示例 {#shi-li}

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## reverse {#reverse}

反转字符串。

## reverseUTF8 {#reverseutf8}

以Unicode字符为单位反转UTF-8编码的字符串。如果字符串不是UTF-8编码，则可能获取到一个非预期的结果（不会抛出异常）。

## format(pattern, s0, s1, ...) {#formatpattern-s0-s1}

使用常量字符串`pattern`格式化其他参数。`pattern`字符串中包含由大括号`{}`包围的«替换字段»。 未被包含在大括号中的任何内容都被视为文本内容，它将原样保留在返回值中。 如果你需要在文本内容中包含一个大括号字符，它可以通过加倍来转义：`{{ '{{' }}`和`{{ '{{' }} '}}' }}`。 字段名称可以是数字（从零开始）或空（然后将它们视为连续数字）

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')

┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘

SELECT format('{} {}', 'Hello', 'World')

┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat(s1, s2, ...) {#concat-s1-s2}

将参数中的多个字符串拼接，不带分隔符。

## concatAssumeInjective(s1, s2, ...) {#concatassumeinjectives1-s2}

与[concat](#concat-s1-s2)相同，区别在于，你需要保证concat(s1, s2, s3) -\> s4是单射的，它将用于GROUP BY的优化。

## substring(s,offset,length),mid(s,offset,length),substr(s,offset,length) {#substrings-offset-length-mids-offset-length-substrs-offset-length}

以字节为单位截取指定位置字符串，返回以’offset’位置为开头，长度为’length’的子串。’offset’从1开始（与标准SQL相同）。’offset’和’length’参数必须是常量。

## substringUTF8(s,offset,length) {#substringutf8s-offset-length}

与’substring’相同，但其操作单位为Unicode字符，函数假设字符串是以UTF-8进行编码的文本。如果不是则可能返回一个预期外的结果（不会抛出异常）。

## appendTrailingCharIfAbsent(s,c) {#appendtrailingcharifabsents-c}

如果’s’字符串非空并且末尾不包含’c’字符，则将’c’字符附加到末尾。

## convertCharset(s,from,to) {#convertcharsets-from-to}

返回从’from’中的编码转换为’to’中的编码的字符串’s’。

## base64Encode(s) {#base64encodes}

将字符串’s’编码成base64

## base64Decode(s) {#base64decodes}

使用base64将字符串解码成原始字符串。如果失败则抛出异常。

## tryBase64Decode(s) {#trybase64decodes}

使用base64将字符串解码成原始字符串。但如果出现错误，将返回空字符串。

## endsWith(s,后缀) {#endswiths-suffix}

返回是否以指定的后缀结尾。如果字符串以指定的后缀结束，则返回1，否则返回0。

## startsWith（s，前缀) {#startswiths-prefix}

返回是否以指定的前缀开头。如果字符串以指定的前缀开头，则返回1，否则返回0。

## trimLeft(s) {#trimlefts}

返回一个字符串，用于删除左侧的空白字符。

## trimRight(s) {#trimrights}

返回一个字符串，用于删除右侧的空白字符。

## trimBoth(s) {#trimboths}

返回一个字符串，用于删除任一侧的空白字符。

## soundex(s)

返回一个字符串的soundex值。输出类型是FixedString，示例如下：

``` sql
select soundex('aksql');

┌─soundex('aksel')─┐
│ A240             │
└──────────────────┘
```
