# JSON函数 {#jsonhan-shu}

在Yandex.Metrica中，用户使用JSON作为访问参数。为了处理这些JSON，实现了一些函数。（尽管在大多数情况下，JSON是预先进行额外处理的，并将结果值放在单独的列中。）所有的这些函数都进行了尽可能的假设。以使函数能够尽快的完成工作。

我们对JSON格式做了如下假设：

1.  字段名称（函数的参数）必须使常量。
2.  字段名称必须使用规范的编码。例如：`visitParamHas('{"abc":"def"}', 'abc') = 1`，但是 `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  函数可以随意的在多层嵌套结构下查找字段。如果存在多个匹配字段，则返回第一个匹配字段。
4.  JSON除字符串文本外不存在空格字符。

## visitParamHas(参数，名称) {#visitparamhasparams-name}

检查是否存在«name»名称的字段

## visitParamExtractUInt(参数，名称) {#visitparamextractuintparams-name}

将名为«name»的字段的值解析成UInt64。如果这是一个字符串字段，函数将尝试从字符串的开头解析一个数字。如果该字段不存在，或无法从它中解析到数字，则返回0。

## visitParamExtractInt(参数，名称) {#visitparamextractintparams-name}

与visitParamExtractUInt相同，但返回Int64。

## visitParamExtractFloat(参数，名称) {#visitparamextractfloatparams-name}

与visitParamExtractUInt相同，但返回Float64。

## visitParamExtractBool(参数，名称) {#visitparamextractboolparams-name}

解析true/false值。其结果是UInt8类型的。

## visitParamExtractRaw(参数，名称) {#visitparamextractrawparams-name}

返回字段的值，包含空格符。

示例:

    visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
    visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'

## visitParamExtractString(参数，名称) {#visitparamextractstringparams-name}

使用双引号解析字符串。这个值没有进行转义。如果转义失败，它将返回一个空白字符串。

示例:

    visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
    visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
    visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
    visitParamExtractString('{"abc":"hello}', 'abc') = ''

目前不支持`\uXXXX\uYYYY`这些字符编码，这些编码不在基本多文种平面中（它们被转化为CESU-8而不是UTF-8）。

以下函数基于[simdjson](https://github.com/lemire/simdjson)，专为更复杂的JSON解析要求而设计。但上述假设2仍然适用。

## JSONHas(json\[, indices_or_keys\]…) {#jsonhasjson-indices-or-keys}

如果JSON中存在该值，则返回`1`。

如果该值不存在，则返回`0`。

示例：

    select JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
    select JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0

`indices_or_keys`可以是零个或多个参数的列表，每个参数可以是字符串或整数。

-   String = 按成员名称访问JSON对象成员。
-   正整数 = 从头开始访问第n个成员/成员名称。
-   负整数 = 从末尾访问第n个成员/成员名称。

您可以使用整数来访问JSON数组和JSON对象。

例如：

    select JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
    select JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
    select JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
    select JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
    select JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'

## JSONLength(json\[, indices_or_keys\]…) {#jsonlengthjson-indices-or-keys}

返回JSON数组或JSON对象的长度。

如果该值不存在或类型错误，将返回`0`。

示例：

    select JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
    select JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2

## JSONType(json\[, indices_or_keys\]…) {#jsontypejson-indices-or-keys}

返回JSON值的类型。

如果该值不存在，将返回`Null`。

示例：

    select JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
    select JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
    select JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'

## JSONExtractUInt(json\[, indices_or_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices_or_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices_or_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices_or_keys\]…) {#jsonextractbooljson-indices-or-keys}

解析JSON并提取值。这些函数类似于`visitParam*`函数。

如果该值不存在或类型错误，将返回`0`。

示例:

    select JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
    select JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
    select JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300

## JSONExtractString(json\[, indices_or_keys\]…) {#jsonextractstringjson-indices-or-keys}

解析JSON并提取字符串。此函数类似于`visitParamExtractString`函数。

如果该值不存在或类型错误，则返回空字符串。

该值未转义。如果unescaping失败，则返回一个空字符串。

示例:

    select JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
    select JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
    select JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
    select JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
    select JSONExtractString('{"abc":"hello}', 'abc') = ''

## JSONExtract(json\[, indices_or_keys…\], Return_type) {#jsonextractjson-indices-or-keys-return-type}

解析JSON并提取给定ClickHouse数据类型的值。

这是以前的`JSONExtract<type>函数的变体。 这意味着`JSONExtract(…, ‘String’)`返回与`JSONExtractString()`返回完全相同。`JSONExtract(…, ‘Float64’)`返回于`JSONExtractFloat()\`返回完全相同。

示例:

    SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
    SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
    SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
    SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
    SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
    SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
    SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'

## JSONExtractKeysAndValues(json\[, indices_or_keys…\], Value_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

从JSON中解析键值对，其中值是给定的ClickHouse数据类型。

示例：

    SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];

## JSONExtractRaw(json\[, indices_or_keys\]…) {#jsonextractrawjson-indices-or-keys}

返回JSON的部分。

如果部件不存在或类型错误，将返回空字符串。

示例:

    select JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
