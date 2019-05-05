# JSON函数

在Yandex.Metrica中，用户使用JSON作为访问参数。为了处理这些JSON，实现了一些函数。（尽管在大多数情况下，JSON是预先进行额外处理的，并将结果值放在单独的列中。）所有的这些函数都进行了尽可能的假设。以使函数能够尽快的完成工作。

我们对JSON格式做了如下假设：

1. 字段名称（函数的参数）必须使常量。
2. 字段名称必须使用规范的编码。例如：`visitParamHas('{"abc":"def"}', 'abc') = 1`，但是 `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3. 函数可以随意的在多层嵌套结构下查找字段。如果存在多个匹配字段，则返回第一个匹配字段。
4. JSON除字符串文本外不存在空格字符。

## visitParamHas(params, name)

检查是否存在“name”名称的字段

## visitParamExtractUInt(params, name)

将名为“name”的字段的值解析成UInt64。如果这是一个字符串字段，函数将尝试从字符串的开头解析一个数字。如果该字段不存在，或无法从它中解析到数字，则返回0。

## visitParamExtractInt(params, name)

与visitParamExtractUInt相同，但返回Int64。

## visitParamExtractFloat(params, name)

与visitParamExtractUInt相同，但返回Float64。

## visitParamExtractBool(params, name)

解析true/false值。其结果是UInt8类型的。

## visitParamExtractRaw(params, name)

返回字段的值，包含空格符。

示例:

```
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString(params, name)

使用双引号解析字符串。这个值没有进行转义。如果转义失败，它将返回一个空白字符串。

示例:

```
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

目前不支持`\uXXXX\uYYYY`这些字符编码，这些编码不在基本多文种平面中（它们被转化为CESU-8而不是UTF-8）。


[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/json_functions/) <!--hide-->
