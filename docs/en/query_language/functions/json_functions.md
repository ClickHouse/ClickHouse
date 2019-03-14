# Functions for working with JSON

In Yandex.Metrica, JSON is transmitted by users as session parameters. There are some special functions for working with this JSON. (Although in most of the cases, the JSONs are additionally pre-processed, and the resulting values are put in separate columns in their processed format.) All these functions are based on strong assumptions about what the JSON can be, but they try to do as little as possible to get the job done.

The following assumptions are made:

1. The field name (function argument) must be a constant.
2. The field name is somehow canonically encoded in JSON. For example: `visitParamHas('{"abc":"def"}', 'abc') = 1`, but `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3. Fields are searched for on any nesting level, indiscriminately. If there are multiple matching fields, the first occurrence is used.
4. The JSON doesn't have space characters outside of string literals.

## visitParamHas(params, name)

Checks whether there is a field with the 'name' name.

## visitParamExtractUInt(params, name)

Parses UInt64 from the value of the field named 'name'. If this is a string field, it tries to parse a number from the beginning of the string. If the field doesn't exist, or it exists but doesn't contain a number, it returns 0.

## visitParamExtractInt(params, name)

The same as for Int64.

## visitParamExtractFloat(params, name)

The same as for Float64.

## visitParamExtractBool(params, name)

Parses a true/false value. The result is UInt8.

## visitParamExtractRaw(params, name)

Returns the value of a field, including separators.

Examples:

```
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString(params, name)

Parses the string in double quotes. The value is unescaped. If unescaping failed, it returns an empty string.

Examples:

```
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

There is currently no support for code points in the format `\uXXXX\uYYYY` that are not from the basic multilingual plane (they are converted to CESU-8 instead of UTF-8).

# New style functions for working with JSON

## Motivation
Storing JSON in a String column is fundamentally against the columnstore nature of Clickhouse 
and is almost always a wrong choice leading to performance problems. On the other hand, the maximum level 
of nested structures is limited to 2, and modelling 1 to M relationships using several relational tables
is also not always possible in an efficient way.

Another use case for JSONs in String columns could be a generic storage system receiving
JSON messages in unspecified and persisting them; further data processing could be done by
parsing the JSON using DEFAULT columns and materialized VIEWS.

The new style JSON functions aim to solve some limitations of the existing JSON functions. Use them, if you 
- must extract JSON arrays,
- must perform extraction of nested structures,
- or must extract several matching keys.

All format limitations concerning JSON that exist in the visitParams* functions
still exist in the new style functions, eg. you cannot have a space between property name and the colon, 
there is no support  for code points in the format \uXXXX\uYYYY, etc.
 
## jsonAny(jsonString, jsonPath)
 
jsonString must be a canonical JSON. 
 
jsonPath is either the name of the property to find, or a path of properties separated by dots. For
example,

```
jsonAny('{"myparam":{"nested": "value"},"other":123}', 'myparam.nested') = 'value'
```
The function always returns a `String`. Use the Clickhouse type conversion functions to get the
data type you need, for example

```
 toUInt8(jsonAny('{"myparam":{"nested": 1},"other":123}', 'other')) = 123 --UInt8
```

Because the function always returns Strings, you can also use it for nested function calls. For example,

```
jsonAny(jsonAny('{"myparam":{"nested": "value"},"other":123}', 'myparam'), 'nested') = 'value'
```

This function is not suitable if you need to extract an array - use `jsonAnyArray` instead.

 
## jsonAnyArray(jsonString, jsonPath)
 
This function works similarly like jsonAny, but always returns `Array(String)`, and therefore is suitable
for array extraction. If there is a scalar value under the jsonPath, an array with it as a single element
will be returned.
 
Examples:

``` 
jsonAnyArray('{"myparam":[100,101,102],"other":123}', 'myparam') = ['100', '101', '102']
jsonAnyArray('{"myparam":{"nested": "value"},"other":123}', 'other') = ['123']
jsonAnyArray('{"myparam":{"nested": "value"},"other":123}', 'myparam.nested') = ['value']
```

If you need to extract arrays of the types other than Strings, you can convert them using the Clickhouse
type conversion functions:

```
arrayMap(x -> toInt8(x), jsonsAny('{"myparam":[100,101,102],"other":123}', 'myparam')) = [100, 101, 102] --Array(Int8)
```

## jsonAll(jsonString, jsonPath)

Similar to jsonAny, but returns all matches of jsonPath in the jsonAll as `Array(String)`

Examples: 

```
jsonAll('{"myparam":[{"nested": "value1"},{"nested": "value2"},{"nested": "value3"}]}', 'myparam.nested') = ['value1', 'value2', 'value3']

jsonAll('{"parent": [{"child": [{"A": 1}, {"A": 2}]},{"child": [{"A": 3}]}]}', 'A') = ['1','2','3']
```


## jsonAllArrays(jsonString, jsonPath)

Similar to jsonAll, but expects to match several arrays and therefore returns all matches of jsonPath 
in the jsonAll as `Array(Array(String))`

Example:

```
jsonAllArrays('{"myparam": [{"A": 1, "B": [17,18,19]}, {"A": 2, "B": [27,28,29]}, {"A": 3, "B": [37,38,39]}]}', 'myparam.B') = [['17','18','19'],['27','28','29'],['37','38','39']]
```

## jsonCount(jsonString, jsonPath)

Counts the number of occurences of jsonPath in jsonString. For example:

```
jsonCount('{"myparam": 5}', 'myparam') = 1
jsonCount('{"myparam": 5}', 'notthere') = 0
jsonCount('{"myparam": [{"A": 1, "B": [17,18,19]}, {"A": 2, "B": [27,28,29]}, {"A": 3, "B": [37,38,39]}]}', 'myparam.B') = 3
```


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/json_functions/) <!--hide-->
