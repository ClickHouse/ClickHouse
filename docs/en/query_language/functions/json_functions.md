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


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/json_functions/) <!--hide-->

The following functions are based on [simdjson](https://github.com/lemire/simdjson) designed for more complex JSON parsing requirements. The assumption 2 mentioned above still applies.

## jsonHas(params[, accessors]...)

If the value exists in the JSON document, `1` will be returned.

If the value does not exist, `null` will be returned.

Examples:

```
select jsonHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
select jsonHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = null
```

An accessor can be either a string, a positive integer or a negative integer.

* String = access object member by key.
* Positive integer = access the n-th member/key from the beginning.
* Negative integer = access the n-th member/key from the end.

You may use integers to access both JSON arrays and JSON objects. JSON objects are accessed as an array with the `[key, value, key, value, ...]` layout.

So, for example:

```
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'hello'
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'b'
```

## jsonLength(params[, accessors]...)

Return the length of a JSON array or a JSON object. For JSON objects, both keys and values are included.

If the value does not exist or has a wrong type, `null` will be returned.

Examples:

```
select jsonLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
select jsonLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 4
```

The usage of accessors is the same as above.

## jsonType(params[, accessors]...)

Return the type of a JSON value.

If the value does not exist, `null` will be returned.

Examples:

```
select jsonType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
select jsonType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
select jsonType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

The usage of accessors is the same as above.

## jsonExtractUInt(params[, accessors]...)
## jsonExtractInt(params[, accessors]...)
## jsonExtractFloat(params[, accessors]...)
## jsonExtractBool(params[, accessors]...)
## jsonExtractString(params[, accessors]...)

Parse data from JSON values which is similar to `visitParam` functions.

If the value does not exist or has a wrong type, `null` will be returned.

Examples:

```
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
select jsonExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
select jsonExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
select jsonExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

The usage of accessors is the same as above.

## jsonExtract(params, type[, accessors]...)

Parse data from JSON values with a given ClickHouse data type.

If the value does not exist or has a wrong type, `null` will be returned.

Examples:

```
select jsonExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Int8', 'b', 1) = -100
select jsonExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, String, String, Array(Float64))') = ('a', 'hello', 'b', [-100.0, 200.0, 300.0])
```

The usage of accessors is the same as above.
