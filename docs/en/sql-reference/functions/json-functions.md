---
slug: /en/sql-reference/functions/json-functions
sidebar_position: 105
sidebar_label: JSON
---

There are two sets of functions to parse JSON.
   - `simpleJSON*` (`visitParam*`) is made to parse a special very limited subset of a JSON, but these functions are extremely fast.
   - `JSONExtract*` is made to parse normal JSON.

# simpleJSON/visitParam functions

ClickHouse has special functions for working with simplified JSON. All these JSON functions are based on strong assumptions about what the JSON can be, but they try to do as little as possible to get the job done.

The following assumptions are made:

1.  The field name (function argument) must be a constant.
2.  The field name is somehow canonically encoded in JSON. For example: `simpleJSONHas('{"abc":"def"}', 'abc') = 1`, but `simpleJSONHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Fields are searched for on any nesting level, indiscriminately. If there are multiple matching fields, the first occurrence is used.
4.  The JSON does not have space characters outside of string literals.

## simpleJSONHas

Checks whether there is a field named `field_name`.  The result is `UInt8`.

**Syntax**

```sql
simpleJSONHas(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns `1` if the field exists, `0` otherwise.

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONHas(json, 'foo') FROM jsons;
SELECT simpleJSONHas(json, 'bar') FROM jsons;
```

```response
1
0
```
## simpleJSONExtractUInt

Parses `UInt64` from the value of the field named `field_name`. If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns `0`.

**Syntax**

```sql
simpleJSONExtractUInt(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns the number parsed from the field if the field exists and contains a number, `0` otherwise.

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"4e3"}');
INSERT INTO jsons VALUES ('{"foo":3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractUInt(json, 'foo') FROM jsons ORDER BY json;
```

```response
0
4
0
3
5
```

## simpleJSONExtractInt

Parses `Int64` from the value of the field named `field_name`. If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns `0`.

**Syntax**

```sql
simpleJSONExtractInt(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns the number parsed from the field if the field exists and contains a number, `0` otherwise.

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractInt(json, 'foo') FROM jsons ORDER BY json;
```

```response
0
-4
0
-3
5
```

## simpleJSONExtractFloat

Parses `Float64` from the value of the field named `field_name`. If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns `0`.

**Syntax**

```sql
simpleJSONExtractFloat(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns the number parsed from the field if the field exists and contains a number, `0` otherwise.

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractFloat(json, 'foo') FROM jsons ORDER BY json;
```

```response
0
-4000
0
-3.4
5
```

## simpleJSONExtractBool

Parses a true/false value from the value of the field named `field_name`. The result is `UInt8`.

**Syntax**

```sql
simpleJSONExtractBool(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns `1` if the value of the field is `true`, `0` otherwise. This means this function will return `0` including (and not only) in the following cases:
 - If the field doesn't exists.
 - If the field contains `true` as a string, e.g.: `{"field":"true"}`.
 - If the field contains `1` as a numerical value.

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":false,"bar":true}');
INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONExtractBool(json, 'bar') FROM jsons ORDER BY json;
SELECT simpleJSONExtractBool(json, 'foo') FROM jsons ORDER BY json;
```

```response
0
1
0
0
```

## simpleJSONExtractRaw

Returns the value of the field named `field_name` as a `String`, including separators.

**Syntax**

```sql
simpleJSONExtractRaw(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns the value of the field as a [`String`](../../sql-reference/data-types/string.md#string), including separators if the field exists, or an empty `String` otherwise.

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":{"def":[1,2,3]}}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractRaw(json, 'foo') FROM jsons ORDER BY json;
```

```response

"-4e3"
-3.4
5
{"def":[1,2,3]}
```

## simpleJSONExtractString

Parses `String` in double quotes from the value of the field named `field_name`.

**Syntax**

```sql
simpleJSONExtractString(json, field_name)
```

**Parameters**

- `json`: The JSON in which the field is searched for. [String](../../sql-reference/data-types/string.md#string)
- `field_name`: The name of the field to search for. [String literal](../syntax#string)

**Returned value**

It returns the value of a field as a [`String`](../../sql-reference/data-types/string.md#string), including separators. The value is unescaped. It returns an empty `String`: if the field doesn't contain a double quoted string, if unescaping fails or if the field doesn't exist.

**Implementation details**

There is currently no support for code points in the format `\uXXXX\uYYYY` that are not from the basic multilingual plane (they are converted to CESU-8 instead of UTF-8).

**Example**

Query:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"\\n\\u0000"}');
INSERT INTO jsons VALUES ('{"foo":"\\u263"}');
INSERT INTO jsons VALUES ('{"foo":"\\u263a"}');
INSERT INTO jsons VALUES ('{"foo":"hello}');

SELECT simpleJSONExtractString(json, 'foo') FROM jsons ORDER BY json;
```

```response
\n\0

☺

```

## visitParamHas

This function is [an alias of `simpleJSONHas`](./json-functions#simplejsonhas).

## visitParamExtractUInt

This function is [an alias of `simpleJSONExtractUInt`](./json-functions#simplejsonextractuint).

## visitParamExtractInt

This function is [an alias of `simpleJSONExtractInt`](./json-functions#simplejsonextractint).

## visitParamExtractFloat

This function is [an alias of `simpleJSONExtractFloat`](./json-functions#simplejsonextractfloat).

## visitParamExtractBool

This function is [an alias of `simpleJSONExtractBool`](./json-functions#simplejsonextractbool).

## visitParamExtractRaw

This function is [an alias of `simpleJSONExtractRaw`](./json-functions#simplejsonextractraw).

## visitParamExtractString

This function is [an alias of `simpleJSONExtractString`](./json-functions#simplejsonextractstring).

# JSONExtract functions

The following functions are based on [simdjson](https://github.com/lemire/simdjson) designed for more complex JSON parsing requirements.

## isValidJSON(json)

Checks that passed string is a valid json.

Examples:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices_or_keys\]…)

If the value exists in the JSON document, `1` will be returned.

If the value does not exist, `0` will be returned.

Examples:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` is a list of zero or more arguments each of them can be either string or integer.

- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

Minimum index of the element is 1. Thus the element 0 does not exist.

You may use integers to access both JSON arrays and JSON objects.

So, for example:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices_or_keys\]…)

Return the length of a JSON array or a JSON object.

If the value does not exist or has a wrong type, `0` will be returned.

Examples:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices_or_keys\]…)

Return the type of a JSON value.

If the value does not exist, `Null` will be returned.

Examples:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices_or_keys\]…)

## JSONExtractInt(json\[, indices_or_keys\]…)

## JSONExtractFloat(json\[, indices_or_keys\]…)

## JSONExtractBool(json\[, indices_or_keys\]…)

Parses a JSON and extract a value. These functions are similar to `visitParam` functions.

If the value does not exist or has a wrong type, `0` will be returned.

Examples:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices_or_keys\]…)

Parses a JSON and extract a string. This function is similar to `visitParamExtractString` functions.

If the value does not exist or has a wrong type, an empty string will be returned.

The value is unescaped. If unescaping failed, it returns an empty string.

Examples:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices_or_keys…\], Return_type)

Parses a JSON and extract a value of the given ClickHouse data type.

This is a generalization of the previous `JSONExtract<type>` functions.
This means
`JSONExtract(..., 'String')` returns exactly the same as `JSONExtractString()`,
`JSONExtract(..., 'Float64')` returns exactly the same as `JSONExtractFloat()`.

Examples:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": "world"}', 'Map(String, String)') = map('a',  'hello', 'b', 'world');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices_or_keys…\], Value_type)

Parses key-value pairs from a JSON where the values are of the given ClickHouse data type.

Example:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

## JSONExtractKeys

Parses a JSON string and extracts the keys.

**Syntax**

``` sql
JSONExtractKeys(json[, a, b, c...])
```

**Arguments**

- `json` — [String](../../sql-reference/data-types/string.md) with valid JSON.
- `a, b, c...` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [String](../../sql-reference/data-types/string.md) to get the field by the key or an [Integer](../../sql-reference/data-types/int-uint.md) to get the N-th field (indexed from 1, negative integers count from the end). If not set, the whole JSON is parsed as the top-level object. Optional parameter.

**Returned value**

Array with the keys of the JSON.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Example**

Query:

```sql
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}');
```

Result:

```
text
┌─JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}')─┐
│ ['a','b']                                                  │
└────────────────────────────────────────────────────────────┘
```

## JSONExtractRaw(json\[, indices_or_keys\]…)

Returns a part of JSON as unparsed string.

If the part does not exist or has a wrong type, an empty string will be returned.

Example:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]';
```

## JSONExtractArrayRaw(json\[, indices_or_keys…\])

Returns an array with elements of JSON array, each represented as unparsed string.

If the part does not exist or isn’t array, an empty array will be returned.

Example:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"'];
```

## JSONExtractKeysAndValuesRaw

Extracts raw data from a JSON object.

**Syntax**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**Arguments**

- `json` — [String](../../sql-reference/data-types/string.md) with valid JSON.
- `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [string](../../sql-reference/data-types/string.md) to get the field by the key or an [integer](../../sql-reference/data-types/int-uint.md) to get the N-th field (indexed from 1, negative integers count from the end). If not set, the whole JSON is parsed as the top-level object. Optional parameter.

**Returned values**

- Array with `('key', 'value')` tuples. Both tuple members are strings.
- Empty array if the requested object does not exist, or input JSON is invalid.

Type: [Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md), [String](../../sql-reference/data-types/string.md)).

**Examples**

Query:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}');
```

Result:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b');
```

Result:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c');
```

Result:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## JSON_EXISTS(json, path)

If the value exists in the JSON document, `1` will be returned.

If the value does not exist, `0` will be returned.

Examples:

``` sql
SELECT JSON_EXISTS('{"hello":1}', '$.hello');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.hello.world');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[0]');
```

:::note
Before version 21.11 the order of arguments was wrong, i.e. JSON_EXISTS(path, json)
:::

## JSON_QUERY(json, path)

Parses a JSON and extract a value as JSON array or JSON object.

If the value does not exist, an empty string will be returned.

Example:

``` sql
SELECT JSON_QUERY('{"hello":"world"}', '$.hello');
SELECT JSON_QUERY('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_QUERY('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_QUERY('{"hello":2}', '$.hello'));
```

Result:

``` text
["world"]
[0, 1, 4, 0, -1, -4]
[2]
String
```
:::note
Before version 21.11 the order of arguments was wrong, i.e. JSON_QUERY(path, json)
:::

## JSON_VALUE(json, path)

Parses a JSON and extract a value as JSON scalar.

If the value does not exist, an empty string will be returned by default, and by SET `function_json_value_return_type_allow_nullable` = `true`, `NULL` will be returned. If the value is complex type (such as: struct, array, map), an empty string will be returned by default, and by SET `function_json_value_return_type_allow_complex` = `true`, the complex value will be returned.

Example:

``` sql
SELECT JSON_VALUE('{"hello":"world"}', '$.hello');
SELECT JSON_VALUE('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_VALUE('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_VALUE('{"hello":2}', '$.hello'));
select JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true;
select JSON_VALUE('{"hello":{"world":"!"}}', '$.hello') settings function_json_value_return_type_allow_complex=true;
```

Result:

``` text
world
0
2
String
```

:::note
Before version 21.11 the order of arguments was wrong, i.e. JSON_VALUE(path, json)
:::

## toJSONString

Serializes a value to its JSON representation. Various data types and nested structures are supported.
64-bit [integers](../../sql-reference/data-types/int-uint.md) or bigger (like `UInt64` or `Int128`) are enclosed in quotes by default. [output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) controls this behavior.
Special values `NaN` and `inf` are replaced with `null`. Enable [output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals) setting to show them.
When serializing an [Enum](../../sql-reference/data-types/enum.md) value, the function outputs its name.

**Syntax**

``` sql
toJSONString(value)
```

**Arguments**

- `value` — Value to serialize. Value may be of any data type.

**Returned value**

- JSON representation of the value.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

The first example shows serialization of a [Map](../../sql-reference/data-types/map.md).
The second example shows some special values wrapped into a [Tuple](../../sql-reference/data-types/tuple.md).

Query:

``` sql
SELECT toJSONString(map('key1', 1, 'key2', 2));
SELECT toJSONString(tuple(1.25, NULL, NaN, +inf, -inf, [])) SETTINGS output_format_json_quote_denormals = 1;
```

Result:

``` text
{"key1":1,"key2":2}
[1.25,null,"nan","inf","-inf",[]]
```

**See Also**

- [output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers)
- [output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals)


## JSONArrayLength

Returns the number of elements in the outermost JSON array. The function returns NULL if input JSON string is invalid.

**Syntax**

``` sql
JSONArrayLength(json)
```

Alias: `JSON_ARRAY_LENGTH(json)`.

**Arguments**

- `json` — [String](../../sql-reference/data-types/string.md) with valid JSON.

**Returned value**

- If `json` is a valid JSON array string, returns the number of array elements, otherwise returns NULL.

Type: [Nullable(UInt64)](../../sql-reference/data-types/int-uint.md).

**Example**

``` sql
SELECT
    JSONArrayLength(''),
    JSONArrayLength('[1,2,3]')

┌─JSONArrayLength('')─┬─JSONArrayLength('[1,2,3]')─┐
│                ᴺᵁᴸᴸ │                          3 │
└─────────────────────┴────────────────────────────┘
```


## jsonMergePatch

Returns the merged JSON object string which is formed by merging multiple JSON objects.

**Syntax**

``` sql
jsonMergePatch(json1, json2, ...)
```

**Arguments**

- `json` — [String](../../sql-reference/data-types/string.md) with valid JSON.

**Returned value**

- If JSON object strings are valid, return the merged JSON object string.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT jsonMergePatch('{"a":1}', '{"name": "joey"}', '{"name": "tom"}', '{"name": "zoey"}') AS res

┌─res───────────────────┐
│ {"a":1,"name":"zoey"} │
└───────────────────────┘
```
