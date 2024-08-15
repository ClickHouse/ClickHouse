---
slug: /en/sql-reference/functions/json-functions
sidebar_position: 105
sidebar_label: JSON
---

There are two sets of functions to parse JSON:
   - [`simpleJSON*` (`visitParam*`)](#simplejson-visitparam-functions) which is made for parsing a limited subset of JSON extremely fast.
   - [`JSONExtract*`](#jsonextract-functions) which is made for parsing ordinary JSON.

## simpleJSON (visitParam) functions

ClickHouse has special functions for working with simplified JSON. All these JSON functions are based on strong assumptions about what the JSON can be. They try to do as little as possible to get the job done as quickly as possible.

The following assumptions are made:

1.  The field name (function argument) must be a constant.
2.  The field name is somehow canonically encoded in JSON. For example: `simpleJSONHas('{"abc":"def"}', 'abc') = 1`, but `simpleJSONHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Fields are searched for on any nesting level, indiscriminately. If there are multiple matching fields, the first occurrence is used.
4.  The JSON does not have space characters outside of string literals.

### simpleJSONHas

Checks whether there is a field named `field_name`.  The result is `UInt8`.

**Syntax**

```sql
simpleJSONHas(json, field_name)
```

Alias: `visitParamHas`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

**Returned value**

- Returns `1` if the field exists, `0` otherwise. [UInt8](../data-types/int-uint.md).

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

Result:

```response
1
0
```
### simpleJSONExtractUInt

Parses `UInt64` from the value of the field named `field_name`. If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns `0`.

**Syntax**

```sql
simpleJSONExtractUInt(json, field_name)
```

Alias: `visitParamExtractUInt`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

**Returned value**

- Returns the number parsed from the field if the field exists and contains a number, `0` otherwise. [UInt64](../data-types/int-uint.md).

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

Result:

```response
0
4
0
3
5
```

### simpleJSONExtractInt

Parses `Int64` from the value of the field named `field_name`. If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns `0`.

**Syntax**

```sql
simpleJSONExtractInt(json, field_name)
```

Alias: `visitParamExtractInt`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

**Returned value**

- Returns the number parsed from the field if the field exists and contains a number, `0` otherwise. [Int64](../data-types/int-uint.md).

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

Result:

```response
0
-4
0
-3
5
```

### simpleJSONExtractFloat

Parses `Float64` from the value of the field named `field_name`. If this is a string field, it tries to parse a number from the beginning of the string. If the field does not exist, or it exists but does not contain a number, it returns `0`.

**Syntax**

```sql
simpleJSONExtractFloat(json, field_name)
```

Alias: `visitParamExtractFloat`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

**Returned value**

- Returns the number parsed from the field if the field exists and contains a number, `0` otherwise. [Float64](../data-types/float.md/#float32-float64).

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

Result:

```response
0
-4000
0
-3.4
5
```

### simpleJSONExtractBool

Parses a true/false value from the value of the field named `field_name`. The result is `UInt8`.

**Syntax**

```sql
simpleJSONExtractBool(json, field_name)
```

Alias: `visitParamExtractBool`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

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

Result:

```response
0
1
0
0
```

### simpleJSONExtractRaw

Returns the value of the field named `field_name` as a `String`, including separators.

**Syntax**

```sql
simpleJSONExtractRaw(json, field_name)
```

Alias: `visitParamExtractRaw`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

**Returned value**

- Returns the value of the field as a string, including separators if the field exists, or an empty string otherwise. [`String`](../data-types/string.md#string)

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

Result:

```response

"-4e3"
-3.4
5
{"def":[1,2,3]}
```

### simpleJSONExtractString

Parses `String` in double quotes from the value of the field named `field_name`.

**Syntax**

```sql
simpleJSONExtractString(json, field_name)
```

Alias: `visitParamExtractString`.

**Parameters**

- `json` — The JSON in which the field is searched for. [String](../data-types/string.md#string)
- `field_name` — The name of the field to search for. [String literal](../syntax#string)

**Returned value**

- Returns the unescaped value of a field as a string, including separators. An empty string is returned if the field doesn't contain a double quoted string, if unescaping fails or if the field doesn't exist. [String](../data-types/string.md).

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

Result:

```response
\n\0

☺

```

## JSONExtract functions

The following functions are based on [simdjson](https://github.com/lemire/simdjson), and designed for more complex JSON parsing requirements.

### isValidJSON

Checks that passed string is valid JSON.

**Syntax**

```sql
isValidJSON(json)
```

**Examples**

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

### JSONHas

If the value exists in the JSON document, `1` will be returned. If the value does not exist, `0` will be returned.

**Syntax**

```sql
JSONHas(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns `1` if the value exists in `json`, otherwise `0`. [UInt8](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

The minimum index of the element is 1. Thus the element 0 does not exist. You may use integers to access both JSON arrays and JSON objects. For example:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

### JSONLength

Return the length of a JSON array or a JSON object. If the value does not exist or has the wrong type, `0` will be returned.

**Syntax**

```sql
JSONLength(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns the length of the JSON array or JSON object. Returns `0` if the value does not exist or has the wrong type. [UInt64](../data-types/int-uint.md).

**Examples**

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

### JSONType

Return the type of a JSON value. If the value does not exist, `Null` will be returned.

**Syntax**

```sql
JSONType(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns the type of a JSON value as a string, otherwise if the value doesn't exists it returns `Null`. [String](../data-types/string.md).

**Examples**

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

### JSONExtractUInt

Parses JSON and extracts a value of UInt type.

**Syntax**

```sql
JSONExtractUInt(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns a UInt value if it exists, otherwise it returns `Null`. [UInt64](../data-types/string.md).

**Examples**

Query:

``` sql
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) as x, toTypeName(x);
```

Result:

```response
┌───x─┬─toTypeName(x)─┐
│ 300 │ UInt64        │
└─────┴───────────────┘
```

### JSONExtractInt

Parses JSON and extracts a value of Int type.

**Syntax**

```sql
JSONExtractInt(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns an Int value if it exists, otherwise it returns `Null`. [Int64](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) as x, toTypeName(x);
```

Result:

```response
┌───x─┬─toTypeName(x)─┐
│ 300 │ Int64         │
└─────┴───────────────┘
```

### JSONExtractFloat

Parses JSON and extracts a value of Int type.

**Syntax**

```sql
JSONExtractFloat(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns an Float value if it exists, otherwise it returns `Null`. [Float64](../data-types/float.md).

**Examples**

Query:

``` sql
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) as x, toTypeName(x);
```

Result:

```response
┌───x─┬─toTypeName(x)─┐
│ 200 │ Float64       │
└─────┴───────────────┘
```

### JSONExtractBool

Parses JSON and extracts a boolean value. If the value does not exist or has a wrong type, `0` will be returned.

**Syntax**

```sql
JSONExtractBool(json\[, indices_or_keys\]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns a Boolean value if it exists, otherwise it returns `0`. [Bool](../data-types/boolean.md).

**Example**

Query:

``` sql
SELECT JSONExtractBool('{"passed": true}', 'passed');
```

Result:

```response
┌─JSONExtractBool('{"passed": true}', 'passed')─┐
│                                             1 │
└───────────────────────────────────────────────┘
```

### JSONExtractString

Parses JSON and extracts a string. This function is similar to [`visitParamExtractString`](#simplejsonextractstring) functions. If the value does not exist or has a wrong type, an empty string will be returned.

**Syntax**

```sql
JSONExtractString(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns an unescaped string from `json`. If unescaping failed, if the value does not exist or if it has a wrong type then it returns an empty string. [String](../data-types/string.md).

**Examples**

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

### JSONExtract

Parses JSON and extracts a value of the given ClickHouse data type. This function is a generalized version of the previous `JSONExtract<type>` functions. Meaning:

`JSONExtract(..., 'String')` returns exactly the same as `JSONExtractString()`,
`JSONExtract(..., 'Float64')` returns exactly the same as `JSONExtractFloat()`.

**Syntax**

```sql
JSONExtract(json [, indices_or_keys...], return_type)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).
- `return_type` — A string specifying the type of the value to extract. [String](../data-types/string.md). 

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns a value if it exists of the specified return type, otherwise it returns `0`, `Null`, or an empty-string depending on the specified return type. [UInt64](../data-types/int-uint.md), [Int64](../data-types/int-uint.md), [Float64](../data-types/float.md), [Bool](../data-types/boolean.md) or [String](../data-types/string.md).

**Examples**

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

### JSONExtractKeysAndValues

Parses key-value pairs from JSON where the values are of the given ClickHouse data type.

**Syntax**

```sql
JSONExtractKeysAndValues(json [, indices_or_keys...], value_type)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).
- `value_type` — A string specifying the type of the value to extract. [String](../data-types/string.md). 

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns an array of parsed key-value pairs. [Array](../data-types/array.md)([Tuple](../data-types/tuple.md)(`value_type`)). 

**Example**

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

### JSONExtractKeys

Parses a JSON string and extracts the keys.

**Syntax**

``` sql
JSONExtractKeys(json[, a, b, c...])
```

**Parameters**

- `json` — [String](../data-types/string.md) with valid JSON.
- `a, b, c...` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [String](../data-types/string.md) to get the field by the key or an [Integer](../data-types/int-uint.md) to get the N-th field (indexed from 1, negative integers count from the end). If not set, the whole JSON is parsed as the top-level object. Optional parameter.

**Returned value**

- Returns an array with the keys of the JSON. [Array](../data-types/array.md)([String](../data-types/string.md)).

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

### JSONExtractRaw

Returns part of the JSON as an unparsed string. If the part does not exist or has the wrong type, an empty string will be returned.

**Syntax**

```sql
JSONExtractRaw(json [, indices_or_keys]...)
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns part of the JSON as an unparsed string. If the part does not exist or has the wrong type, an empty string is returned. [String](../data-types/string.md).

**Example**

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]';
```

### JSONExtractArrayRaw

Returns an array with elements of JSON array, each represented as unparsed string. If the part does not exist or isn’t an array, then an empty array will be returned.

**Syntax**

```sql
JSONExtractArrayRaw(json [, indices_or_keys...])
```

**Parameters**

- `json` — JSON string to parse. [String](../data-types/string.md).
- `indices_or_keys` — A list of zero or more arguments, each of which can be either string or integer. [String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` type:
- String = access object member by key.
- Positive integer = access the n-th member/key from the beginning.
- Negative integer = access the n-th member/key from the end.

**Returned value**

- Returns an array with elements of JSON array, each represented as unparsed string. Otherwise, an empty array is returned if the part does not exist or is not an array. [Array](../data-types/array.md)([String](../data-types/string.md)).

**Example**

```sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"'];
```

### JSONExtractKeysAndValuesRaw

Extracts raw data from a JSON object.

**Syntax**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**Arguments**

- `json` — [String](../data-types/string.md) with valid JSON.
- `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [string](../data-types/string.md) to get the field by the key or an [integer](../data-types/int-uint.md) to get the N-th field (indexed from 1, negative integers count from the end). If not set, the whole JSON is parsed as the top-level object. Optional parameter.

**Returned values**

- Array with `('key', 'value')` tuples. Both tuple members are strings. [Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), [String](../data-types/string.md)).
- Empty array if the requested object does not exist, or input JSON is invalid. [Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), [String](../data-types/string.md)).

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

### JSON_EXISTS

If the value exists in the JSON document, `1` will be returned. If the value does not exist, `0` will be returned.

**Syntax**

```sql
JSON_EXISTS(json, path)
```

**Parameters**

- `json` — A string with valid JSON. [String](../data-types/string.md). 
- `path` — A string representing the path. [String](../data-types/string.md).

:::note
Before version 21.11 the order of arguments was wrong, i.e. JSON_EXISTS(path, json)
:::

**Returned value**

- Returns `1` if the value exists in the JSON document, otherwise `0`.

**Examples**

``` sql
SELECT JSON_EXISTS('{"hello":1}', '$.hello');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.hello.world');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[0]');
```

### JSON_QUERY

Parses a JSON and extract a value as a JSON array or JSON object. If the value does not exist, an empty string will be returned.

**Syntax**

```sql
JSON_QUERY(json, path)
```

**Parameters**

- `json` — A string with valid JSON. [String](../data-types/string.md). 
- `path` — A string representing the path. [String](../data-types/string.md).

:::note
Before version 21.11 the order of arguments was wrong, i.e. JSON_EXISTS(path, json)
:::

**Returned value**

- Returns the extracted value as a JSON array or JSON object. Otherwise it returns an empty string if the value does not exist. [String](../data-types/string.md).

**Example**

Query:

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

### JSON_VALUE

Parses a JSON and extract a value as a JSON scalar. If the value does not exist, an empty string will be returned by default.

This function is controlled by the following settings:

- by SET `function_json_value_return_type_allow_nullable` = `true`, `NULL` will be returned. If the value is complex type (such as: struct, array, map), an empty string will be returned by default.
- by SET `function_json_value_return_type_allow_complex` = `true`, the complex value will be returned.

**Syntax**

```sql
JSON_VALUE(json, path)
```

**Parameters**

- `json` — A string with valid JSON. [String](../data-types/string.md). 
- `path` — A string representing the path. [String](../data-types/string.md).

:::note
Before version 21.11 the order of arguments was wrong, i.e. JSON_EXISTS(path, json)
:::

**Returned value**

- Returns the extracted value as a JSON scalar if it exists, otherwise an empty string is returned. [String](../data-types/string.md).

**Example**

Query:

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

### toJSONString

Serializes a value to its JSON representation. Various data types and nested structures are supported.
64-bit [integers](../data-types/int-uint.md) or bigger (like `UInt64` or `Int128`) are enclosed in quotes by default. [output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) controls this behavior.
Special values `NaN` and `inf` are replaced with `null`. Enable [output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals) setting to show them.
When serializing an [Enum](../data-types/enum.md) value, the function outputs its name.

**Syntax**

``` sql
toJSONString(value)
```

**Arguments**

- `value` — Value to serialize. Value may be of any data type.

**Returned value**

- JSON representation of the value. [String](../data-types/string.md).

**Example**

The first example shows serialization of a [Map](../data-types/map.md).
The second example shows some special values wrapped into a [Tuple](../data-types/tuple.md).

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


### JSONArrayLength

Returns the number of elements in the outermost JSON array. The function returns NULL if input JSON string is invalid.

**Syntax**

``` sql
JSONArrayLength(json)
```

Alias: `JSON_ARRAY_LENGTH(json)`.

**Arguments**

- `json` — [String](../data-types/string.md) with valid JSON.

**Returned value**

- If `json` is a valid JSON array string, returns the number of array elements, otherwise returns NULL. [Nullable(UInt64)](../data-types/int-uint.md).

**Example**

``` sql
SELECT
    JSONArrayLength(''),
    JSONArrayLength('[1,2,3]')

┌─JSONArrayLength('')─┬─JSONArrayLength('[1,2,3]')─┐
│                ᴺᵁᴸᴸ │                          3 │
└─────────────────────┴────────────────────────────┘
```


### jsonMergePatch

Returns the merged JSON object string which is formed by merging multiple JSON objects.

**Syntax**

``` sql
jsonMergePatch(json1, json2, ...)
```

**Arguments**

- `json` — [String](../data-types/string.md) with valid JSON.

**Returned value**

- If JSON object strings are valid, return the merged JSON object string. [String](../data-types/string.md).

**Example**

``` sql
SELECT jsonMergePatch('{"a":1}', '{"name": "joey"}', '{"name": "tom"}', '{"name": "zoey"}') AS res

┌─res───────────────────┐
│ {"a":1,"name":"zoey"} │
└───────────────────────┘
```

### JSONAllPaths

Returns the list of all paths stored in each row in [JSON](../data-types/newjson.md) column.

**Syntax**

``` sql
JSONAllPaths(json)
```

**Arguments**

- `json` — [JSON](../data-types/newjson.md).

**Returned value**

- An array of paths. [Array(String)](../data-types/array.md).

**Example**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONAllPaths(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a":"42"}                           │ ['a']              │
│ {"b":"Hello"}                        │ ['b']              │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ ['a','c']          │
└──────────────────────────────────────┴────────────────────┘
```

### JSONAllPathsWithTypes

Returns the map of all paths and their data types stored in each row in [JSON](../data-types/newjson.md) column.

**Syntax**

``` sql
JSONAllPathsWithTypes(json)
```

**Arguments**

- `json` — [JSON](../data-types/newjson.md).

**Returned value**

- An array of paths. [Map(String, String)](../data-types/array.md).

**Example**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONAllPathsWithTypes(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONAllPathsWithTypes(json)───────────────┐
│ {"a":"42"}                           │ {'a':'Int64'}                             │
│ {"b":"Hello"}                        │ {'b':'String'}                            │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ {'a':'Array(Nullable(Int64))','c':'Date'} │
└──────────────────────────────────────┴───────────────────────────────────────────┘
```

### JSONDynamicPaths

Returns the list of dynamic paths that are stored as separate subcolumns in [JSON](../data-types/newjson.md) column.

**Syntax**

``` sql
JSONDynamicPaths(json)
```

**Arguments**

- `json` — [JSON](../data-types/newjson.md).

**Returned value**

- An array of paths. [Array(String)](../data-types/array.md).

**Example**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONDynamicPaths(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONDynamicPaths(json)─┐
| {"a":"42"}                           │ ['a']                  │
│ {"b":"Hello"}                        │ []                     │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ ['a']                  │
└──────────────────────────────────────┴────────────────────────┘
```

### JSONDynamicPathsWithTypes

Returns the map of dynamic paths that are stored as separate subcolumns and their types in each row in [JSON](../data-types/newjson.md) column.

**Syntax**

``` sql
JSONAllPathsWithTypes(json)
```

**Arguments**

- `json` — [JSON](../data-types/newjson.md).

**Returned value**

- An array of paths. [Map(String, String)](../data-types/array.md).

**Example**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONDynamicPathsWithTypes(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONDynamicPathsWithTypes(json)─┐
│ {"a":"42"}                           │ {'a':'Int64'}                   │
│ {"b":"Hello"}                        │ {}                              │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ {'a':'Array(Nullable(Int64))'}  │
└──────────────────────────────────────┴─────────────────────────────────┘
```

### JSONSharedDataPaths

Returns the list of paths that are stored in shared data structure in [JSON](../data-types/newjson.md) column.

**Syntax**

``` sql
JSONSharedDataPaths(json)
```

**Arguments**

- `json` — [JSON](../data-types/newjson.md).

**Returned value**

- An array of paths. [Array(String)](../data-types/array.md).

**Example**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONSharedDataPaths(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONSharedDataPaths(json)─┐
│ {"a":"42"}                           │ []                        │
│ {"b":"Hello"}                        │ ['b']                     │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ ['c']                     │
└──────────────────────────────────────┴───────────────────────────┘
```

### JSONSharedDataPathsWithTypes

Returns the map of paths that are stored in shared data structure and their types in each row in [JSON](../data-types/newjson.md) column.

**Syntax**

``` sql
JSONSharedDataPathsWithTypes(json)
```

**Arguments**

- `json` — [JSON](../data-types/newjson.md).

**Returned value**

- An array of paths. [Map(String, String)](../data-types/array.md).

**Example**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONSharedDataPathsWithTypes(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONSharedDataPathsWithTypes(json)─┐
│ {"a":"42"}                           │ {}                                 │
│ {"b":"Hello"}                        │ {'b':'String'}                     │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ {'c':'Date'}                       │
└──────────────────────────────────────┴────────────────────────────────────┘
```
