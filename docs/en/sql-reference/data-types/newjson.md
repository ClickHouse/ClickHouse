---
slug: /en/sql-reference/data-types/newjson
sidebar_position: 63
sidebar_label: JSON
keywords: [json, data type]
---

# JSON Data Type

Stores JavaScript Object Notation (JSON) documents in a single column.

:::note
This feature is experimental and is not production-ready. If you need to work with JSON documents, consider using [this guide](/docs/en/integrations/data-formats/json/overview) instead.
If you want to use JSON type, set `allow_experimental_json_type = 1`. 
:::

To declare a column of `JSON` type, use the following syntax:

``` sql
<column_name> JSON(max_dynamic_paths=N, max_dynamic_types=M, some.path TypeName, SKIP path.to.skip, SKIP REGEXP 'paths_regexp')
```
Where:
- `max_dynamic_paths` is an optional parameter indicating how many paths can be stored separately as subcolumns across single block of data that is stored separately (for example across single data part for MergeTree table). If this limit is exceeded, all other paths will be stored together in a single structure. Default value of `max_dynamic_paths` is `1024`.
- `max_dynamic_types` is an optional parameter between `1` and `255` indicating how many different data types can be stored inside a single path column with type `Dynamic` across single block of data that is stored separately (for example across single data part for MergeTree table). If this limit is exceeded, all new types will be converted to type `String`. Default value of `max_dynamic_types` is `32`.
- `some.path TypeName` is an optional type hint for particular path in the JSON. Such paths will be always stored as subcolumns with specified type.
- `SKIP path.to.skip` is an optional hint for particular path that should be skipped during JSON parsing. Such paths will never be stored in the JSON column. If specified path is a nested JSON object, the whole nested object will be skipped.
- `SKIP REGEXP 'path_regexp'` is an optional hint with a regular expression that is used to skip paths during JSON parsing. All paths that match this regular expression will never be stored in the JSON column.

## Creating JSON

Using `JSON` type in table column definition:

```sql
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":[1,2,3]}        │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":[4,5,6]}        │
└───────────────────────────────────┘
```

Using CAST from 'String':

```sql
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON as json;
```

```text
┌─json───────────────────────────────────────────┐
│ {"a":{"b":42},"c":[1,2,3],"d":"Hello, World!"} │
└────────────────────────────────────────────────┘
```

CAST from `JSON`, named `Tuple`, `Map` and `Object('json')` to `JSON` type will be supported later.

## Reading JSON paths as subcolumns

JSON type supports reading every path as a separate subcolumn. If type of the requested path was not specified in the JSON type declaration, the subcolumn of the path will always have type [Dynamic](/docs/en/sql-reference/data-types/dynamic.md).

For example:

```sql
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text
┌─json──────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":[1,2,3],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}    │
│ {"a":{"b":43,"g":43.43},"c":[4,5,6]}                  │
└───────────────────────────────────────────────────────┘
```

```sql
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

If the requested path wasn't found in the data, it will be filled with `NULL` values:

```sql
SELECT json.non.existing.path FROM test;
```

```text
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

Let's check the data types of returned subcolumns:
```sql
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

As we can see, for `a.b` the type is `UInt32` as we specified in the JSON type declaration, and for all other subcolumns the type is `Dynamic`.

It is also possible to read subcolumns of a `Dynamic` type using special syntax `json.some.path.:TypeName`:

```sql
select json.a.g.:Float64, dynamicType(json.a.g), json.d.:Date, dynamicType(json.d) FROM test;
```

```text
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

`Dynamic` subcolumns can be casted to any data type. In this case the exception will be thrown if internal type inside `Dynamic` cannot be casted to the requested type:

```sql
select json.a.g::UInt64 as uint FROM test;
```

```text
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql
select json.a.g::UUID as float FROM test;
```

```text
Received exception:
Code: 48. DB::Exception: Conversion between numeric types and UUID is not supported. Probably the passed UUID is unquoted: while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'. (NOT_IMPLEMENTED)
```

## Reading JSON sub-objects as subcolumns

JSON type supports reading nested objects as subcolumns with type `JSON` using special syntax `json.^some.path`:

```sql
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text
┌─json────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":42,"g":42.42}},"c":[1,2,3],"d":{"e":{"f":{"g":"Hello, World","h":[1,2,3]}}}} │
│ {"d":{"e":{"f":{"h":[4,5,6]}}},"f":"Hello, World!"}                                         │
│ {"a":{"b":{"c":43,"e":10,"g":43.43}},"c":[4,5,6]}                                           │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text
┌─json.^`a`.b───────────────┬─json.^`d`.e.f────────────────────┐
│ {"c":42,"g":42.42}        │ {"g":"Hello, World","h":[1,2,3]} │
│ {}                        │ {"h":[4,5,6]}                    │
│ {"c":43,"e":10,"g":43.43} │ {}                               │
└───────────────────────────┴──────────────────────────────────┘
```

:::note
Reading sub-objects as subcolumns may be inefficient, as this may require almost full scan of the JSON data.
:::

## Types inference for paths

During JSON parsing ClickHouse tries to detect the most appropriate data type for each JSON path. It works similar to [automatic schema inference from input data](/docs/en/interfaces/schema-inference.md) and controlled by the same settings:
 
- [input_format_try_infer_integers](/docs/en/interfaces/schema-inference.md#inputformattryinferintegers)
- [input_format_try_infer_dates](/docs/en/interfaces/schema-inference.md#inputformattryinferdates)
- [input_format_try_infer_datetimes](/docs/en/interfaces/schema-inference.md#inputformattryinferdatetimes)
- [schema_inference_make_columns_nullable](/docs/en/interfaces/schema-inference.md#schemainferencemakecolumnsnullable)
- [input_format_json_try_infer_numbers_from_strings](/docs/en/interfaces/schema-inference.md#inputformatjsontryinfernumbersfromstrings)
- [input_format_json_infer_incomplete_types_as_strings](/docs/en/interfaces/schema-inference.md#inputformatjsoninferincompletetypesasstrings)
- [input_format_json_read_numbers_as_strings](/docs/en/interfaces/schema-inference.md#inputformatjsonreadnumbersasstrings)
- [input_format_json_read_bools_as_strings](/docs/en/interfaces/schema-inference.md#inputformatjsonreadboolsasstrings)
- [input_format_json_read_bools_as_numbers](/docs/en/interfaces/schema-inference.md#inputformatjsonreadboolsasnumbers)
- [input_format_json_read_arrays_as_strings](/docs/en/interfaces/schema-inference.md#inputformatjsonreadarraysasstrings)

Let's see some examples:

```sql
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

## Handling arrays of JSON objects

JSON paths that contains an array of objects are parsed as type `Array(JSON)` and inserted into `Dynamic` column for this path. To read an array of objects you can extract it from `Dynamic` column as a subcolumn:

```sql
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text3
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

As you can notice, the `max_dynamic_types/max_dynamic_paths` parameters of the nested `JSON` type were reduced compared to the default values. It's needed to avoid number of subcolumns to grow  uncontrolled on nested arrays of JSON objects.

Let's try to read subcolumns from this nested `JSON` column:

```sql
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test; 
```

```text
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

We can avoid writing `Array(JSON)` subcolumn name using special syntax:

```sql
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

The number of `[]` after path indicates the array level. `json.path[][]` will be transformed to `json.path.:Array(Array(JSON))`

Let's check the paths and types inside our `Array(JSON)`:

```sql
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

Let's read subcolumns from `Array(JSON)` column:

```sql
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

We can also read sub-object subcolumns from nested `JSON` column:

```sql
SELECT json.a.b[].^k FROM test
```

```text
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

## Reading JSON type from the data

All text formats (JSONEachRow, TSV, CSV, CustomSeparated, Values, etc) supports reading `JSON` type.

Examples:

```sql
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

For text formats like CSV/TSV/etc `JSON` is parsed from a string containing JSON object

```sql
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

## Reaching the limit of dynamic paths inside JSON

`JSON` data type can store only limited number of paths as separate subcolumns inside. By default, this limit is 1024, but you can change it in type declaration using parameter `max_dynamic_paths`.
When the limit is reached, all new paths inserted to `JSON` column will be stored in a single shared data structure. It's still possible to read such paths as subcolumns, but it will require reading the whole
shared data structure to extract the values of this path. This limit is needed to avoid the enormous number of different subcolumns that can make the table unusable.

Let's see what happens when the limit is reached in different scenarios.

### Reaching the limit during data parsing

During parsing of `JSON` object from the data, when the limit is reached for current block of data, all new paths will be stored in a shared data structure. We can check it using introspection functions `JSONDynamicPaths, JSONSharedDataPaths`:

```sql
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

As we can see, after inserting paths `e` and `f.g` the limit was reached and we inserted them into shared data structure.

### During merges of data parts in MergeTree table engines

During merge of several data parts in MergeTree table the `JSON` column in the resulting data part can reach the limit of dynamic paths and won't be able to store all paths from source parts as subcolumns.
In this case ClickHouse chooses what paths will remain as subcolumns after merge and what paths will be stored in the shared data structure. In most cases ClickHouse tries to keep paths that contain
the largest number of non-null values and move the rarest paths to the shared data structure, but it depends on the implementation.

Let's see an example of such merge. First, let's create a table with `JSON` column, set the limit of dynamic paths to `3` and insert values with `5` different paths:

```sql
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) engine=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e)  FROM numbers(1);
```

Each insert will create a separate data pert with `JSON` column containing single path:
```sql
SELECT count(), JSONDynamicPaths(json) AS dynamic_paths, JSONSharedDataPaths(json) AS shared_data_paths, _part FROM test GROUP BY _part, dynamic_paths, shared_data_paths ORDER BY _part ASC
```

```text
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘

```

Now, let's merge all parts into one and see what will happen:

```sql
SYSTEM START MERGES test;
OPTIMIZE TABLE test FINAL;
SELECT count(), dynamicType(d), _part FROM test GROUP BY _part, dynamicType(d) ORDER BY _part;
```

```text
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       1 │ ['a','b','c'] │ ['e']             │ all_1_5_2 │
│       2 │ ['a','b','c'] │ ['d']             │ all_1_5_2 │
│      12 │ ['a','b','c'] │ []                │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

As we can see, ClickHouse kept the most frequent paths `a`, `b` and `c` and moved paths `e` and `d` to shared data structure.

## Introspection functions

There are several functions that can help to inspect the content of the JSON column: [JSONAllPaths](../functions/json-functions.md#jsonallpaths), [JSONAllPathsWithTypes](../functions/json-functions.md#jsonallpathswithtypes), [JSONDynamicPaths](../functions/json-functions.md#jsondynamicpaths), [JSONDynamicPathsWithTypes](../functions/json-functions.md#jsondynamicpathswithtypes), [JSONSharedDataPaths](../functions/json-functions.md#jsonshareddatapaths), [JSONSharedDataPathsWithTypes](../functions/json-functions.md#jsonshareddatapathswithtypes), [distinctDynamicTypes](../aggregate-functions/reference/distinctdynamictypes.md), [distinctJSONPaths and distinctJSONPathsAndTypes](../aggregate-functions/reference/distinctjsonpaths.md)

**Examples**

Let's investigate the content of [GH Archive](https://www.gharchive.org/) dataset for `2020-01-01` date:

```sql
SELECT arrayJoin(distinctJSONPaths(json)) FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject) 
```

```text
┌─arrayJoin(distinctJSONPaths(json))─────────────────────────┐
│ actor.avatar_url                                           │
│ actor.display_login                                        │
│ actor.gravatar_id                                          │
│ actor.id                                                   │
│ actor.login                                                │
│ actor.url                                                  │
│ created_at                                                 │
│ id                                                         │
│ org.avatar_url                                             │
│ org.gravatar_id                                            │
│ org.id                                                     │
│ org.login                                                  │
│ org.url                                                    │
│ payload.action                                             │
│ payload.before                                             │
│ payload.comment._links.html.href                           │
│ payload.comment._links.pull_request.href                   │
│ payload.comment._links.self.href                           │
│ payload.comment.author_association                         │
│ payload.comment.body                                       │
│ payload.comment.commit_id                                  │
│ payload.comment.created_at                                 │
│ payload.comment.diff_hunk                                  │
│ payload.comment.html_url                                   │
│ payload.comment.id                                         │
│ payload.comment.in_reply_to_id                             │
│ payload.comment.issue_url                                  │
│ payload.comment.line                                       │
│ payload.comment.node_id                                    │
│ payload.comment.original_commit_id                         │
│ payload.comment.original_position                          │
│ payload.comment.path                                       │
│ payload.comment.position                                   │
│ payload.comment.pull_request_review_id                     │
...
│ payload.release.node_id                                    │
│ payload.release.prerelease                                 │
│ payload.release.published_at                               │
│ payload.release.tag_name                                   │
│ payload.release.tarball_url                                │
│ payload.release.target_commitish                           │
│ payload.release.upload_url                                 │
│ payload.release.url                                        │
│ payload.release.zipball_url                                │
│ payload.size                                               │
│ public                                                     │
│ repo.id                                                    │
│ repo.name                                                  │
│ repo.url                                                   │
│ type                                                       │
└─arrayJoin(distinctJSONPaths(json))─────────────────────────┘
```

```sql
SELECT arrayJoin(distinctJSONPathsAndTypes(json)) FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject) SETTINGS date_time_input_format='best_effort'
```


```text
┌─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┐
│ ('actor.avatar_url',['String'])                             │
│ ('actor.display_login',['String'])                          │
│ ('actor.gravatar_id',['String'])                            │
│ ('actor.id',['Int64'])                                      │
│ ('actor.login',['String'])                                  │
│ ('actor.url',['String'])                                    │
│ ('created_at',['DateTime'])                                 │
│ ('id',['String'])                                           │
│ ('org.avatar_url',['String'])                               │
│ ('org.gravatar_id',['String'])                              │
│ ('org.id',['Int64'])                                        │
│ ('org.login',['String'])                                    │
│ ('org.url',['String'])                                      │
│ ('payload.action',['String'])                               │
│ ('payload.before',['String'])                               │
│ ('payload.comment._links.html.href',['String'])             │
│ ('payload.comment._links.pull_request.href',['String'])     │
│ ('payload.comment._links.self.href',['String'])             │
│ ('payload.comment.author_association',['String'])           │
│ ('payload.comment.body',['String'])                         │
│ ('payload.comment.commit_id',['String'])                    │
│ ('payload.comment.created_at',['DateTime'])                 │
│ ('payload.comment.diff_hunk',['String'])                    │
│ ('payload.comment.html_url',['String'])                     │
│ ('payload.comment.id',['Int64'])                            │
│ ('payload.comment.in_reply_to_id',['Int64'])                │
│ ('payload.comment.issue_url',['String'])                    │
│ ('payload.comment.line',['Int64'])                          │
│ ('payload.comment.node_id',['String'])                      │
│ ('payload.comment.original_commit_id',['String'])           │
│ ('payload.comment.original_position',['Int64'])             │
│ ('payload.comment.path',['String'])                         │
│ ('payload.comment.position',['Int64'])                      │
│ ('payload.comment.pull_request_review_id',['Int64'])        │
...
│ ('payload.release.node_id',['String'])                      │
│ ('payload.release.prerelease',['Bool'])                     │
│ ('payload.release.published_at',['DateTime'])               │
│ ('payload.release.tag_name',['String'])                     │
│ ('payload.release.tarball_url',['String'])                  │
│ ('payload.release.target_commitish',['String'])             │
│ ('payload.release.upload_url',['String'])                   │
│ ('payload.release.url',['String'])                          │
│ ('payload.release.zipball_url',['String'])                  │
│ ('payload.size',['Int64'])                                  │
│ ('public',['Bool'])                                         │
│ ('repo.id',['Int64'])                                       │
│ ('repo.name',['String'])                                    │
│ ('repo.url',['String'])                                     │
│ ('type',['String'])                                         │
└─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┘
```

## Tips for better usage of the JSON type

Before creating `JSON` column and loading data into it, consider the following tips:

- Investigate your data and specify as many path hints with types as you can. It will make the storage and the reading much more efficient.
- Think about what paths you will need and what paths you will never need. Specify paths that you won't need in the SKIP section and SKIP REGEXP if needed. It will improve the storage.
- Don't set `max_dynamic_paths` parameter to very high values, it can make the storage and reading less efficient.
