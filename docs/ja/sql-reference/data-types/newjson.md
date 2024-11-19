---
slug: /ja/sql-reference/data-types/newjson
sidebar_position: 63
sidebar_label: JSON
keywords: [json, data type]
---

# JSON データ型

JavaScript Object Notation (JSON) ドキュメントを単一のカラムに保存します。

:::note
この機能はエクスペリメンタルなものであり、実稼働環境には適していません。JSON ドキュメントを扱う必要がある場合は、[このガイド](/docs/ja/integrations/data-formats/json/overview)の使用を検討してください。
JSON 型を使用するには、`allow_experimental_json_type = 1` を設定してください。
:::

`JSON` 型のカラムを宣言するには、次の構文を使用します：

``` sql
<column_name> JSON(max_dynamic_paths=N, max_dynamic_types=M, some.path TypeName, SKIP path.to.skip, SKIP REGEXP 'paths_regexp')
```
ここで：
- `max_dynamic_paths` は、単一のデータブロック（たとえば、MergeTree テーブルの単一データ部分）に渡ってサブカラムとして別々に保存できるパスの数を示す任意のパラメータです。この制限を超えると、他のパスはすべて単一の構造にまとめて保存されます。`max_dynamic_paths` のデフォルト値は `1024` です。
- `max_dynamic_types` は、タイプ `Dynamic` の単一パスカラム内に保存できる異なるデータ型の数を示す `1` から `255` の間の任意のパラメータです。この制限を超えると、新しいタイプはすべて `String` 型に変換されます。`max_dynamic_types` のデフォルト値は `32` です。
- `some.path TypeName` は JSON 内の特定のパスに対する任意の型ヒントです。そのようなパスは常に指定された型のサブカラムとして保存されます。
- `SKIP path.to.skip` は、JSON パース中にスキップされるべき特定のパスのための任意のヒントです。そのようなパスは JSON カラムに保存されることはありません。指定されたパスがネストされた JSON オブジェクトである場合、ネストされた全体のオブジェクトがスキップされます。
- `SKIP REGEXP 'path_regexp'` は、JSON パース中にパスをスキップするために使用される正規表現を持つ任意のヒントです。この正規表現と一致するすべてのパスは JSON カラムに保存されることはありません。

## JSON の作成

テーブルカラム定義で `JSON` 型を使用する：

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

`String` からの CAST を使用する：

```sql
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text
┌─json───────────────────────────────────────────┐
│ {"a":{"b":42},"c":[1,2,3],"d":"Hello, World!"} │
└────────────────────────────────────────────────┘
```

`Tuple` からの CAST を使用する：

```sql
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text
┌─json───────────────────────────────────────────┐
│ {"a":{"b":42},"c":[1,2,3],"d":"Hello, World!"} │
└────────────────────────────────────────────────┘
```

`Map` からの CAST を使用する：

```sql
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text
┌─json───────────────────────────────────────────┐
│ {"a":{"b":42},"c":[1,2,3],"d":"Hello, World!"} │
└────────────────────────────────────────────────┘
```

非推奨の `Object('json')` からの CAST を使用する：

```sql
 SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::Object('json')::JSON AS json;
 ```

```text
┌─json───────────────────────────────────────────┐
│ {"a":{"b":42},"c":[1,2,3],"d":"Hello, World!"} │
└────────────────────────────────────────────────┘
```

:::note
`Tuple`/`Map`/`Object('json')` から `JSON` への CAST は、カラムを `String` カラムにシリアライズして JSON オブジェクトを含め、それを `JSON` 型のカラムにデシリアライズすることで実装されています。
:::

異なる引数を持つ `JSON` 型間の CAST は後でサポートされます。

## サブカラムとしての JSON パスの読み取り

JSON 型は、すべてのパスを別のサブカラムとして読むことをサポートします。JSON 型の宣言で要求されたパスの型が指定されていない場合、そのパスのサブカラムは常に [Dynamic](/docs/ja/sql-reference/data-types/dynamic.md) 型を持ちます。

例：

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

要求されたパスがデータ内に見つからない場合、それは `NULL` 値で埋められます：

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

返されたサブカラムのデータ型を確認してみましょう：
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

ご覧の通り、`a.b` のタイプは JSON 型宣言で指定した `UInt32` であり、他のサブカラムのタイプはすべて `Dynamic` です。

`Dynamic` 型サブカラムを特別な構文 `json.some.path.:TypeName` を使って読み取ることも可能です：

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

`Dynamic` サブカラムは任意のデータ型にキャスト可能です。この場合、`Dynamic` 内部の型が要求された型にキャストできない場合、例外がスローされます：

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

## サブカラムとしての JSON サブオブジェクトの読み取り

JSON 型は、特殊な構文 `json.^some.path` を使用して、ネストされたオブジェクトを `JSON` 型のサブカラムとして読むことをサポートします：

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
サブオブジェクトをサブカラムとして読み取ることは非効率的であるかもしれません。JSON データのほぼ完全なスキャンを必要とする可能性があります。
:::

## パスの型推論

JSON パース中、ClickHouse は各 JSON パスに最適なデータ型を検出しようとします。これは [入力データからの自動スキーマ推論](/docs/ja/interfaces/schema-inference.md) と同様に機能し、同じ設定によって制御されます：

- [input_format_try_infer_integers](/docs/ja/interfaces/schema-inference.md#inputformattryinferintegers)
- [input_format_try_infer_dates](/docs/ja/interfaces/schema-inference.md#inputformattryinferdates)
- [input_format_try_infer_datetimes](/docs/ja/interfaces/schema-inference.md#inputformattryinferdatetimes)
- [schema_inference_make_columns_nullable](/docs/ja/interfaces/schema-inference.md#schemainferencemakecolumnsnullable)
- [input_format_json_try_infer_numbers_from_strings](/docs/ja/interfaces/schema-inference.md#inputformatjsontryinfernumbersfromstrings)
- [input_format_json_infer_incomplete_types_as_strings](/docs/ja/interfaces/schema-inference.md#inputformatjsoninferincompletetypesasstrings)
- [input_format_json_read_numbers_as_strings](/docs/ja/interfaces/schema-inference.md#inputformatjsonreadnumbersasstrings)
- [input_format_json_read_bools_as_strings](/docs/ja/interfaces/schema-inference.md#inputformatjsonreadboolsasstrings)
- [input_format_json_read_bools_as_numbers](/docs/ja/interfaces/schema-inference.md#inputformatjsonreadboolsasnumbers)
- [input_format_json_read_arrays_as_strings](/docs/ja/interfaces/schema-inference.md#inputformatjsonreadarraysasstrings)

いくつかの例を見てみましょう：

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

## JSON オブジェクトの配列の扱い

オブジェクトの配列を含む JSON パスは `Array(JSON)` 型としてパースされ、`Dynamic` カラムに挿入されます。この配列を読み取るには、`Dynamic` カラムからサブカラムとして抽出できます：

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

ご覧の通り、ネストされた `JSON` 型の `max_dynamic_types/max_dynamic_paths` パラメータはデフォルト値よりも削減されています。これは、ネストされた JSON オブジェクトの配列においてサブカラムの数が無制御に増加するのを防ぐためです。

このネストされた `JSON` カラムからサブカラムを読み取ってみましょう：

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

`Array(JSON)` サブカラム名を書くのを避けるために特別な構文を使用できます：

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

パスの後に続く `[]` の数は配列レベルを示します。`json.path[][]` は `json.path.:Array(Array(JSON))` に変換されます。

`Array(JSON)` 内のパスとその型を確認してみましょう：

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

`Array(JSON)` カラムからサブカラムを読み取ってみましょう：

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

ネストされた `JSON` カラムからサブオブジェクトサブカラムを読み取ることもできます：

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

## データからの JSON 型の読み取り

すべてのテキストフォーマット（JSONEachRow、TSV、CSV、CustomSeparated、Values など）は `JSON` 型の読み取りをサポートしています。

例：

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

CSV/TSV/etc のようなテキストフォーマットでは、`JSON` は JSON オブジェクトを含む文字列からパースされます。

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

## JSON 内の動的パスの限界を超える

`JSON` データ型は内部に有限の数のパスしか別々のサブカラムとして保存できません。デフォルトでは、この制限は 1024 です。しかし、この制限は型宣言において `max_dynamic_paths` パラメータを使用して変更できます。この制限が達成されると、`JSON` カラムに挿入されたすべての新しいパスは単一の共有データ構造に保存されます。この制限は、テーブルが使用不可能になるほどの膨大な数の異なるサブカラムを避けるために必要です。

異なるシナリオでこの制限が達成されたときに何が起こるか見てみましょう。

### データパース中に制限に達する

JSON オブジェクトをデータからパースする間に、現在のデータブロックの制限に達すると、すべての新しいパスは共有データ構造に保存されます。内省関数 `JSONDynamicPaths, JSONSharedDataPaths` を使用してそれを確認できます：

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

ご覧のとおり、パス `e` と `f.g` を挿入した後、制限に達し、それらを共有データ構造に挿入しました。

### MergeTree テーブルエンジンでのデータパーツのマージ中

MergeTree テーブルのいくつかのデータパーツのマージ中、生成されたデータパートの `JSON` カラムは動的パスの制限に達し、サブカラムとしてすべてのパスを保存できなくなる可能性があります。この場合、ClickHouse はマージ後にサブカラムとして残るパスと共有データ構造に保存されるパスを選択します。多くの場合、ClickHouse は最も非ヌル値の多いパスを保持し、最も希少なパスを共有データ構造に移動しようとしますが、これは実装によります。

そのようなマージの例を見てみましょう。最初に、`JSON` カラムを含むテーブルを作成し、動的パスの制限を `3` に設定し、` 5`つの異なるパスを持つ値を挿入します：

```sql
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) engine=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e)  FROM numbers(1);
```

各挿入は `JSON` カラムを含む単一のパスを持つ別々のデータパートを作成します：
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

さて、すべてのパートを1つにマージして、何が起こるか見てみましょう：

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
└─────────┴───────────────┴───────────────────┘
```

ご覧のとおり、ClickHouse は最も頻繁に現れるパス `a`、`b`、`c` を保持し、パス `e` と `d` を共有データ構造に移動しました。

## 内省関数

JSON カラムの内容を調べるのに役立ついくつかの関数があります：[JSONAllPaths](../functions/json-functions.md#jsonallpaths)、[JSONAllPathsWithTypes](../functions/json-functions.md#jsonallpathswithtypes)、[JSONDynamicPaths](../functions/json-functions.md#jsondynamicpaths)、[JSONDynamicPathsWithTypes](../functions/json-functions.md#jsondynamicpathswithtypes)、[JSONSharedDataPaths](../functions/json-functions.md#jsonshareddatapaths)、[JSONSharedDataPathsWithTypes](../functions/json-functions.md#jsonshareddatapathswithtypes)、[distinctDynamicTypes](../aggregate-functions/reference/distinctdynamictypes.md)、[distinctJSONPaths and distinctJSONPathsAndTypes](../aggregate-functions/reference/distinctjsonpaths.md)

**例**

`2020-01-01` 日付の [GH Archive](https://www.gharchive.org/) データセットの内容を調査してみましょう：

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

## ALTER MODIFY COLUMN を JSON 型に

既存のテーブルを変更し、カラムの型を新しい `JSON` 型に変更することができます。現在のところ、サポートされているのは `String` 型からの変更のみです。

**例**

```sql
CREATE TABLE test (json String) ENGINE=MergeTree ORDeR BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}')), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

## JSON 型のより良い使用のためのヒント

`JSON` カラムを作成してデータをロードする前に、次のヒントを検討してください：

- データを調査し、可能な限り多くのパスヒントと型を指定してください。それにより、ストレージと読み取りが非常に効率的になります。
- 必要なパスと不要なパスを考え、必要のないパスを SKIP セクションや SKIP REGEXP に指定してください。それにより、ストレージの効率が向上します。
- `max_dynamic_paths` パラメータを非常に高い値に設定しないでください。それはストレージと読み取りを効率的でなくする可能性があります。
