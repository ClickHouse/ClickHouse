---
description: 'JSON データをネイティブに扱える ClickHouse の JSON データ型に関するドキュメント'
keywords: ['json', 'data type']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'JSON データ型'
doc_type: 'reference'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="JSON 型の使用例や高度な機能、利用時の考慮事項については、JSON のベストプラクティスガイドをご覧ください。" icon="book" infoText="詳細はこちら" infoUrl="/docs/best-practices/use-json-where-appropriate" title="ガイドをお探しですか？" />
</Link>

<br />

`JSON` 型は、JavaScript Object Notation (JSON) ドキュメントを 1 つのカラムに格納します。

:::note
ClickHouse オープンソースでは、`JSON` データ型はバージョン 25.3 で本番環境対応となっています。以前のバージョンでは、この型を本番環境で使用することは推奨されません。
:::

`JSON` 型のカラムを宣言するには、次の構文を使用します。

```sql
<column_name> JSON
(
    max_dynamic_paths=N,
    max_dynamic_types=M,
    some.path TypeName,
    SKIP path.to.skip,
    SKIP REGEXP 'paths_regexp'
)
```

上記の構文における各パラメーターの定義は次のとおりです。

| Parameter                   | Description                                                                                                                                                                                                                                                                                   | Default Value |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `max_dynamic_paths`         | 個別に保存される単一のデータブロック (たとえば、MergeTree table の単一の data part) 内で、サブカラムとして別々に保存できるパス数を示す省略可能なパラメーターです。<br /><br />この上限を超えると、それ以外のすべてのパスは [共有データ](#shared-data-structure) と呼ばれる単一の構造にまとめて保存されます。<br /><br />また、このパラメーターを変更せずに動的パス数の上限を変更する[方法](#controlling-the-number-of-dynamic-paths)もあります。 | `1024`        |
| `max_dynamic_types`         | `1` から `255` までの値を取る省略可能なパラメーターで、型 `Dynamic` の単一のパスカラム内で、個別に保存される単一のデータブロック (たとえば、MergeTree table の単一の data part) ごとに、何種類の異なる data types を別々に保存できるかを示します。<br /><br />この上限を超えると、新しい型はすべて `shared variant` と呼ばれる単一の構造にまとめて保存されます。                                                               | `32`          |
| `some.path TypeName`        | JSON 内の特定のパスに対する省略可能な type hint です。このようなパスは、指定した型のサブカラムとして常に保存されます。                                                                                                                                                                                                                           |               |
| `SKIP path.to.skip`         | JSON のパース時にスキップする特定のパスを指定する省略可能な hint です。これらのパスは JSON column には保存されません。指定したパスがネストされた JSONオブジェクト の場合は、そのネストされたオブジェクト全体がスキップされます。                                                                                                                                                              |               |
| `SKIP REGEXP 'path_regexp'` | JSON のパース時にパスをスキップするための正規表現を指定する、省略可能な hint です。この正規表現に一致するすべてのパスは JSON column には保存されません。                                                                                                                                                                                                      |               |

<WhenToUseJson />

<div id="creating-json">
  ## `JSON` の作成
</div>

このセクションでは、`JSON` を作成するさまざまな方法を紹介します。

<div id="using-json-in-a-table-column-definition">
  ### テーブルのカラム定義で `JSON` を使用する
</div>

```sql title="Query (Example 1)"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 1)"
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql title="Query (Example 2)"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 2)"
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":["1","2","3"]}  │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":["4","5","6"]}  │
└───────────────────────────────────┘
```

<div id="using-cast-with-json">
  ### `::JSON` を使って CAST する
</div>

特別な構文 `::JSON` を使うと、さまざまな型を CAST できます。

<div id="cast-from-string-to-json">
  #### `String` から `JSON` への CAST
</div>

```sql title="Query"
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-tuple-to-json">
  #### `Tuple` から `JSON` への CAST
</div>

```sql title="Query"
SET enable_named_columns_in_function_tuple = 1;
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-map-to-json">
  #### `Map` を `JSON` に CAST
</div>

```sql title="Query"
SET use_variant_as_common_type=1;
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

:::note
JSONパスはフラット化されて保存されます。つまり、`a.b.c` のようなパスから JSONオブジェクトを生成する際、
そのオブジェクトを `{ "a.b.c" : ... }` として構築すべきなのか、それとも `{ "a": { "b": { "c": ... } } }` として構築すべきなのかは判断できません。
この実装では、常に後者として扱います。

たとえば:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

次が返されます:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

**ではなく**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## JSONパスをサブカラムとして読み取る
</div>

`JSON` 型では、すべてのパスを個別のサブカラムとして読み取ることができます。
要求されたパスの型が JSON 型の宣言で指定されていない場合、
そのパスのサブカラムは常に [Dynamic](/ja/sql-reference/data-types/dynamic.md) 型になります。

たとえば:

```sql title="Query"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":["1","2","3"],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}          │
│ {"a":{"b":43,"g":43.43},"c":["4","5","6"]}                  │
└─────────────────────────────────────────────────────────────┘
```

```sql title="Query (Reading JSON paths as sub-columns)"
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text title="Response (Reading JSON paths as sub-columns)"
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

`getSubcolumn` 関数を使用して、JSON型からサブカラムを読み出すこともできます:

```sql title="Query"
SELECT getSubcolumn(json, 'a.b'), getSubcolumn(json, 'a.g'), getSubcolumn(json, 'c'), getSubcolumn(json, 'd') FROM test;
```

```text title="Response"
┌─getSubcolumn(json, 'a.b')─┬─getSubcolumn(json, 'a.g')─┬─getSubcolumn(json, 'c')─┬─getSubcolumn(json, 'd')─┐
│                        42 │ 42.42                     │ [1,2,3]                 │ 2020-01-01              │
│                         0 │ ᴺᵁᴸᴸ                      │ ᴺᵁᴸᴸ                    │ 2020-01-02              │
│                        43 │ 43.43                     │ [4,5,6]                 │ ᴺᵁᴸᴸ                    │
└───────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

データ内に指定したパスが見つからない場合、その箇所は `NULL` で補完されます：

```sql title="Query"
SELECT json.non.existing.path FROM test;
```

```text title="Response"
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

返されたサブカラムのデータ型を確認しましょう：

```sql title="Query"
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text title="Response"
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

ご覧のとおり、`a.b` については、JSON型の宣言で指定したとおり型は `UInt32` であり、
それ以外のすべてのサブカラムの型は `Dynamic` です。

また、`Dynamic` 型のサブカラムは、特殊構文 `json.some.path.:TypeName` を使って読み取ることもできます。

```sql title="Query"
SELECT
    json.a.g.:Float64,
    dynamicType(json.a.g),
    json.d.:Date,
    dynamicType(json.d)
FROM test
```

```text title="Response"
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

`Dynamic` のサブカラムは任意のデータ型にキャストできます。この場合、`Dynamic` 内部の型を指定した型にキャストできないと、例外が発生します：

```sql title="Query"
SELECT json.a.g::UInt64 AS uint
FROM test;
```

```text title="Response"
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql title="Query"
SELECT json.a.g::UUID AS float
FROM test;
```

```text title="Response"
Received exception from server:
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception:
Conversion between numeric types and UUID is not supported.
Probably the passed UUID is unquoted:
while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'.
(NOT_IMPLEMENTED)
```

:::note
Compact 形式の MergeTree パーツからサブカラムを効率的に読み取るには、MergeTree の設定 [write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts) が有効になっていることを確認してください。
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## JSON のサブオブジェクトをサブカラムとして読み取る
</div>

`JSON` 型では、特別な構文 `json.^some.path` を使用して、ネストされたオブジェクトを `JSON` 型のサブカラムとして読み取れます。

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":"42","g":42.42}},"c":["1","2","3"],"d":{"e":{"f":{"g":"Hello, World","h":["1","2","3"]}}}} │
│ {"d":{"e":{"f":{"h":["4","5","6"]}}},"f":"Hello, World!"}                                                 │
│ {"a":{"b":{"c":"43","e":"10","g":43.43}},"c":["4","5","6"]}                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text title="Response"
┌─json.^`a`.b───────────────────┬─json.^`d`.e.f──────────────────────────┐
│ {"c":"42","g":42.42}          │ {"g":"Hello, World","h":["1","2","3"]} │
│ {}                            │ {"h":["4","5","6"]}                    │
│ {"c":"43","e":"10","g":43.43} │ {}                                     │
└───────────────────────────────┴────────────────────────────────────────┘
```

:::note
パスが基本的な (`map`) [共有データ](#shared-data-structure)に格納されている場合、サブオブジェクトのサブカラムの読み取りは、共有データ構造全体をスキャンする必要があるため、非効率になることがあります。`map_with_buckets` または `advanced` の共有データシリアライゼーションでは、共有データからサブカラムを読み取る処理が大幅に最適化されています。
:::

<div id="reading-json-combined-sub-columns">
  ## JSON の結合サブカラムの読み取り
</div>

`JSON` 型では、特別な構文 `json.@some.path` を使って、パスを **結合サブカラム** として読み取れます。
指定したパスの結合サブカラムは、次のいずれかを返します。

* そのパスにリテラル値がある場合は、その値を `Dynamic` として返します。
* そのパスにリテラル値はないものの、ネストされたサブパスがある場合は、そのパスの JSON サブオブジェクトを `Dynamic` として返します。
* そのパスにリテラル値もサブパスも存在しない場合は、`NULL` を返します。

これは、行によって同じパスにスカラー値またはネストされたオブジェクトのいずれかが入る可能性がある場合に便利で、リテラルサブカラム (`json.a`) とサブオブジェクトサブカラム (`json.^a`) を別々にクエリするよりも扱いやすくなります。

次の例では、パス `a` に対する 3 種類すべてのサブカラム型を比較します。

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : 42, "b" : {"c" : 1, "d" : "Hello"}}'), ('{"a" : {"x": 1, "y": 2}, "b" : {"c" : 1}}'), ('{"c" : "World"}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────┐
│ {"a":42,"b":{"c":1,"d":"Hello"}}│
│ {"a":{"x":1,"y":2},"b":{"c":1}}│
│ {"c":"World"}                   │
└─────────────────────────────────┘
```

```sql title="Query"
SELECT
    json.a,
    dynamicType(json.a),
    json.^a,
    toTypeName(json.^a),
    json.@a,
    dynamicType(json.@a)
FROM test;
```

```text title="Response"
┌─json.a─┬─dynamicType(json.a)─┬─json.^a───────┬─toTypeName(json.^a)─┬─json.@a───────┬─dynamicType(json.@a)─┐
│ 42     │ Int64               │ {}            │ JSON                │ 42            │ Int64                │
│ NULL   │ None                │ {"x":1,"y":2} │ JSON                │ {"x":1,"y":2} │ JSON                 │
│ NULL   │ None                │ {}            │ JSON                │ NULL          │ None                 │
└────────┴─────────────────────┴───────────────┴─────────────────────┴───────────────┴──────────────────────┘
```

* 行 1: `a` にはリテラル `42` が入っています。`json.a` はそれを `Dynamic(Int64)` として返し、`json.^a` は空のサブオブジェクト `{}` (`a` の下にネストしたキーがないため) を返し、`json.@a` はリテラル `42` を返します。
* 行 2: `a` にはネストされたオブジェクトが入っています。`json.a` は `NULL` を返し (そのパスにはリテラルが存在しないため) 、`json.^a` はそのサブオブジェクトを `JSON` として返し、`json.@a` もそのサブオブジェクトを `Dynamic(JSON)` として返します。
* 行 3: `a` はまったく存在しません。`json.a` と `json.@a` はどちらも `NULL` を返し、`json.^a` は空の `{}` を返します。

:::note
パスが基本的な (`map`) [共有データ](#shared-data-structure) に格納されている場合、結合サブカラムの読み取りでは共有データ構造全体をスキャンする必要があるため、非効率になることがあります。`map_with_buckets` または `advanced` の共有データシリアライゼーションでは、共有データからのサブカラムの読み取りは大幅に最適化されています。
:::

<div id="type-inference-for-paths">
  ## パスの型推論
</div>

`JSON` のパース時に、ClickHouse は各 JSON パスに対して最も適切なデータ型を推論しようとします。
これは [入力データからの自動スキーマ推論](/ja/interfaces/schema-inference.md) と同様に動作し、
同じ設定で制御されます。

* [input&#95;format&#95;try&#95;infer&#95;dates](/ja/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/ja/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/ja/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ja/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/ja/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/ja/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/ja/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/ja/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/ja/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/ja/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

いくつか例を見てみましょう：

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text title="Response"
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text title="Response"
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text title="Response"
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text title="Response"
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

<div id="handling-arrays-of-json-objects">
  ## JSON オブジェクトの配列の処理
</div>

オブジェクトの配列を含む JSON パスは、`Array(JSON)` 型として解析され、そのパスの `Dynamic` カラムに挿入されます。
オブジェクトの配列を読み取るには、`Dynamic` カラムからサブカラムとして抽出できます。

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text title="Response"
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

お気づきかもしれませんが、ネストされた `JSON` 型の `max_dynamic_types`/`max_dynamic_paths` パラメータは、デフォルト値より小さく設定されています。
これは、JSON オブジェクトのネストされた配列においてサブカラムの数が際限なく増加するのを防ぐために必要です。

それでは、ネストされた `JSON` カラムからサブカラムを読み取ってみましょう:

```sql title="Query"
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

特別な構文を使うと、`Array(JSON)` のサブカラム名を書かずに済みます:

```sql title="Query"
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

パスの後ろに付く `[]` の数は、配列の階層を示します。たとえば、`json.path[][]` は `json.path.:Array(Array(JSON))` に変換されます

それでは、`Array(JSON)` 内のパスと型を確認してみましょう。

```sql title="Query"
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text title="Response"
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

`Array(JSON)` カラムからサブカラムを読み取ってみましょう:

```sql title="Query"
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

ネストされた `JSON` カラムから、サブオブジェクトのサブカラムを読み取ることもできます：

```sql title="Query"
SELECT json.a.b[].^k FROM test
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

<div id="handling-json-keys-with-nulls">
  ## NULL 値を持つ JSON キーの扱い
</div>

この JSON 実装では、`null` と値が存在しない状態は同等とみなされます。

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

これは、元の JSON データに NULL 値のパスが含まれていたのか、そもそもそのパス自体が存在しなかったのかを判別できないことを意味します。

<div id="handling-json-keys-with-dots">
  ## ドットを含むJSONキーの扱い
</div>

JSONカラムでは内部的に、すべてのパスと値がフラット化された形式で保存されます。つまり、デフォルトでは次の2つのオブジェクトは同じものと見なされます。

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

どちらも内部的には、パス `a.b` と値 `42` の組として格納されます。JSON のフォーマット時には、ドットで区切られたパスの各部分に基づいて、常にネストされたオブジェクトが構成されます。

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

ご覧のとおり、元の JSON `{"a.b" : 42}` は `{"a" : {"b" : 42}}` の形式に整形されます。

この制限により、次のような有効な JSON オブジェクトもパースできなくなります。

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

ドットを含むキーをそのまま保持し、ネストされたオブジェクトとしてフォーマットされないようにするには、
設定 [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/ja/operations/settings/formats#json_type_escape_dots_in_keys) (`25.8` バージョン以降で利用可能) を有効にできます。この場合、パース時に JSON キー内のすべてのドットが
`%2E` にエスケープされ、フォーマット時に再び元に戻されます。

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a.b":"42"} │ ['a.b']             │ ['a%2Eb']           │
└──────────────────┴──────────────┴─────────────────────┴─────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
```

```text title="Response"
┌─json──────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ ['a%2Eb','a.b']    │
└───────────────────────────────────────┴────────────────────┘
```

ドットをエスケープしたキーをサブカラムとして読み取るには、サブカラム名でもドットをエスケープする必要があります。

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

注: 識別子パーサーおよびアナライザの制限により、サブカラム `` json.`a.b` `` はサブカラム `json.a.b` と同等であり、エスケープされたドットを含むパスは読み取れません:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

また、ドットを含むキーがある JSON path のヒントを指定する場合 (またはそれを `SKIP`/`SKIP REGEX` セクションで使用する場合) は、ヒント内でドットをエスケープする必要があります:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);
```

```text title="Response"
┌─json────────────────────────────────┬─json.a%2Eb─┬─toTypeName(json.a%2Eb)─┐
│ {"a.b":42,"a":{"b":"Hello World!"}} │         42 │ UInt8                  │
└─────────────────────────────────────┴────────────┴────────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(SKIP `a%2Eb`) as json, json.`a%2Eb`;
```

```text title="Response"
┌─json───────────────────────┬─json.a%2Eb─┐
│ {"a":{"b":"Hello World!"}} │ ᴺᵁᴸᴸ       │
└────────────────────────────┴────────────┘
```

<div id="reading-json-type-from-data">
  ## データからJSON型を読み込む
</div>

すべてのテキストフォーマット
([`JSONEachRow`](/ja/interfaces/formats/JSONEachRow),
[`TSV`](/ja/interfaces/formats/TabSeparated),
[`CSV`](/ja/interfaces/formats/CSV),
[`CustomSeparated`](/ja/interfaces/formats/CustomSeparated),
[`Values`](/ja/interfaces/formats/Values) など) は、`JSON` 型の読み込みをサポートしています。

例:

```sql title="Query"
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

`CSV`/`TSV`/などのテキストフォーマットでは、`JSON` は JSON オブジェクトを含む文字列からパースされます。

```sql title="Query"
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

<div id="reaching-the-limit-of-dynamic-paths-inside-json">
  ## JSON 内の動的パス数の上限に達した場合
</div>

`JSON` データ型では、内部的に個別のサブカラムとして保持できるパス数に上限があります。
デフォルトではこの上限は `1024` ですが、型宣言で `max_dynamic_paths` パラメータを使って変更できます。

この上限に達すると、`JSON` カラムに新たに挿入されるすべてのパスは、1 つの共有データ構造に格納されます。
このようなパスも引き続きサブカラムとして読み取れますが、
効率は低下する可能性があります ([共有データに関するセクションを参照](#shared-data-structure)) 。
この上限は、テーブルが実質的に使用不能になるほど大量の異なるサブカラムが作成されるのを防ぐために必要です。

では、いくつかの異なるシナリオで、上限に達すると何が起こるかを見てみましょう。

<div id="reaching-the-limit-during-data-parsing">
  ### データのパース中に上限に達した場合
</div>

データから `JSON` オブジェクトをパースする際、現在のデータブロックで上限に達すると、
それ以降の新しいパスはすべて共有データ構造に格納されます。次の 2 つのイントロスペクション関数 `JSONDynamicPaths`、`JSONSharedDataPaths` を使用できます。

```sql title="Query"
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text title="Response"
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

ご覧のとおり、パス `e` と `f.g` を挿入した時点で上限に達し、
それらは共有データ構造に挿入されました。

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### MergeTree テーブルエンジンでのデータパーツのマージ時
</div>

`MergeTree` テーブルで複数のデータパーツをマージすると、生成されるデータパーツ内の `JSON` カラムが動的パスの上限に達し、
ソースパーツ内のすべてのパスをサブカラムとして保持できなくなることがあります。
この場合、ClickHouse は、マージ後もどのパスをサブカラムとして残し、どのパスを共有データ構造に保存するかを選択します。
ほとんどの場合、ClickHouse は
最も多くの非 NULL 値を含むパスを残し、出現頻度の低いパスを共有データ構造に移そうとします。ただし、これは実装に依存します。

このようなマージの例を見てみましょう。
まず、`JSON` カラムを持つテーブルを作成し、動的パスの上限を `3` に設定してから、`5` 個の異なるパスを持つ値を挿入します:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

insert のたびに、`JSON` カラムに 1 つのパスを含む個別のデータパーツが作成されます:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

では、すべてのパーツを1つにマージするとどうなるか見てみましょう。

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│      15 │ ['a','b','c'] │ ['d','e']         │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

ご覧のとおり、ClickHouse は最も頻繁に現れるパス `a`、`b`、`c` を保持し、パス `d` と `e` は共有データ構造に移しました。

<div id="shared-data-structure">
  ## 共有データ構造
</div>

前のセクションで説明したとおり、`max_dynamic_paths` の上限に達すると、以降の新しいパスはすべて 1 つの共有データ構造に格納されます。
このセクションでは、共有データ構造の詳細と、そこからパスのサブカラムをどのように読み出すかを見ていきます。

JSONカラムの内容を調べるために使用される関数の詳細については、[&quot;イントロスペクション関数&quot;](/ja/sql-reference/data-types/newjson#introspection-functions) のセクションを参照してください。

<div id="shared-data-structure-in-memory">
  ### メモリ内の共有データ構造
</div>

メモリ内では、共有データ構造は、フラット化された JSON パスからバイナリ形式でエンコードされた値へのマッピングを格納する `Map(String, String)` 型の単なるサブカラムです。
ここからパスのサブカラムを抽出するには、この `Map` カラム内のすべての行を順に走査し、要求されたパスとその値を探します。

<div id="shared-data-structure-in-merge-tree-parts">
  ### MergeTree パーツ内の共有データ構造
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルでは、ローカルまたはリモートのディスク上にあるすべてのデータを、データパーツとして保存します。また、ディスク上のデータはメモリ上とは異なる方法で保存できます。
現在、MergeTree データパーツにおける共有データ構造のシリアライゼーションには、`map`、`map_with_buckets`、
`advanced` の 3 種類があります。

シリアル化バージョンは、MergeTree の
設定 [object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
および [object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
で制御します (ゼロレベルパーツはテーブルへのデータ挿入時に作成されるパーツで、merge 中に作成されるパーツはより高いレベルになります) 。

注: 共有データ構造のシリアライゼーションの変更は、
`v3` の [object serialization version](../../operations/settings/merge-tree-settings.md#object_serialization_version)
でのみサポートされています

<div id="shared-data-map">
  #### Map
</div>

`map` シリアル化バージョンでは、shared data はメモリ内で保存される場合と同じく、`Map(String, String)` 型の単一のカラムとしてシリアル化されます。この型のシリアル化からパスのサブカラムを読み取るには、ClickHouse は `Map` カラム全体を読み取り、メモリ内で要求されたパスを抽出します。

このシリアル化は、データの書き込みや `JSON` カラム全体の読み取りには効率的ですが、パスのサブカラムの読み取りには効率的ではありません。

<div id="shared-data-map-with-buckets">
  #### バケット付き Map
</div>

`map_with_buckets` シリアル化バージョンでは、shared data は型 `Map(String, String)` の `N` 個のカラム (「バケット」) としてシリアライズされます。
各バケットには、パスの一部だけが含まれます。この種のシリアライゼーションでパスのサブカラムを読み取るには、ClickHouse は
1 つのバケットから `Map` カラム全体を読み込み、要求されたパスをメモリ上で抽出します。

このシリアライゼーションは、データの書き込みや `JSON` カラム全体の読み取りでは効率が劣りますが、パスのサブカラムの読み取りにはより効率的です。
これは、必要なバケットのデータだけを読み込むためです。

バケット数 `N` は、MergeTree 設定 [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (デフォルトは 8)
および [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (デフォルトは 32) で制御されます。
両方の設定で許可される最大値は 256 です。

<div id="shared-data-advanced">
  #### advanced
</div>

`advanced` シリアル化バージョンでは、shared data は、要求されたパスのデータだけを読み取れるようにする追加情報を保持した特別なデータ構造にシリアル化されます。これにより、パスのサブカラム読み取り性能が最大化されます。
このシリアル化はバケットにも対応しており、各バケットにはパスの一部だけが含まれます。

このシリアル化はデータの書き込みにはかなり非効率なため (そのため、ゼロレベルのパーツにこのシリアル化を使用することは推奨されません) 、`JSON` カラム全体の読み取りも `map` シリアル化と比べるとやや非効率ですが、パスのサブカラムの読み取りには非常に効率的です。

注記: このデータ構造には追加情報も保持されるため、このシリアル化では `map` および `map_with_buckets` シリアル化と比べてディスク上の保存サイズが大きくなります。

新しいshared dataのシリアル化について、より詳しい概要と実装の詳細は、[ブログ記事](https://clickhouse.com/blog/json-data-type-gets-even-better)を参照してください。

<div id="controlling-the-number-of-dynamic-paths">
  ## MergeTree パーツ内の JSON の動的パス数を制御する
</div>

JSON 内の動的パスに上限を設定する主な方法は、JSON 型宣言内で `max_dynamic_paths` パラメータを使用することです。
ただし、既存のカラムの `max_dynamic_paths` を変更するには、`ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)` を実行する必要があり、これにより既存のすべてのパーツを書き換えるバックグラウンドミューテーションが開始されます。
このミューテーションは非常に負荷が高くなることがあり、完了するまでサーバーのパフォーマンスに影響する可能性があります。これを避けるため、新しいデータパーツに対する MergeTree テーブル内の動的パス数の上限変更に役立つ、次の 3 つの設定を使用できます。

* `merge_max_dynamic_subcolumns_in_wide_part` - wide パーツへのマージ時に、各 JSON カラムの動的サブカラム数を制限する MergeTree 設定。
* `merge_max_dynamic_subcolumns_in_compact_part` - compact パーツへのマージ時に、各 JSON カラムの動的サブカラム数を制限する MergeTree 設定。
* `max_dynamic_subcolumns_in_json_type_parsing` - JSON データを JSON カラムにパースする際に、各 JSON カラムの動的サブカラム数を制限するセッション設定。

注: 動的パス数の上限は、ここで説明した設定の値がより大きくても、`max_dynamic_paths` パラメータで指定された値を超えることはできません。

<div id="introspection-functions">
  ## イントロスペクション関数
</div>

JSONカラムの内容を調べるのに役立つ関数がいくつかあります。

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**例**

日付 `2020-01-01` の [GH Archive](https://www.gharchive.org/) データセットの内容を調べてみましょう。

```sql title="Query"
SELECT arrayJoin(distinctJSONPaths(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
```

```text title="Response"
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

```sql title="Query"
SELECT arrayJoin(distinctJSONPathsAndTypes(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
SETTINGS date_time_input_format = 'best_effort'
```

```text title="Response"
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

<div id="alter-modify-column-to-json-type">
  ## ALTER MODIFY COLUMN で JSON 型に変更する
</div>

既存のテーブルに対して ALTER を実行し、カラムの型を新しい `JSON` 型に変更できます。現時点でサポートされているのは、`String` 型からの `ALTER` のみです。

**例**

```sql title="Query"
CREATE TABLE test (json String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}'), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text title="Response"
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

<div id="lazy-type-hints">
  ## Lazy Type Hints (Experimental)
</div>

:::note
この機能は実験的なもので、設定 `allow_experimental_json_lazy_type_hints` を有効にする必要があります。
:::

`ALTER TABLE ... MODIFY COLUMN` を使って JSON カラムの型ヒントを追加または変更すると、ClickHouse は通常、新しい型ヒントを materialize するために、すべてのパーツを書き換えます。数百テラバイト規模の履歴データを持つテーブルでは、これは非常に高コストになる可能性があります。

**遅延型ヒント** を使うと、既存データを書き換えずに、メタデータのみの操作として型ヒントを追加できます。

* **古いパーツ**: 型ヒントは、`Dynamic` から指定された型へのキャストによってクエリ時に適用されます
* **新しいパーツ**: 型ヒントは `INSERT` 時に materialize されます
* **マージ**: 型ヒントはパーツのマージ時に materialize されます

つまり、型ヒントを即座に追加でき、通常のバックグラウンドマージに伴ってデータは徐々に変換されます。

<div id="enabling-lazy-type-hints">
  ### Lazy Type Hints を有効にする
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### 例
</div>

```sql title="Query"
-- Create a table and insert data
CREATE TABLE test_lazy (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_lazy VALUES ('{"user_id": "123", "score": "95.5"}');

-- Enable experimental setting
SET allow_experimental_json_lazy_type_hints = 1;

-- Add type hints - this completes instantly without mutation
ALTER TABLE test_lazy MODIFY COLUMN json JSON(user_id UInt64, score Float64);

-- Query the data - type hints are applied at read time
SELECT json.user_id, toTypeName(json.user_id), json.score, toTypeName(json.score) FROM test_lazy;
```

```text title="Response"
┌─json.user_id─┬─toTypeName(json.user_id)─┬─json.score─┬─toTypeName(json.score)─┐
│          123 │ UInt64                   │       95.5 │ Float64                │
└──────────────┴──────────────────────────┴────────────┴────────────────────────┘
```

<div id="verifying-no-mutation-occurred">
  ### ミューテーションが発生していないことを確認する
</div>

`system.mutations` テーブルを確認すると、`ALTER` がミューテーションを伴わずに完了したことを確認できます。

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

Lazy Type Hints を有効にすると、このクエリは行を返さず、この操作がメタデータのみの操作だったことが確認できます。

<div id="materializing-type-hints">
  ### 型ヒントのマテリアライズ
</div>

既存データの型ヒントをマテリアライズするには、次のいずれかの方法を使用します。

1. **バックグラウンドマージを待つ**: ClickHouse はパーツのマージ時に、自動的に型ヒントをマテリアライズします
2. **マージを強制する**: `OPTIMIZE TABLE test_lazy FINAL` を使用して、すべてのパーツを直ちにマージします
3. **パーツを書き換える**: `ALTER TABLE test_lazy REWRITE PARTS` を使用して、新しいメタデータでパーツを書き換えます

<div id="lazy-type-hints-limitations">
  ### 制限事項
</div>

* この機能は実験的なものであり、今後のバージョンで変更される可能性があります
* クエリ時の型変換は、事前にマテリアライズされた型と比べて、特に大きな JSON オブジェクトでは大きな性能オーバーヘッドが生じる可能性があります
* この機能が適用されるのは、`typed_paths` (型ヒント) を変更する場合のみです。`max_dynamic_paths`、`SKIP`、`SKIP REGEXP` などの他の JSON パラメータでは、引き続き mutation が必要です

<div id="comparison-between-values-of-the-json-type">
  ## JSON 型の値の比較
</div>

JSON オブジェクトは、Map と同様に比較されます。

たとえば:

```sql title="Query"
CREATE TABLE test (json1 JSON, json2 JSON) ENGINE=Memory;
INSERT INTO test FORMAT JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : [1, 2, 3]}}
{"json1" : {"a" : 42}, "json2" : {"a" : "Hello"}}
{"json1" : {"a" : 42}, "json2" : {"b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41, "b" : 42}}

SELECT json1, json2, json1 < json2, json1 = json2, json1 > json2 FROM test;
```

```text title="Response"
┌─json1──────┬─json2───────────────┬─less(json1, json2)─┬─equals(json1, json2)─┬─greater(json1, json2)─┐
│ {}         │ {}                  │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {}                  │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"41"}          │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"42"}          │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {"a":["1","2","3"]} │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"Hello"}       │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"b":"42"}          │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"42","b":"42"} │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"41","b":"42"} │                  0 │                    0 │                     1 │
└────────────┴─────────────────────┴────────────────────┴──────────────────────┴───────────────────────┘
```

**注:** 2 つのパスに含まれる値のデータ型が異なる場合、それらは `Variant` データ型の[比較ルール](/ja/sql-reference/data-types/variant#comparing-values-of-variant-data)に従って比較されます。

<div id="data-skipping-indexes-for-json">
  ## JSONのデータスキッピングインデックス
</div>

[データスキッピングインデックス](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) は、`JSON` カラムに対して次の 3 つの方法で使用できます。

1. **特定のサブカラムに対する索引** — 通常のカラムと同様に、既知の JSON パスに対して標準的なスキップ索引を作成します。これにより、そのパスの *値* に索引が作成されます。
2. **`JSONAllPaths` を使用したパスベースの索引** — 各グラニュールに存在する *パスの集合* に索引を付けることで、クエリ対象のパスを含まないグラニュールをスキップします。
3. **`JSONAllValues` を使用した値ベースの索引** — [テキスト索引](/ja/engines/table-engines/mergetree-family/textindexes.md) を使用して、すべての JSON パスにまたがる *すべての値* に索引を付け、単一の索引で任意の JSON サブカラムに対する全文検索を高速化します。

<div id="json-indexes-on-subcolumns">
  ### 特定のサブカラム上の索引
</div>

通常のカラムと同じ構文で、任意の JSON サブカラムにスキップ索引を作成できます。
[サポートされている索引タイプ](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)であれば、どれでも使用できます (`minmax`、`set`、`bloom_filter`、`tokenbf_v1`、`ngrambf_v1` など) 。

索引式で JSON サブカラムを参照する方法は 2 つあります。

* JSON type hint で宣言された **型付きパス** — `json.a` のように名前で直接アクセスします。
* 明示的にキャストする **動的パス** — `json.b::String` のように `::` キャスト構文を使用します。

`json.a || json.b::String` のように、複数のサブカラムを組み合わせた式を使用することもできます。

<div id="json-indexes-on-subcolumns-example">
  #### 例
</div>

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id UInt32),
    INDEX idx_sensor data.sensor_id TYPE minmax GRANULARITY 1,
    INDEX idx_location data.location::String TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

型付きサブカラム `data.sensor_id` の `minmax` 索引により、スキャン対象は該当するグラニュールに絞り込まれます：

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id < 2;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: minmax GRANULARITY 1
        Parts: 1/2
        Granules: 2/8
```

キャストしたサブカラム `data.location::String` に対する `bloom_filter` 索引も機能します：

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  ### JSONAllPaths を使ったパスベースの索引
</div>

[データスキッピングインデックス](/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) は、[`JSONAllPaths`](/ja/sql-reference/functions/json-functions#JSONAllPaths) 関数を使って `JSON` カラムに対して作成することもできます。
これは、`mapKeys` を使って [`Map`](/ja/sql-reference/data-types/map) カラムにスキップ索引を作成する場合と同様です。索引には各グラニュールに存在する JSON パスの集合が保存され、それを使って、クエリ対象のパスを含みえないグラニュールをスキップします。

<div id="json-indexes-jsonallpaths-supported-types">
  #### 対応している索引タイプ
</div>

`JSONAllPaths` は、次のスキップ索引タイプで使用できます。

* [`bloom_filter`](/ja/engines/table-engines/mergetree-family/mergetree#bloom-filter) — `equals`、`in`、`IS NOT NULL` をサポートします。
* [`tokenbf_v1`](/ja/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — `equals` と `IS NOT NULL` をサポートします。
* [`ngrambf_v1`](/ja/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — `equals` と `IS NOT NULL` をサポートします。
* [`text`](/ja/engines/table-engines/mergetree-family/textindexes) (転置索引) — `equals`、`in`、`IS NOT NULL` をサポートします。

<div id="json-indexes-on-subcolumns-example">
  #### 例
</div>

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

`EXPLAIN indexes = 1` を使うと、スキップ索引が使用されていることを確認できます。あるパスが片方のパーツにしか存在しない場合、索引によってもう片方のパーツはスキップされます：

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

あるパスがいずれのパーツにも存在しない場合、すべてのパーツとグラニュールがスキップされます：

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` でも索引が使用され、パスが存在しないグラニュールは (その場合、値が `NULL` になるため) スキップされます:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallpaths-how-it-works">
  #### 仕組み
</div>

`JSONAllPaths(json_column)` 式は、JSON 値に含まれるすべてのパスを格納した `Array(String)` を返します。
スキップ索引は、これらのパス文字列をそのデータ構造 (bloom filter または転置索引) に格納します。
クエリで `json.some.path` を条件に絞り込むと、索引は各グラニュールについて文字列 `"some.path"` が索引内に存在するかどうかを確認し、存在しないグラニュールをスキップします。

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### 欠落したパスに対する安全性
</div>

JSON path がグラニュール内に存在しない場合、サブカラムは次のように評価されます。

* `Dynamic` 型 (例: `json.path`) および `Nullable` 型のサブカラム (例: `json.path.:Int64`) では `NULL` になります。`NULL` との比較は常に false を返すため、スキップは安全です。
* `Nullable` ではない CAST 式では、その型のデフォルト値になります (例: パスが存在しない場合、`json.path::Int64` は `0` になります) 。この場合、比較する値がデフォルト値と異なるときにのみスキップは安全です。索引はこの違いを自動的に処理します。

<div id="json-indexes-jsonallvalues">
  ### `JSONAllValues` を使った全文検索
</div>

[Text indexes](/ja/engines/table-engines/mergetree-family/textindexes.md) は、[`JSONAllValues`](/ja/sql-reference/functions/json-functions#JSONAllValues) 関数を使用して、JSON カラムに対する全文検索を高速化できます。
`JSONAllValues` は JSON カラム内のすべての値を `Array(String)` として返し、これをテキスト索引で索引化できます。
`JSONAllValues(json_column)` に対する 1 つの索引ですべての JSON パスをカバーできるため、各パスごとに個別の索引を作成しなくても、任意のサブカラムで全文検索を行えます。

詳細と例については、テキスト索引のドキュメントにある [Value-based indexes with JSONAllValues](/ja/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues) を参照してください。

<div id="tips-for-better-usage-of-the-json-type">
  ## JSON 型をより効果的に使用するためのヒント
</div>

`JSON`カラムを作成してデータを読み込む前に、次の点を考慮してください。

* データを調査し、できるだけ多くのパスヒントと型を指定してください。これにより、保存と読み取りの効率が大幅に向上します。
* 必要になるパスと、不要なパスを検討してください。不要なパスは `SKIP` セクションに、必要に応じて `SKIP REGEXP` セクションに指定してください。これにより、ストレージ効率が向上します。
* `max_dynamic_paths` パラメータは、極端に大きな値に設定しないでください。保存と読み取りの効率が低下する可能性があります。
  これはメモリや CPU などのシステムパラメータに大きく依存しますが、一般的な目安としては、ローカルファイルシステムストレージでは `max_dynamic_paths` を 10 000 以下、リモートファイルシステムストレージでは 1024 以下に設定することを推奨します。

<div id="further-reading">
  ## 参考資料
</div>

* [ClickHouse向けに強力な新しいJSONデータ型をどのように構築したか](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [10億ドキュメントJSONチャレンジ: ClickHouse vs. MongoDB、Elasticsearch など](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)