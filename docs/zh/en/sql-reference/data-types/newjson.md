---
description: 'ClickHouse 中 JSON 数据类型的文档，提供对 JSON 数据处理的原生支持'
keywords: ['json', '数据类型']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'JSON 数据类型'
doc_type: 'reference'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="查看我们的 JSON 最佳实践指南，了解使用 JSON 类型的示例、高级功能及相关注意事项。" icon="book" infoText="阅读更多" infoUrl="/docs/best-practices/use-json-where-appropriate" title="想找指南？" />
</Link>

<br />

`JSON` 类型将 JavaScript Object Notation (JSON) 文档存储在单列中。

:::note
在 ClickHouse 开源版中，JSON 数据类型自 25.3 版本起被标记为可用于生产环境。在更早的版本中，不建议在生产环境中使用此类型。
:::

要声明 `JSON` 类型的列，可以使用以下语法：

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

上述语法中的参数定义如下：

| 参数                          | 说明                                                                                                                                                                                                                                        | 默认值    |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `max_dynamic_paths`         | 一个可选参数，用于指定在单个独立存储的数据块中，有多少路径可以作为子列单独存储 (例如，在 MergeTree 表的单个数据分区片段中) 。<br /><br />如果超过此限制，其余所有路径都会统一存储到一个名为[共享数据](#shared-data-structure)的结构中。<br /><br />此外，也可以通过一些[方法](#controlling-the-number-of-dynamic-paths)在不修改此参数的情况下调整动态路径数量的限制。 | `1024` |
| `max_dynamic_types`         | 一个取值范围为 `1` 到 `255` 的可选参数，用于指定在单个独立存储的数据块中，类型为 `Dynamic` 的单个路径列内可以单独存储多少种不同的数据类型 (例如，在 MergeTree 表的单个数据分区片段中) 。<br /><br />如果超过此限制，所有新类型都会统一存储到一个名为 `shared variant` 的结构中。                                                                | `32`   |
| `some.path TypeName`        | JSON 中特定路径的可选类型提示。此类路径将始终以指定类型作为子列存储。                                                                                                                                                                                                     |        |
| `SKIP path.to.skip`         | 针对特定路径的可选提示，用于指定在 JSON 解析期间跳过该路径。此类路径永远不会存储在 JSON 列中。如果指定的路径是嵌套的 JSON 对象，则整个嵌套对象都会被跳过。                                                                                                                                                    |        |
| `SKIP REGEXP 'path_regexp'` | 一个带有正则表达式的可选提示，用于在 JSON 解析期间跳过路径。所有匹配该正则表达式的路径都不会存储在 JSON 列中。                                                                                                                                                                             |        |

<WhenToUseJson />

<div id="creating-json">
  ## 创建 `JSON`
</div>

本节将介绍创建 `JSON` 的各种方法。

<div id="using-json-in-a-table-column-definition">
  ### 在表的列定义中使用 `JSON`
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
  ### 使用 `::JSON` 进行 CAST
</div>

可以使用特殊语法 `::JSON` 对各种类型执行转换。

<div id="cast-from-string-to-json">
  #### 使用 CAST 将 `String` 转换为 `JSON`
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
  #### 将 `Tuple` CAST 为 `JSON`
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
  #### 将 `Map` CAST 为 `JSON`
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
JSON paths 以扁平化形式存储。这意味着，当根据 `a.b.c` 这样的路径将一个 JSON 对象格式化出来时，
无法判断该对象应构造成 `{ "a.b.c" : ... }`，还是 `{ "a": { "b": { "c": ... } } }`。
我们的实现始终假定为后者。

例如：

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

将返回：

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

而 **非**：

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## 将 JSON 路径 作为子列读取
</div>

`JSON` 类型支持将每个 路径 作为单独的子列读取。
如果请求的 路径 类型未在 JSON 类型声明中指定，
则该 路径 对应的子列将始终为 [Dynamic](/zh/sql-reference/data-types/dynamic.md) 类型。

例如：

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

你也可以使用 `getSubcolumn` 函数读取 JSON 类型中的子列：

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

如果在数据中找不到所请求的路径，则会用 `NULL` 值填充：

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

我们来查看返回的子列的数据类型：

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

正如我们所见，对于 `a.b`，其类型是 `UInt32`，这正是我们在 JSON 类型 声明中指定的类型；
而所有其他子列的类型都是 `Dynamic`。

也可以使用特殊语法 `json.some.path.:TypeName` 来读取 `Dynamic` 类型的子列：

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

`Dynamic` 子列可以转换为任何数据类型。在这种情况下，如果 `Dynamic` 内部类型无法转换为所请求的类型，则会抛出异常：

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
要高效地从 Compact MergeTree parts 中读取子列，请确保已启用 MergeTree 设置 [write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts)。
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## 将 JSON 子对象读取为子列
</div>

`JSON` 类型支持使用特殊语法 `json.^some.path`，将嵌套对象作为 `JSON` 类型的子列读取：

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
当路径存储在基础 (`map`) [共享数据](#shared-data-structure)中时，读取子对象子列的效率可能不高，因为这需要扫描整个共享数据结构。而使用 `map_with_buckets` 或 `advanced` 共享数据序列化时，从共享数据中读取子列则会经过高度优化。
:::

<div id="reading-json-combined-sub-columns">
  ## 读取 JSON 组合子列
</div>

`JSON` 类型支持使用特殊语法 `json.@some.path`，将某个 路径 作为**组合子列**读取。
给定 路径 的组合子列会返回：

* 如果该 路径 上有字面值，则以 `Dynamic` 形式返回存储在该 路径 上的字面值。
* 如果该 路径 上没有字面值，但存在嵌套子路径，则以 `Dynamic` 形式返回该 路径 上的 JSON 子对象。
* 如果该 路径 上既没有字面值，也不存在任何子路径，则返回 `NULL`。

当某个 路径 在不同行中可能保存标量值或嵌套对象时，这种方式非常有用；相比于分别查询字面子列 (`json.a`) 和子对象子列 (`json.^a`) ，这样也更方便。

下面的示例比较了 路径 `a` 的这三种子列类型：

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

* 第 1 行：`a` 的值是字面量 `42`。`json.a` 将其作为 `Dynamic(Int64)` 返回，`json.^a` 返回空子对象 `{}` (`a` 下没有嵌套键) ，`json.@a` 返回字面量 `42`。
* 第 2 行：`a` 的值是一个嵌套对象。`json.a` 返回 `NULL` (该路径上没有字面量) ，`json.^a` 将该子对象作为 `JSON` 返回，`json.@a` 也将该子对象作为 `Dynamic(JSON)` 返回。
* 第 3 行：`a` 完全不存在。`json.a` 和 `json.@a` 都返回 `NULL`，而 `json.^a` 返回空的 `{}`。

:::note
当路径存储在基本 (`map`) [共享数据](#shared-data-structure)中时，读取组合子列的效率可能较低，因为这需要扫描整个共享数据结构。使用 `map_with_buckets` 或 `advanced` 共享数据序列化时，从共享数据中读取子列会得到高度优化。
:::

<div id="type-inference-for-paths">
  ## 路径的类型推断
</div>

在解析 `JSON` 时，ClickHouse 会尝试为每个 JSON 路径推断出最合适的数据类型。
其工作方式与[从输入数据自动推断 schema](/zh/interfaces/schema-inference.md)类似，
并由相同的设置控制：

* [input&#95;format&#95;try&#95;infer&#95;dates](/zh/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/zh/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/zh/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/zh/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/zh/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/zh/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/zh/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/zh/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/zh/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/zh/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

下面来看一些示例：

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
  ## 处理 JSON 对象数组
</div>

包含对象数组的 JSON 路径会被解析为 `Array(JSON)` 类型，并写入该路径对应的 `Dynamic` 列中。
要读取对象数组，可以将其作为子列从 `Dynamic` 列中提取出来：

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

如你所见，与默认值相比，嵌套 `JSON` 类型的 `max_dynamic_types`/`max_dynamic_paths` 参数已被调低。
这是为了避免 JSON 对象的嵌套数组中的子列数量失控增长。

让我们尝试从嵌套的 `JSON` 列中读取子列：

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

我们可以使用一种特殊语法，避免写出 `Array(JSON)` 子列名称：

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

路径后面的 `[]` 数量表示数组层级。例如，`json.path[][]` 会被转换为 `json.path.:Array(Array(JSON))`

让我们来查看 `Array(JSON)` 内部的路径和类型：

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

下面来从 `Array(JSON)` 列中读取子列：

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

我们还可以从嵌套的 `JSON` 列中读取子对象的子列：

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
  ## 处理值为 NULL 的 JSON 键
</div>

在我们的 JSON 实现中，`null` 和值缺失被视为等同：

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

这意味着，无法判断原始 JSON 数据中某个路径是存在且值为 NULL，还是根本不存在。

<div id="handling-json-keys-with-dots">
  ## 处理带点号的 JSON 键
</div>

JSON 列在内部会以扁平化形式存储所有路径和值。这意味着在默认情况下，这 2 个对象会被视为相同：

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

它们在内部都会存储为一对：路径 `a.b` 和值 `42`。在对 JSON 进行格式化时，我们始终根据由点分隔的路径部分构造嵌套对象：

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

如你所见，原始 JSON `{"a.b" : 42}` 现在会被格式化为 `{"a" : {"b" : 42}}`。

这一限制还会导致像下面这样的合法 JSON 对象解析失败：

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

如果你想保留带点的键，并避免将其格式化为嵌套对象，可以启用
设置 [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/zh/operations/settings/formats#json_type_escape_dots_in_keys) (自 `25.8` 版本起可用) 。在这种情况下，在解析时，JSON 键中的所有点都会被
转义为 `%2E`，并在格式化时再还原回来。

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

要将带有转义点号的键读取为子列，必须在子列名中使用转义点号：

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

注意：由于标识符解析器和 analyzer 的限制，子列 `` json.`a.b` `` 等同于子列 `json.a.b`，因此无法读取包含转义点的路径：

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

另外，如果你想为包含带点键名的 JSON path 指定 hint (或在 `SKIP`/`SKIP REGEX` 部分中使用它) ，则必须在 hint 中对点号进行转义：

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
  ## 从数据中读取 JSON 类型
</div>

所有文本格式
([`JSONEachRow`](/zh/interfaces/formats/JSONEachRow),
[`TSV`](/zh/interfaces/formats/TabSeparated),
[`CSV`](/zh/interfaces/formats/CSV),
[`CustomSeparated`](/zh/interfaces/formats/CustomSeparated),
[`Values`](/zh/interfaces/formats/Values) 等) 都支持读取 `JSON` 类型。

示例：

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

在 `CSV`/`TSV`/等文本格式中，`JSON` 会从包含 JSON 对象的字符串中解析：

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
  ## 达到 JSON 内部动态路径上限
</div>

`JSON` 数据类型在内部只能将有限数量的路径存储为独立的子列。
默认情况下，此上限为 `1024`，但你可以在类型声明中通过参数 `max_dynamic_paths` 修改它。

达到该上限后，所有新插入到 `JSON` 列中的路径都会存储在单个共享数据结构中。
这些路径仍然可以作为子列读取，
但效率可能会更低 ([参见关于共享数据的章节](#shared-data-structure)) 。
之所以需要这一限制，是为了避免产生数量庞大的不同子列，从而导致表无法使用。

下面来看几个不同场景下，达到该上限时会发生什么。

<div id="reaching-the-limit-during-data-parsing">
  ### 在数据解析过程中达到限制
</div>

在从数据中解析 `JSON` 对象时，如果当前数据块已达到限制，
所有新路径都会存储在共享数据结构中。我们可以使用以下两个内部信息函数 `JSONDynamicPaths`、`JSONSharedDataPaths`：

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

正如我们所见，在插入路径 `e` 和 `f.g` 后，已达到限制，
因此它们被插入到共享数据结构中。

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### MergeTree 表引擎中的数据分区片段合并期间
</div>

在 `MergeTree` 表中合并多个数据分区片段时，结果数据分区片段中的 `JSON` 列可能会达到动态路径数量的限制，
因此无法将源数据分区片段中的所有路径都存储为子列。
在这种情况下，ClickHouse 会决定哪些路径在合并后继续保留为子列，哪些路径会存储在共享数据结构中。
大多数情况下，ClickHouse 会尽量保留包含
最多非 NULL 值的路径，并将最少见的路径移到共享数据结构中。不过，这也取决于具体实现。

下面来看一个这样的合并示例。
首先，创建一个包含 `JSON` 列的表，将动态路径数量限制设置为 `3`，然后插入包含 `5` 个不同路径的值：

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

每次插入都会创建一个独立的数据分区片段，其中 `JSON` 列只包含一个路径：

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

现在，我们把所有 parts 合并成一个，看看会发生什么：

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

正如我们所见，ClickHouse 保留了最常见的路径 `a`、`b` 和 `c`，并将路径 `d` 和 `e` 转移到了共享数据结构中。

<div id="shared-data-structure">
  ## 共享数据结构
</div>

如前一节所述，当达到 `max_dynamic_paths` 限制时，所有新路径都会存储在同一个共享数据结构中。
本节将介绍共享数据结构的细节，以及如何从中读取路径子列。

有关用于检查 JSON 列内容的函数的详细信息，请参见[“内部信息函数”](/zh/sql-reference/data-types/newjson#introspection-functions)一节。

<div id="shared-data-structure-in-memory">
  ### 内存中的共享数据结构
</div>

在内存中，共享数据结构本质上只是一个类型为 `Map(String, String)` 的子列，用于存储从扁平化 JSON 路径到二进制编码值的映射。
要从中提取某个路径子列，我们只需遍历该 `Map` 列中的所有行，并尝试找到请求的路径及其对应的值。

<div id="shared-data-structure-in-merge-tree-parts">
  ### MergeTree parts 中的共享数据结构
</div>

在 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表中，数据存储在 parts 中，所有内容都会写入磁盘 (本地或远程) 。而磁盘上的数据存储方式可能与内存中不同。
目前，MergeTree parts 中的共享数据结构有 3 种不同的序列化方式：`map`、`map_with_buckets`
和 `advanced`。

序列化版本由 MergeTree
设置 [object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
和 [object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
控制 (零级 parts 是向表中插入数据时创建的 parts；在合并过程中，parts 会具有更高的级别) 。

注意：仅支持为 `v3` [object serialization version](../../operations/settings/merge-tree-settings.md#object_serialization_version)
更改共享数据结构的序列化方式

<div id="shared-data-map">
  #### Map
</div>

在 `map` 序列化版本中，共享数据会被序列化为单个 `Map(String, String)` 类型的列，与其在内存中的存储形式相同。
要从这种序列化中读取路径子列，ClickHouse 需要读取整个 `Map` 列，
并在内存中提取所请求的路径。

这种序列化方式对数据写入和读取整个 `JSON` 列都很高效，但不适合读取路径子列。

<div id="shared-data-map-with-buckets">
  #### 带桶的 Map
</div>

在 `map_with_buckets` 序列化版本中，共享数据会被序列化为 `N` 个列 (“桶”) ，类型为 `Map(String, String)`。
每个桶只包含部分路径。要从这种序列化中读取路径子列，ClickHouse
会从单个桶中读取整个 `Map` 列，并在内存中提取所需的路径。

这种序列化在写入数据和读取整个 `JSON` 列时效率较低，但在读取路径子列时效率更高，
因为它只会从所需的桶中读取数据。

桶的数量 `N` 由 MergeTree settings [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (默认值为 8)
和 [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (默认值为 32) 控制。
这两个设置允许的最大值均为 256。

<div id="shared-data-advanced">
  #### 高级
</div>

在 `advanced` 序列化版本中，共享数据会被序列化为一种特殊的数据结构。该结构通过存储一些额外信息，最大限度提升路径子列的读取性能，从而只读取所请求路径的数据。
这种序列化也支持桶，因此每个桶只包含部分路径。

这种序列化在写入数据时效率较低 (因此不建议将这种序列化用于零级 parts) ；读取整个 `JSON` 列时，与 `map` 序列化相比效率会略低一些，但在读取路径子列时非常高效。

注意：由于这种数据结构中存储了额外信息，与
`map` 和 `map_with_buckets` 序列化相比，使用这种序列化会占用更多磁盘存储空间。

如需进一步了解新的共享数据序列化的概述和实现细节，请阅读这篇[博客文章](https://clickhouse.com/blog/json-data-type-gets-even-better)。

<div id="controlling-the-number-of-dynamic-paths">
  ## 控制 MergeTree parts 中 JSON 内动态路径的数量
</div>

限制 JSON 中动态路径数量的主要方式，是在 JSON type 声明中使用 `max_dynamic_paths` 参数。
但是，要修改现有列的 `max_dynamic_paths`，需要执行 `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)`，这会启动后台变更，并重写所有现有 parts。
这种变更可能非常耗费资源，并且在完成之前会影响 server 性能。为避免这种情况，你可以使用以下 3 个 setting，来调整 MergeTree 表中新 parts 的动态路径数量限制：

* `merge_max_dynamic_subcolumns_in_wide_part` - 一个 MergeTree setting，用于限制合并到 Wide parts 时每个 JSON 列的动态子列数量。
* `merge_max_dynamic_subcolumns_in_compact_part` - 一个 MergeTree setting，用于限制合并到 Compact parts 时每个 JSON 列的动态子列数量。
* `max_dynamic_subcolumns_in_json_type_parsing` - 一个 session setting，用于限制将 JSON 数据解析到 JSON 列时每个 JSON 列的动态子列数量。

注意：动态路径的数量限制不能超过 `max_dynamic_paths` 参数中指定的值，即使上述 setting 的值更高也是如此。

<div id="introspection-functions">
  ## 内部信息函数
</div>

有几个函数可用于检查 JSON 列中的内容：

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**示例**

我们来检查 [GH Archive](https://www.gharchive.org/) 数据集中 `2020-01-01` 这一天的内容：

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
  ## 将 COLUMN 的类型通过 ALTER MODIFY 修改为 JSON 类型
</div>

可以修改现有表，将列的类型更改为新的 `JSON` 类型。目前仅支持从 `String` 类型执行 `ALTER`。

**示例**

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
  ## 惰性类型提示 (Experimental)
</div>

:::note
此功能仍处于实验阶段，需要启用设置 `allow_experimental_json_lazy_type_hints`。
:::

当你使用 `ALTER TABLE ... MODIFY COLUMN` 为 JSON 列添加或修改类型提示时，ClickHouse 通常会重写所有 parts，以物化新的类型提示。对于包含海量历史数据 (数百 TB) 的表，这样做的代价可能极其高昂。

**惰性类型提示**允许仅以元数据操作的方式添加类型提示，而无需重写现有数据：

* **旧 parts**：类型提示会在查询时通过将 `Dynamic` 转换为提示的类型来应用
* **新 parts**：类型提示会在 `INSERT` 操作期间被物化
* **合并**：类型提示会在 parts 合并时被物化

这意味着你可以立即添加类型提示，而数据会随着常规后台合并的进行逐步完成转换。

<div id="enabling-lazy-type-hints">
  ### 启用惰性类型提示
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### 示例
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
  ### 验证未发生变更
</div>

你可以通过检查 `system.mutations` 表，确认 `ALTER` 在未触发变更的情况下已完成：

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

启用惰性类型提示后，此查询不会返回任何行，这说明该操作仅修改了元数据。

<div id="materializing-type-hints">
  ### 类型提示物化
</div>

要将现有数据中的类型提示物化，您可以采用以下任一方式：

1. **等待后台合并**：ClickHouse 会在 parts 合并时自动将类型提示物化
2. **强制合并**：使用 `OPTIMIZE TABLE test_lazy FINAL` 立即合并所有 parts
3. **重写 parts**：使用 `ALTER TABLE test_lazy REWRITE PARTS` 按新的元数据重写 parts

<div id="lazy-type-hints-limitations">
  ### 限制
</div>

* 此功能为 Experimental，未来版本中可能会有所变化
* 与预先 materialized 的类型相比，查询时进行类型转换可能会带来显著的性能开销，尤其是在处理大型 JSON 对象时
* 此功能仅适用于修改 `typed_paths` (类型提示) 时；其他 JSON 参数 (如 `max_dynamic_paths`、`SKIP` 或 `SKIP REGEXP`) 仍需通过变更来处理

<div id="comparison-between-values-of-the-json-type">
  ## JSON 类型值的比较
</div>

JSON 对象的比较方式与 Map 类似。

例如：

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

**注意：**当两个路径包含的数据值属于不同的数据类型时，会根据 `Variant` 数据类型的[比较规则](/zh/sql-reference/data-types/variant#comparing-values-of-variant-data)进行比较。

<div id="data-skipping-indexes-for-json">
  ## JSON 的数据跳过索引
</div>

[数据跳过索引](/zh/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)可通过以下三种方式用于 `JSON` 列：

1. **特定子列上的索引** — 在已知的 JSON 路径上创建标准跳过索引，就像对普通列所做的那样。这会为该路径上的*值*建立索引。
2. **使用 `JSONAllPaths` 的基于路径的索引** — 为每个粒度中存在的*路径集合*建立索引，从而跳过不可能包含所查询路径的粒度。
3. **使用 `JSONAllValues` 的基于值的索引** — 使用[文本索引](/zh/engines/table-engines/mergetree-family/textindexes.md)为所有 JSON 路径中的*所有值*建立索引，从而仅用一个索引即可加速对任意 JSON 子列的全文搜索。

<div id="json-indexes-on-subcolumns">
  ### 特定子列上的索引
</div>

你可以像为普通列创建索引一样，使用相同的语法为任何 JSON 子列创建跳过索引。
任何[受支持的索引类型](/zh/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)都适用 (`minmax`、`set`、`bloom_filter`、`tokenbf_v1`、`ngrambf_v1` 等) 。

在索引表达式中引用 JSON 子列有两种方式：

* 在 JSON 类型提示中声明的**类型化路径**——可直接按名称访问：`json.a`。
* 带显式转换的**动态路径**——使用 `::` 转换语法：`json.b::String`。

你也可以使用组合多个子列的表达式，例如 `json.a || json.b::String`。

<div id="json-indexes-on-subcolumns-example">
  #### 示例
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

类型化子列 `data.sensor_id` 上的 `minmax` 索引会将扫描限定到匹配的粒度：

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

对经类型转换的子列 `data.location::String` 使用的 `bloom_filter` 索引也同样生效：

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
  ### 使用 JSONAllPaths 的基于路径的索引
</div>

也可以使用 [`JSONAllPaths`](/zh/sql-reference/functions/json-functions#JSONAllPaths) 函数在 `JSON` 列上创建[数据跳过索引](/zh/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)。
其工作方式与通过 `mapKeys` 在 [`Map`](/zh/sql-reference/data-types/map) 列上创建跳过索引类似——索引会存储每个粒度中存在的 JSON 路径集合，并据此跳过不可能包含所查询路径的粒度。

<div id="json-indexes-jsonallpaths-supported-types">
  #### 支持的索引类型
</div>

`JSONAllPaths` 可用于以下跳过索引类型：

* [`bloom_filter`](/zh/engines/table-engines/mergetree-family/mergetree#bloom-filter) — 支持 `equals`、`in` 和 `IS NOT NULL`。
* [`tokenbf_v1`](/zh/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — 支持 `equals` 和 `IS NOT NULL`。
* [`ngrambf_v1`](/zh/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — 支持 `equals` 和 `IS NOT NULL`。
* [`text`](/zh/engines/table-engines/mergetree-family/textindexes) (倒排索引) — 支持 `equals`、`in` 和 `IS NOT NULL`。

<div id="json-indexes-on-subcolumns-example">
  #### 示例
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

你可以使用 `EXPLAIN indexes = 1` 来确认是否用到了跳过索引。当某个路径仅存在于一个分片中时，索引会跳过另一个分片：

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

当某个路径在任何 part 中都不存在时，所有 parts 和粒度都会被跳过：

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

`IS NOT NULL` 也会使用该索引——它会跳过不存在该路径的粒度 (因为其值将为 `NULL`) ：

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
  #### 工作原理
</div>

`JSONAllPaths(json_column)` 表达式会生成一个 `Array(String)`，其中包含 JSON 值中存在的所有路径。
跳过索引会将这些路径字符串存储在其数据结构中 (布隆过滤器或倒排索引) 。
当查询按 `json.some.path` 进行过滤时，索引会检查每个粒度的索引中是否存在字符串 `"some.path"`，并跳过不存在该字符串的粒度。

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### 缺失路径时的安全性
</div>

当某个 JSON path 在一个粒度中不存在时，该子列的求值结果为：

* 对于 `Dynamic` 类型 (例如 `json.path`) 以及 `Nullable` 类型的子列 (例如 `json.path.:Int64`) ，结果为 `NULL` —— 与 `NULL` 的比较始终返回 false，因此可以安全地进行 skipping。
* 对于非 `Nullable` 的 CAST expressions，结果为该类型的默认值 (例如，路径缺失时 `json.path::Int64` 会产生 `0`) —— 只有当比较值不同于默认值时，skipping 才是安全的。索引会自动处理这种差异。

<div id="json-indexes-jsonallvalues">
  ### 使用 JSONAllValues 进行全文检索
</div>

[文本索引](/zh/engines/table-engines/mergetree-family/textindexes.md)可通过 [`JSONAllValues`](/zh/sql-reference/functions/json-functions#JSONAllValues) 函数来加速对 JSON 列的全文检索。
`JSONAllValues` 会将 JSON 列中的所有值以 `Array(String)` 形式返回，这些值可以由文本索引建立索引。
在 `JSONAllValues(json_column)` 上创建一个索引即可覆盖所有 JSON 路径，因此无需为每个路径单独创建索引，就能对任意子列进行全文检索。

详细信息和示例请参阅文本索引文档中的[使用 JSONAllValues 的基于值的索引](/zh/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues)。

<div id="tips-for-better-usage-of-the-json-type">
  ## 更好地使用 JSON 类型的技巧
</div>

在创建 `JSON` 列并向其中加载数据之前，请参考以下建议：

* 先分析你的数据，并尽可能为更多路径指定类型提示。这会显著提升存储和读取效率。
* 想清楚哪些路径是你会用到的，哪些路径是你永远不会用到的。在 `SKIP` 部分中指定不需要的路径；如有需要，也可以在 `SKIP REGEXP` 部分中指定。这将提升存储效率。
* 不要将 `max_dynamic_paths` 参数设置得过高，否则会降低存储和读取效率。
  虽然这在很大程度上取决于内存、CPU 等系统参数，但一个通用的经验法则是：对于本地文件系统存储，不要将 `max_dynamic_paths` 设置为大于 10 000；对于远程文件系统存储，不要将其设置为大于 1024。

<div id="further-reading">
  ## 延伸阅读
</div>

* [我们如何为 ClickHouse 打造出一种全新且强大的 JSON 数据类型](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [十亿文档 JSON 挑战：ClickHouse vs. MongoDB、Elasticsearch 等](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)