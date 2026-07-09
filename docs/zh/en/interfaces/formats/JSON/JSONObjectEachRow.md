---
alias: []
description: 'JSONObjectEachRow 格式文档'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

在这种格式中，所有数据都表示为单个 JSON 对象，其中每一行都作为该对象中的一个独立字段表示，类似于 [`JSONEachRow`](./JSONEachRow.md) 格式。

<div id="example-usage">
  ## 使用示例
</div>

<div id="basic-example">
  ### 基本示例
</div>

给定如下 JSON：

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

如果要将对象名称用作列值，可以使用特殊设置 [`format_json_object_each_row_column_for_object_name`](/zh/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name)。
该设置的值应设为某一列的名称，在结果对象中，该列会作为某一行的 JSON 键。

<div id="output">
  #### 输出
</div>

假设我们有一个名为 `test` 的表，其中包含两列：

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

我们以 `JSONObjectEachRow` 格式输出，并使用 `format_json_object_each_row_column_for_object_name` 设置：

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### 输入
</div>

假设我们将上一个示例的输出保存到了名为 `data.json` 的文件中：

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

这同样适用于 schema 推断：

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### 插入数据
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse 允许：

* 对象中的键值对按任意顺序出现。
* 省略某些值。

ClickHouse 会忽略元素之间的空格以及对象后的逗号。你可以将所有对象都放在同一行中传入，无需用换行符将它们分隔开。

<div id="omitted-values-processing">
  #### 省略值的处理
</div>

ClickHouse 会用对应[数据类型](/zh/sql-reference/data-types/index.md)的默认值来填补省略的值。

如果指定了 `DEFAULT expr`，ClickHouse 会根据 [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/zh/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) 设置采用不同的填补规则。

考虑下表：

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* 如果 `input_format_defaults_for_omitted_fields = 0`，则 `x` 和 `a` 的默认值都为 `0` (即 `UInt32` 数据类型的默认值) 。
* 如果 `input_format_defaults_for_omitted_fields = 1`，则 `x` 的默认值为 `0`，但 `a` 的默认值为 `x * 2`。

:::note
当使用 `input_format_defaults_for_omitted_fields = 1` 插入数据时，相比使用 `input_format_defaults_for_omitted_fields = 0` 插入数据，ClickHouse 会消耗更多计算资源。
:::

<div id="json-selecting-data">
  ### 查询数据
</div>

以 `UserActivity` 表为例：

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

查询 `SELECT * FROM UserActivity FORMAT JSONEachRow` 返回：

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

与 [JSON](/zh/interfaces/formats/JSON) 格式不同，这里不会替换无效的 UTF-8 序列。值的转义方式与 `JSON` 相同。

:::info
字符串中可以输出任意字节序列。如果你确定表中的数据可以在不丢失任何信息的情况下格式化为 JSON，请使用 [`JSONEachRow`](./JSONEachRow.md) 格式。
:::

<div id="jsoneachrow-nested">
  ### Nested 结构的用法
</div>

如果你的表包含 [`Nested`](/zh/sql-reference/data-types/nested-data-structures/index.md) 数据类型的列，则可以插入具有相同结构的 JSON 数据。通过 [input&#95;format&#95;import&#95;nested&#95;json](/zh/operations/settings/settings-formats.md/#input_format_import_nested_json) 设置启用此功能。

例如，考虑下面这张表：

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

如 `Nested` 数据类型说明所示，ClickHouse 会将嵌套结构中的每个组成部分视为单独的一列 (在我们的表中，即 `n.s` 和 `n.i`) 。你可以按如下方式插入数据：

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

要以层次结构的 JSON 对象形式插入数据，请设置 [`input_format_import_nested_json=1`](/zh/operations/settings/settings-formats.md/#input_format_import_nested_json)。

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

如果未设置此项，ClickHouse 会抛出异常。

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                                                                                                                           | 描述                                                                                                 | 默认值      | 注释                                                                                                                                                 |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`input_format_import_nested_json`](/zh/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | 将嵌套的 JSON 数据映射到嵌套表中 (适用于 JSONEachRow 格式) 。                                                         | `false`  |                                                                                                                                                    |
| [`input_format_json_read_bools_as_numbers`](/zh/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | 允许在 JSON 输入格式中将布尔值解析为数值。                                                                           | `true`   |                                                                                                                                                    |
| [`input_format_json_read_bools_as_strings`](/zh/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | 允许在 JSON 输入格式中将布尔值作为 String 解析。                                                                    | `true`   |                                                                                                                                                    |
| [`input_format_json_read_numbers_as_strings`](/zh/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | 允许在 JSON 输入格式中将数值作为 String 解析。                                                                     | `true`   |                                                                                                                                                    |
| [`input_format_json_read_arrays_as_strings`](/zh/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | 允许在 JSON 输入格式中将 JSON 数组作为 String 解析。                                                               | `true`   |                                                                                                                                                    |
| [`input_format_json_read_objects_as_strings`](/zh/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | 允许在 JSON 输入格式中将 JSON 对象解析为字符串。                                                                     | `true`   |                                                                                                                                                    |
| [`input_format_json_named_tuples_as_objects`](/zh/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | 将命名元组列解析为 JSON 对象。                                                                                 | `true`   |                                                                                                                                                    |
| [`input_format_json_try_infer_numbers_from_strings`](/zh/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | 在进行 schema 推断时，尝试从字符串字段中推断数值。                                                                      | `false`  |                                                                                                                                                    |
| [`input_format_json_try_infer_named_tuples_from_objects`](/zh/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | 在 schema 推断期间，尝试从 JSON 对象中推断命名元组。                                                                  | `true`   |                                                                                                                                                    |
| [`input_format_json_infer_incomplete_types_as_strings`](/zh/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | 在 JSON 输入格式的 schema 推断期间，对于仅包含 NULL 或空对象/数组的键，使用 String 类型。                                        | `true`   |                                                                                                                                                    |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/zh/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | 在解析命名元组时，为 JSON 对象中缺失的元素插入默认值。                                                                     | `true`   |                                                                                                                                                    |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/zh/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | 在 JSON object 中解析命名元组时，忽略未知键。                                                                      | `false`  |                                                                                                                                                    |
| [`input_format_json_compact_allow_variable_number_of_columns`](/zh/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | 允许 JSONCompact/JSONCompactEachRow format 中的列数可变，忽略多余列，并对缺失列使用默认值。                                  | `false`  |                                                                                                                                                    |
| [`input_format_json_throw_on_bad_escape_sequence`](/zh/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | 如果 JSON string 包含无效的转义序列，则抛出异常。若禁用，错误的转义序列将原样保留在数据中。                                               | `true`   |                                                                                                                                                    |
| [`input_format_json_empty_as_default`](/zh/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | 将 JSON 输入中的空字段视为默认值。                                                                               | `false`. | 对于复杂的默认表达式，还必须启用 [`input_format_defaults_for_omitted_fields`](/zh/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)。 |
| [`output_format_json_quote_64bit_integers`](/zh/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | 控制 JSON 输出格式中 64 位整数是否加引号。                                                                         | `true`   |                                                                                                                                                    |
| [`output_format_json_quote_64bit_floats`](/zh/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | 控制 JSON 输出格式中 64 位浮点数是否加引号。                                                                        | `false`  |                                                                                                                                                    |
| [`output_format_json_quote_denormals`](/zh/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | 在 JSON 输出格式中启用 &#39;+nan&#39;、&#39;-nan&#39;、&#39;+inf&#39;、&#39;-inf&#39; 的输出。                    | `false`  |                                                                                                                                                    |
| [`output_format_json_quote_decimals`](/zh/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | 控制 JSON 输出格式中 Decimal 值是否加引号。                                                                      | `false`  |                                                                                                                                                    |
| [`output_format_json_escape_forward_slashes`](/zh/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | 控制 JSON 输出格式中字符串输出里的正斜杠是否转义。                                                                       | `true`   |                                                                                                                                                    |
| [`output_format_json_named_tuples_as_objects`](/zh/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | 将命名元组列序列化为 JSON 对象。                                                                                | `true`   |                                                                                                                                                    |
| [`output_format_json_array_of_rows`](/zh/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | 以 JSONEachRow(Compact) 格式将所有行输出为一个 JSON 数组。                                                        | `false`  |                                                                                                                                                    |
| [`output_format_json_validate_utf8`](/zh/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | 启用对 JSON 输出格式中 UTF-8 序列的校验 (请注意，这不会影响 JSON/JSONCompact/JSONColumnsWithMetadata 格式，它们始终会校验 UTF-8) 。 | `false`  |                                                                                                                                                    |