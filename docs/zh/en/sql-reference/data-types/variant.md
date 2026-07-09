---
description: 'ClickHouse 中 Variant 数据类型的文档'
sidebar_label: 'Variant(T1, T2, ...)'
sidebar_position: 40
slug: /sql-reference/data-types/variant
title: 'Variant(T1, T2, ...)'
doc_type: 'reference'
---

该类型表示由其他数据类型组成的联合类型。类型 `Variant(T1, T2, ..., TN)` 表示该类型的每一行
都包含一个值，其类型可以是 `T1`、`T2`、...、`TN` 中的任意一种，或者都不是 (即 `NULL` 值) 。

嵌套类型的顺序无关紧要：Variant(T1, T2) = Variant(T2, T1)。
除 Nullable(...)、LowCardinality(Nullable(...)) 和 Variant(...) 类型外，嵌套类型可以是任意类型。

:::note
不建议将相近的类型用作 Variant (例如不同的数值类型，如 `Variant(UInt32, Int64)`，或不同的日期类型，如 `Variant(Date, DateTime)`) ，
因为处理这类类型的值时可能会产生歧义。默认情况下，创建此类 `Variant` 类型会引发异常，但可以通过设置 `allow_suspicious_variant_types` 启用。
:::

<div id="creating-variant">
  ## 创建 Variant
</div>

在表的列定义中使用 `Variant` 类型：

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v FROM test;
```

```text
┌─v─────────────┐
│ ᴺᵁᴸᴸ          │
│ 42            │
│ Hello, World! │
│ [1,2,3]       │
└───────────────┘
```

对普通列使用 CAST：

```sql
SELECT toTypeName(variant) AS type_name, 'Hello, World!'::Variant(UInt64, String, Array(UInt64)) as variant;
```

```text
┌─type_name──────────────────────────────┬─variant───────┐
│ Variant(Array(UInt64), String, UInt64) │ Hello, World! │
└────────────────────────────────────────┴───────────────┘
```

当参数没有共同类型时，使用函数 `if/multiIf` (需启用设置 `use_variant_as_common_type`) ：

```sql
SET use_variant_as_common_type = 1;
SELECT if(number % 2, number, range(number)) as variant FROM numbers(5);
```

```text
┌─variant───┐
│ []        │
│ 1         │
│ [0,1]     │
│ 3         │
│ [0,1,2,3] │
└───────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL) AS variant FROM numbers(4);
```

```text
┌─variant───────┐
│ 42            │
│ [1,2,3]       │
│ Hello, World! │
│ ᴺᵁᴸᴸ          │
└───────────────┘
```

如果数组元素/map 值不具有共同类型，则可使用 &#39;array/map&#39; 函数 (为此需要启用设置 `use_variant_as_common_type`) ：

```sql
SET use_variant_as_common_type = 1;
SELECT array(range(number), number, 'str_' || toString(number)) as array_of_variants FROM numbers(3);
```

```text
┌─array_of_variants─┐
│ [[],0,'str_0']    │
│ [[0],1,'str_1']   │
│ [[0,1],2,'str_2'] │
└───────────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT map('a', range(number), 'b', number, 'c', 'str_' || toString(number)) as map_of_variants FROM numbers(3);
```

```text
┌─map_of_variants───────────────┐
│ {'a':[],'b':0,'c':'str_0'}    │
│ {'a':[0],'b':1,'c':'str_1'}   │
│ {'a':[0,1],'b':2,'c':'str_2'} │
└───────────────────────────────┘
```

<div id="reading-variant-nested-types-as-subcolumns">
  ## 将 Variant 嵌套类型读取为子列
</div>

Variant 类型支持使用类型名称作为子列，从 Variant 列中读取某一种嵌套类型。
因此，如果有一列 `variant Variant(T1, T2, T3)`，你可以使用语法 `variant.T2` 读取类型为 `T2` 的子列。
如果 `T2` 可以位于 `Nullable` 内部，则该子列的类型为 `Nullable(T2)`；否则为 `T2`。该子列的大小将
与原始 `Variant` 列相同，并且在原始 `Variant` 列类型不是 `T2` 的所有行中包含 `NULL` 值 (如果 `T2` 不能位于 `Nullable`
内部，则为空值) 。

也可以使用函数 `variantElement(variant_column, type_name)` 读取 Variant 子列。

示例：

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v, v.String, v.UInt64, v.`Array(UInt64)` FROM test;
```

```text
┌─v─────────────┬─v.String──────┬─v.UInt64─┬─v.Array(UInt64)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │     ᴺᵁᴸᴸ │ []              │
│ 42            │ ᴺᵁᴸᴸ          │       42 │ []              │
│ Hello, World! │ Hello, World! │     ᴺᵁᴸᴸ │ []              │
│ [1,2,3]       │ ᴺᵁᴸᴸ          │     ᴺᵁᴸᴸ │ [1,2,3]         │
└───────────────┴───────────────┴──────────┴─────────────────┘
```

```sql
SELECT toTypeName(v.String), toTypeName(v.UInt64), toTypeName(v.`Array(UInt64)`) FROM test LIMIT 1;
```

```text
┌─toTypeName(v.String)─┬─toTypeName(v.UInt64)─┬─toTypeName(v.Array(UInt64))─┐
│ Nullable(String)     │ Nullable(UInt64)     │ Array(UInt64)               │
└──────────────────────┴──────────────────────┴─────────────────────────────┘
```

```sql
SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM test;
```

```text
┌─v─────────────┬─variantElement(v, 'String')─┬─variantElement(v, 'UInt64')─┬─variantElement(v, 'Array(UInt64)')─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ []                                 │
│ 42            │ ᴺᵁᴸᴸ                        │                          42 │ []                                 │
│ Hello, World! │ Hello, World!               │                        ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ [1,2,3]                            │
└───────────────┴─────────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
```

要了解每一行中存储的是哪种 Variant 类型，可以使用函数 `variantType(variant_column)`。它会为每一行返回包含 Variant 类型名称的 `Enum` (如果该行为 `NULL`，则返回 `'None'`) 。

示例：

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT variantType(v) FROM test;
```

```text
┌─variantType(v)─┐
│ None           │
│ UInt64         │
│ String         │
│ Array(UInt64)  │
└────────────────┘
```

```sql
SELECT toTypeName(variantType(v)) FROM test LIMIT 1;
```

```text
┌─toTypeName(variantType(v))──────────────────────────────────────────┐
│ Enum8('None' = -1, 'Array(UInt64)' = 0, 'String' = 1, 'UInt64' = 2) │
└─────────────────────────────────────────────────────────────────────┘
```

<div id="conversion-between-a-variant-column-and-other-columns">
  ## Variant 列与其他列之间的转换
</div>

`Variant` 类型的列可以进行 4 种转换。

<div id="converting-a-string-column-to-a-variant-column">
  ### 将 String 列转换为 Variant 列
</div>

从 `String` 转换为 `Variant`，是通过从字符串值中解析出 `Variant` 类型的值来实现的：

```sql
SELECT '42'::Variant(String, UInt64) AS variant, variantType(variant) AS variant_type
```

```text
┌─variant─┬─variant_type─┐
│ 42      │ UInt64       │
└─────────┴──────────────┘
```

```sql
SELECT '[1, 2, 3]'::Variant(String, Array(UInt64)) as variant, variantType(variant) as variant_type
```

```text
┌─variant─┬─variant_type──┐
│ [1,2,3] │ Array(UInt64) │
└─────────┴───────────────┘
```

````sql
SELECT CAST(map('key1', '42', 'key2', 'true', 'key3', '2020-01-01'), 'Map(String, Variant(UInt64, Bool, Date))') AS map_of_variants, mapApply((k, v) -> (k, variantType(v)), map_of_variants) AS map_of_variant_types```
````

```text
┌─map_of_variants─────────────────────────────┬─map_of_variant_types──────────────────────────┐
│ {'key1':42,'key2':true,'key3':'2020-01-01'} │ {'key1':'UInt64','key2':'Bool','key3':'Date'} │
└─────────────────────────────────────────────┴───────────────────────────────────────────────┘
```

如果要在从 `String` 转换为 `Variant` 时禁用解析，可以关闭设置 `cast_string_to_dynamic_use_inference`：

```sql
SET cast_string_to_variant_use_inference = 0;
SELECT '[1, 2, 3]'::Variant(String, Array(UInt64)) as variant, variantType(variant) as variant_type
```

```text
┌─variant───┬─variant_type─┐
│ [1, 2, 3] │ String       │
└───────────┴──────────────┘
```

<div id="converting-an-ordinary-column-to-a-variant-column">
  ### 将普通列转换为 Variant 列
</div>

可以将类型为 `T` 的普通列转换为包含该类型的 `Variant` 列：

```sql
SELECT toTypeName(variant) AS type_name, [1,2,3]::Array(UInt64)::Variant(UInt64, String, Array(UInt64)) as variant, variantType(variant) as variant_name
```

```text
┌─type_name──────────────────────────────┬─variant─┬─variant_name──┐
│ Variant(Array(UInt64), String, UInt64) │ [1,2,3] │ Array(UInt64) │
└────────────────────────────────────────┴─────────┴───────────────┘
```

注意：从 `String` 类型进行转换时始终会经过解析；如果你需要在不经过解析的情况下，将 `String` 列转换为 `Variant` 中的 `String` Variant，可以按如下方式操作：

```sql
SELECT '[1, 2, 3]'::Variant(String)::Variant(String, Array(UInt64), UInt64) as variant, variantType(variant) as variant_type
```

```sql
┌─variant───┬─variant_type─┐
│ [1, 2, 3] │ String       │
└───────────┴──────────────┘
```

<div id="converting-a-variant-column-to-an-ordinary-column">
  ### 将 Variant 列转换为普通列
</div>

可以将 `Variant` 列转换为普通列。在这种情况下，所有嵌套的 Variant 都会转换为目标类型：

```sql
CREATE TABLE test (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('42.42');
SELECT v::Nullable(Float64) FROM test;
```

```text
┌─CAST(v, 'Nullable(Float64)')─┐
│                         ᴺᵁᴸᴸ │
│                           42 │
│                        42.42 │
└──────────────────────────────┘
```

<div id="converting-a-variant-to-another-variant">
  ### 将一个 Variant 转换为另一个 Variant
</div>

可以将 `Variant` 列转换为另一个 `Variant` 列，但前提是目标 `Variant` 列包含原始 `Variant` 中的所有嵌套类型：

```sql
CREATE TABLE test (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('String');
SELECT v::Variant(UInt64, String, Array(UInt64)) FROM test;
```

```text
┌─CAST(v, 'Variant(UInt64, String, Array(UInt64))')─┐
│ ᴺᵁᴸᴸ                                              │
│ 42                                                │
│ String                                            │
└───────────────────────────────────────────────────┘
```

<div id="reading-variant-type-from-the-data">
  ## 从数据中读取 Variant 类型
</div>

所有文本格式 (TSV、CSV、CustomSeparated、Values、JSONEachRow 等) 都支持读取 `Variant` 类型。解析数据时，ClickHouse 会尝试将值写入最合适的 Variant 类型。

示例：

```sql
SELECT
    v,
    variantElement(v, 'String') AS str,
    variantElement(v, 'UInt64') AS num,
    variantElement(v, 'Float64') AS float,
    variantElement(v, 'DateTime') AS date,
    variantElement(v, 'Array(UInt64)') AS arr
FROM format(JSONEachRow, 'v Variant(String, UInt64, Float64, DateTime, Array(UInt64))', $$
{"v" : "Hello, World!"},
{"v" : 42},
{"v" : 42.42},
{"v" : "2020-01-01 00:00:00"},
{"v" : [1, 2, 3]}
$$)
```

```text
┌─v───────────────────┬─str───────────┬──num─┬─float─┬────────────────date─┬─arr─────┐
│ Hello, World!       │ Hello, World! │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │                ᴺᵁᴸᴸ │ []      │
│ 42                  │ ᴺᵁᴸᴸ          │   42 │  ᴺᵁᴸᴸ │                ᴺᵁᴸᴸ │ []      │
│ 42.42               │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │ 42.42 │                ᴺᵁᴸᴸ │ []      │
│ 2020-01-01 00:00:00 │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │ 2020-01-01 00:00:00 │ []      │
│ [1,2,3]             │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │                ᴺᵁᴸᴸ │ [1,2,3] │
└─────────────────────┴───────────────┴──────┴───────┴─────────────────────┴─────────┘
```

<div id="comparing-values-of-variant-data">
  ## 比较 Variant 类型的值
</div>

`Variant` 类型的值只能与相同 `Variant` 类型的值比较。

默认情况下，比较运算符会使用[Variant 的默认实现](#functions-with-variant-arguments)，
分别对每个 Variant 类型进行比较。可以通过将设置 `use_variant_default_implementation_for_comparisons = 0`
禁用此行为，以使用下面描述的原生 Variant 比较规则。**注意**，`ORDER BY` 始终使用原生比较。

**原生 Variant 比较规则：**

对于类型 `Variant(..., T1, ... T2, ...)` 中，底层类型为 `T1` 的值 `v1` 和底层类型为 `T2` 的值 `v2`，运算符 `<` 的结果定义如下：

* 如果 `T1 = T2 = T`，结果为 `v1.T < v2.T` (即比较底层值) 。
* 如果 `T1 != T2`，结果为 `T1 < T2` (即比较类型名称) 。

示例：

```sql
SET allow_suspicious_types_in_order_by = 1;
CREATE TABLE test (v1 Variant(String, UInt64, Array(UInt32)), v2 Variant(String, UInt64, Array(UInt32))) ENGINE=Memory;
INSERT INTO test VALUES (42, 42), (42, 43), (42, 'abc'), (42, [1, 2, 3]), (42, []), (42, NULL);
```

```sql
SELECT v2, variantType(v2) AS v2_type FROM test ORDER BY v2;
```

```text
┌─v2──────┬─v2_type───────┐
│ []      │ Array(UInt32) │
│ [1,2,3] │ Array(UInt32) │
│ abc     │ String        │
│ 42      │ UInt64        │
│ 43      │ UInt64        │
│ ᴺᵁᴸᴸ    │ None          │
└─────────┴───────────────┘
```

```sql
SELECT v1, variantType(v1) AS v1_type, v2, variantType(v2) AS v2_type, v1 = v2, v1 < v2, v1 > v2 FROM test;
```

```text
┌─v1─┬─v1_type─┬─v2──────┬─v2_type───────┬─equals(v1, v2)─┬─less(v1, v2)─┬─greater(v1, v2)─┐
│ 42 │ UInt64  │ 42      │ UInt64        │              1 │            0 │               0 │
│ 42 │ UInt64  │ 43      │ UInt64        │              0 │            1 │               0 │
│ 42 │ UInt64  │ abc     │ String        │              0 │            0 │               1 │
│ 42 │ UInt64  │ [1,2,3] │ Array(UInt32) │              0 │            0 │               1 │
│ 42 │ UInt64  │ []      │ Array(UInt32) │              0 │            0 │               1 │
│ 42 │ UInt64  │ ᴺᵁᴸᴸ    │ None          │              0 │            1 │               0 │
└────┴─────────┴─────────┴───────────────┴────────────────┴──────────────┴─────────────────┘

```

如果你需要查找 `Variant` 值为特定值的行，可以采用以下任一方法：

* 将该值转换为对应的 `Variant` 类型：

```sql
SELECT * FROM test WHERE v2 == [1,2,3]::Array(UInt32)::Variant(String, UInt64, Array(UInt32));
```

```text
┌─v1─┬─v2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

* 将 `Variant` 子列与所需类型比较：

```sql
SELECT * FROM test WHERE v2.`Array(UInt32)` == [1,2,3] -- or using variantElement(v2, 'Array(UInt32)')
```

```text
┌─v1─┬─v2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

有时，对 Variant 类型再做一次额外检查会很有用，因为像 `Array/Map/Tuple` 这样的复杂类型子列不能放在 `Nullable` 中，因此对于类型不同的行，会返回默认值而不是 `NULL`：

```sql
SELECT v2, v2.`Array(UInt32)`, variantType(v2) FROM test WHERE v2.`Array(UInt32)` == [];
```

```text
┌─v2───┬─v2.Array(UInt32)─┬─variantType(v2)─┐
│ 42   │ []               │ UInt64          │
│ 43   │ []               │ UInt64          │
│ abc  │ []               │ String          │
│ []   │ []               │ Array(UInt32)   │
│ ᴺᵁᴸᴸ │ []               │ None            │
└──────┴──────────────────┴─────────────────┘
```

```sql
SELECT v2, v2.`Array(UInt32)`, variantType(v2) FROM test WHERE variantType(v2) == 'Array(UInt32)' AND v2.`Array(UInt32)` == [];
```

```text
┌─v2─┬─v2.Array(UInt32)─┬─variantType(v2)─┐
│ [] │ []               │ Array(UInt32)   │
└────┴──────────────────┴─────────────────┘
```

**注意：** 数值类型不同的 Variant 值会被视为不同的 Variant，彼此之间不进行比较，而是比较它们的类型名称。

示例：

```sql
SET allow_suspicious_variant_types = 1;
CREATE TABLE test (v Variant(UInt32, Int64)) ENGINE=Memory;
INSERT INTO test VALUES (1::UInt32), (1::Int64), (100::UInt32), (100::Int64);
SELECT v, variantType(v) FROM test ORDER by v;
```

```text
┌─v───┬─variantType(v)─┐
│ 1   │ Int64          │
│ 100 │ Int64          │
│ 1   │ UInt32         │
│ 100 │ UInt32         │
└─────┴────────────────┘
```

**注意** 默认情况下，不允许将 `Variant` 类型用于 `GROUP BY`/`ORDER BY` 键；如果要使用它，请注意其特殊的比较规则，并启用 `allow_suspicious_types_in_group_by`/`allow_suspicious_types_in_order_by` 设置。

<div id="jsonextract-functions-with-variant">
  ## 接受 Variant 参数的 JSONExtract 函数
</div>

所有 `JSONExtract*` 函数都支持 `Variant` 类型：

```sql
SELECT JSONExtract('{"a" : [1, 2, 3]}', 'a', 'Variant(UInt32, String, Array(UInt32))') AS variant, variantType(variant) AS variant_type;
```

```text
┌─variant─┬─variant_type──┐
│ [1,2,3] │ Array(UInt32) │
└─────────┴───────────────┘
```

```sql
SELECT JSONExtract('{"obj" : {"a" : 42, "b" : "Hello", "c" : [1,2,3]}}', 'obj', 'Map(String, Variant(UInt32, String, Array(UInt32)))') AS map_of_variants, mapApply((k, v) -> (k, variantType(v)), map_of_variants) AS map_of_variant_types
```

```text
┌─map_of_variants──────────────────┬─map_of_variant_types────────────────────────────┐
│ {'a':42,'b':'Hello','c':[1,2,3]} │ {'a':'UInt32','b':'String','c':'Array(UInt32)'} │
└──────────────────────────────────┴─────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractKeysAndValues('{"a" : 42, "b" : "Hello", "c" : [1,2,3]}', 'Variant(UInt32, String, Array(UInt32))') AS variants, arrayMap(x -> (x.1, variantType(x.2)), variants) AS variant_types
```

```text
┌─variants───────────────────────────────┬─variant_types─────────────────────────────────────────┐
│ [('a',42),('b','Hello'),('c',[1,2,3])] │ [('a','UInt32'),('b','String'),('c','Array(UInt32)')] │
└────────────────────────────────────────┴───────────────────────────────────────────────────────┘
```

<div id="functions-with-variant-arguments">
  ## 带有 Variant 参数的函数
</div>

ClickHouse 中的大多数函数都会通过 **Variant 的默认实现** 自动支持 `Variant` 类型参数。
从 `26.1` 版本开始，当某个未显式处理 Variant 类型的函数接收到 Variant 列时，ClickHouse 会：

1. 从 Variant 列中提取每一种 Variant 类型
2. 针对每一种 Variant 类型分别执行该函数
3. 根据结果类型对结果进行适当合并

这样一来，你就可以直接将常规函数用于 Variant 列，而无需进行特殊处理。

**示例：**

```sql
CREATE TABLE test (v Variant(UInt32, String)) ENGINE = Memory;
INSERT INTO test VALUES (42), ('hello'), (NULL);
SELECT *, toTypeName(v) FROM test WHERE v = 42;
```

```text
   ┌─v──┬─toTypeName(v)───────────┐
1. │ 42 │ Variant(String, UInt32) │
   └────┴─────────────────────────┘
```

比较运算符会自动分别应用到每种 Variant 类型，因此可以对 Variant 列进行过滤。

**结果类型行为：**

结果类型取决于函数对每种 variant 返回的结果：

* **结果类型不同**：`Variant(T1, T2, ...)`

  ```sql
  CREATE TABLE test2 (v Variant(UInt64, Float64)) ENGINE = Memory;
  INSERT INTO test2 VALUES (42::UInt64), (42.42);
  SELECT v + 1 AS result, toTypeName(result) FROM test2;
  ```

  ```text
  ┌─result─┬─toTypeName(plus(v, 1))──┐
  │     43 │ Variant(Float64, UInt64) │
  │  43.42 │ Variant(Float64, UInt64) │
  └────────┴─────────────────────────┘
  ```

* **类型不兼容**：对于不兼容的 variant，结果为 `NULL`

  ```sql
  CREATE TABLE test3 (v Variant(Array(UInt32), UInt32)) ENGINE = Memory;
  INSERT INTO test3 VALUES ([1,2,3]), (42);
  SELECT v + 10 AS result, toTypeName(result) FROM test3;
  ```

  ```text
  ┌─result─┬─toTypeName(plus(v, 10))─┐
  │   ᴺᵁᴸᴸ │ Nullable(UInt64)        │
  │     52 │ Nullable(UInt64)        │
  └────────┴─────────────────────────┘
  ```

:::note
**错误处理：** 当函数无法处理某种 variant 类型时，只会捕获与类型相关的错误 (ILLEGAL&#95;TYPE&#95;OF&#95;ARGUMENT,
TYPE&#95;MISMATCH, CANNOT&#95;CONVERT&#95;TYPE, NO&#95;COMMON&#95;TYPE) ，并将这些行的结果设为 NULL。其他错误 (如
除零或内存不足) 仍会正常抛出，以避免悄然掩盖真实问题。
:::

<div id="variant-type-mismatch-behavior">
  ### 类型不匹配时的行为
</div>

设置 `variant_throw_on_type_mismatch` 用于控制：当函数应用于 `Variant` 列，且某一行实际存储的类型与该函数不兼容时，该如何处理：

* `true` (默认) —— 在遇到第一行类型不兼容的行时抛出异常 (`ILLEGAL_TYPE_OF_ARGUMENT`) 。
* `false` —— 对类型不兼容的行返回 `NULL`，并保留兼容行的结果。

**示例：**

```sql
CREATE TABLE test (v Variant(String, UInt64)) ENGINE = Memory;
INSERT INTO test VALUES ('hello'), (42), ('foo');

-- Default (throw on mismatch): length() does not accept UInt64, so the query throws.
SELECT length(v) FROM test;  -- throws ILLEGAL_TYPE_OF_ARGUMENT

-- With throw disabled: incompatible rows return NULL.
SET variant_throw_on_type_mismatch = false;
SELECT v, length(v) FROM test ORDER BY v::String NULLS LAST;
```

```text
┌─v─────┬─length(v)─┐
│ foo   │         3 │
│ hello │         5 │
│ 42    │      ᴺᵁᴸᴸ │
└───────┴───────────┘
```