---
description: 'ClickHouse の Variant データ型に関するドキュメント'
sidebar_label: 'Variant(T1, T2, ...)'
sidebar_position: 40
slug: /sql-reference/data-types/variant
title: 'Variant(T1, T2, ...)'
doc_type: 'reference'
---

この型は、他のデータ型のユニオンを表します。型 `Variant(T1, T2, ..., TN)` は、この型の各行が
`T1` 型、`T2` 型、...、`TN` 型のいずれか、またはそのどれにも属さない値 (`NULL` 値) を持つことを意味します。

ネストされた型の順序は重要ではありません。Variant(T1, T2) = Variant(T2, T1) です。
ネストされた型には、Nullable(...)、LowCardinality(Nullable(...))、Variant(...) 型を除き、任意の型を指定できます。

:::note
似た型を variants として使用することは推奨されません (たとえば、`Variant(UInt32, Int64)` のように異なる数値型を組み合わせる場合や、`Variant(Date, DateTime)` のように異なる日付型を組み合わせる場合) 。
このような型の値を扱うと、曖昧さが生じる可能性があるためです。デフォルトでは、このような `Variant` 型を作成すると例外が発生しますが、設定 `allow_suspicious_variant_types` を使用すると有効にできます。
:::

<div id="creating-variant">
  ## Variant の作成
</div>

テーブルのカラム定義で `Variant` 型を使用する場合:

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

通常のカラムに対してCASTを使用する場合:

```sql
SELECT toTypeName(variant) AS type_name, 'Hello, World!'::Variant(UInt64, String, Array(UInt64)) as variant;
```

```text
┌─type_name──────────────────────────────┬─variant───────┐
│ Variant(Array(UInt64), String, UInt64) │ Hello, World! │
└────────────────────────────────────────┴───────────────┘
```

引数に共通の型がない場合に関数 `if/multiIf` を使用するには (この場合、設定 `use_variant_as_common_type` を有効にする必要があります) :

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

配列要素や map の値に共通の型がない場合は、&#39;array/map&#39; 関数を使用します (そのためには、設定 `use_variant_as_common_type` を有効にする必要があります) :

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
  ## Variant のネストされた型をサブカラムとして読み取る
</div>

Variant 型では、型名をサブカラムとして指定することで、Variant カラムから特定のネストされた型を 1 つ読み取ることができます。
たとえば、`variant Variant(T1, T2, T3)` というカラムがある場合、構文 `variant.T2` を使用して型 `T2` のサブカラムを読み取れます。
このサブカラムの型は、`T2` を `Nullable` の内部に含められる場合は `Nullable(T2)`、そうでない場合は `T2` です。このサブカラムは
元の `Variant` カラムと同じサイズになり、元の `Variant` カラムの型が `T2` でないすべての行では `NULL` 値 (または `T2` を `Nullable` の内部に含められない場合は空の値) を含みます。

Variant のサブカラムは、関数 `variantElement(variant_column, type_name)` を使って読み取ることもできます。

例:

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

各行にどの Variant 型が格納されているかを確認するには、関数 `variantType(variant_column)` を使用できます。これは、各行について Variant 型の型名を持つ `Enum` を返します (行が `NULL` の場合は `'None'` を返します) 。

例:

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
  ## Variant カラムと他のカラム間の変換
</div>

型 `Variant` のカラムに対しては、4 つの変換を行えます。

<div id="converting-a-string-column-to-a-variant-column">
  ### String カラムを Variant カラムに変換する
</div>

`String` から `Variant` への変換は、文字列の値をパースして `Variant` 型の値を生成することで行われます。

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

`String` から `Variant` への変換時にパースを無効にするには、設定 `cast_string_to_dynamic_use_inference` を無効化します。

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
  ### 通常のカラムを Variant カラムに変換する
</div>

`T` 型の通常のカラムは、`T` を含む `Variant` カラムに変換できます。

```sql
SELECT toTypeName(variant) AS type_name, [1,2,3]::Array(UInt64)::Variant(UInt64, String, Array(UInt64)) as variant, variantType(variant) as variant_name
```

```text
┌─type_name──────────────────────────────┬─variant─┬─variant_name──┐
│ Variant(Array(UInt64), String, UInt64) │ [1,2,3] │ Array(UInt64) │
└────────────────────────────────────────┴─────────┴───────────────┘
```

注: `String` 型からの変換は常にパースを介して行われます。パースせずに `String` カラムを `Variant` の `String` Variant に変換する必要がある場合は、次のようにします:

```sql
SELECT '[1, 2, 3]'::Variant(String)::Variant(String, Array(UInt64), UInt64) as variant, variantType(variant) as variant_type
```

```sql
┌─variant───┬─variant_type─┐
│ [1, 2, 3] │ String       │
└───────────┴──────────────┘
```

<div id="converting-a-variant-column-to-an-ordinary-column">
  ### Variant カラムを通常のカラムに変換する
</div>

`Variant` カラムは通常のカラムに変換できます。この場合、ネストされたすべての `Variant` は宛先の型に変換されます。

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
  ### Variant を別の Variant に変換する
</div>

`Variant` カラムは別の `Variant` カラムに変換できますが、変換先の `Variant` カラムに元の `Variant` に含まれるすべてのネストされた型が含まれている場合に限られます。

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
  ## データから Variant 型を読み込む
</div>

すべてのテキストフォーマット (TSV、CSV、CustomSeparated、Values、JSONEachRow など) は、`Variant` 型の読み取りをサポートしています。データのパース時に、ClickHouse は値を最も適切な Variant 型に挿入しようとします。

例:

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
  ## Variant 型の値の比較
</div>

`Variant` 型の値は、同じ `Variant` 型の値とのみ比較できます。

デフォルトでは、比較演算子は [Variant のデフォルト実装](#functions-with-variant-arguments) を使用し、
各 Variant 型ごとに個別に比較を行います。これは setting `use_variant_default_implementation_for_comparisons = 0`
を使用して無効にでき、その場合は以下で説明するネイティブな Variant の比較規則が使用されます。**注**: `ORDER BY` は常にネイティブな比較を使用します。

**ネイティブな Variant の比較規則:**

型 `Variant(..., T1, ... T2, ...)` において、内部型 `T1` を持つ値 `v1` と内部型 `T2` を持つ値 `v2` に対する演算子 `<` の結果は、次のように定義されます。

* `T1 = T2 = T` の場合、結果は `v1.T < v2.T` になります (内部値が比較されます) 。
* `T1 != T2` の場合、結果は `T1 < T2` になります (型名が比較されます) 。

例:

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

特定の `Variant` 値を持つ行を見つける必要がある場合は、次のいずれかの方法を使用できます。

* 値を対応する `Variant` 型にキャストします。

```sql
SELECT * FROM test WHERE v2 == [1,2,3]::Array(UInt32)::Variant(String, UInt64, Array(UInt32));
```

```text
┌─v1─┬─v2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

* `Variant` サブカラムを必要な型と比較します:

```sql
SELECT * FROM test WHERE v2.`Array(UInt32)` == [1,2,3] -- or using variantElement(v2, 'Array(UInt32)')
```

```text
┌─v1─┬─v2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

場合によっては、Variant 型に対して追加の確認を行うと便利です。これは、`Array/Map/Tuple` のような複雑な型を持つサブカラムは `Nullable` にできず、型が異なる行では `NULL` ではなくデフォルト値が入るためです。

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

**注:** 数値型が異なる Variant の値は、別の Variant として扱われ、相互に比較されません。代わりに、それらの型名が比較されます。

例:

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

**注:** `Variant` 型はデフォルトでは `GROUP BY`/`ORDER BY` キーで使用できません。使用する場合は、この型に固有の比較ルールを考慮し、`allow_suspicious_types_in_group_by`/`allow_suspicious_types_in_order_by` 設定を有効にしてください。

<div id="jsonextract-functions-with-variant">
  ## Variant に対応した JSONExtract 関数
</div>

すべての `JSONExtract*` 関数は `Variant` 型をサポートしています。

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
  ## Variant 引数を受け取る関数
</div>

ClickHouse のほとんどの関数は、**Variant に対するデフォルト実装**により、`Variant` 型の引数を自動的にサポートします。
バージョン `26.1` 以降では、Variant 型を明示的に処理しない関数に Variant カラムが渡されると、ClickHouse は次のように動作します。

1. Variant カラムから各 Variant 型を抽出する
2. 各 Variant 型ごとに関数を個別に実行する
3. 結果の型に応じて、結果を適切に結合する

これにより、特別な処理をしなくても、通常の関数を Variant カラムに対して使用できます。

**例:**

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

比較演算子は各 Variant 型に個別に自動適用されるため、Variant カラムに対してフィルタリングを行えます。

**結果型の挙動:**

結果型は、関数が各 variant に対して返す値によって決まります。

* **結果型が異なる場合**: `Variant(T1, T2, ...)`

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

* **型の非互換性**: 互換性のない variant では `NULL`

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
**エラー処理:** 関数が Variant 型を処理できない場合、型関連のエラー (ILLEGAL&#95;TYPE&#95;OF&#95;ARGUMENT、
TYPE&#95;MISMATCH、CANNOT&#95;CONVERT&#95;TYPE、NO&#95;COMMON&#95;TYPE) のみが捕捉され、該当する行の結果は NULL になります。ゼロ除算やメモリ不足などのその他のエラーは、実際の問題が気付かれないまま隠れてしまうのを防ぐため、通常どおり送出されます。
:::

<div id="variant-type-mismatch-behavior">
  ### 型不一致時の動作
</div>

設定 `variant_throw_on_type_mismatch` は、関数を `Variant` カラムに適用した際に、各行に実際に格納されている型がその関数に対応していない場合の動作を制御します。

* `true` (デフォルト) — 最初の非互換な行で例外 (`ILLEGAL_TYPE_OF_ARGUMENT`) をスローします。
* `false` — 非互換な行には `NULL` を返し、互換性のある行の結果はそのまま返します。

**例:**

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