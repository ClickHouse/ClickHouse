#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
#include <Core/Field.h>
#include <Common/SipHash.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <base/find_symbols.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeDynamic::DataTypeDynamic(size_t max_dynamic_types_) : max_dynamic_types(max_dynamic_types_)
{
}

MutableColumnPtr DataTypeDynamic::createColumn() const
{
    return ColumnDynamic::create(max_dynamic_types);
}

String DataTypeDynamic::doGetName() const
{
    if (max_dynamic_types == DEFAULT_MAX_DYNAMIC_TYPES)
        return "Dynamic";
    return "Dynamic(max_types=" + toString(max_dynamic_types) + ")";
}

void DataTypeDynamic::updateHashImpl(SipHash & hash) const
{
    hash.update(max_dynamic_types);
}

Field DataTypeDynamic::getDefault() const
{
    return Field(Null());
}

SerializationPtr DataTypeDynamic::doGetSerialization(const SerializationInfoSettings & settings) const
{
    if (settings.propagate_types_serialization_versions_to_nested_types)
        return SerializationDynamic::create(max_dynamic_types, settings);
    return SerializationDynamic::create(max_dynamic_types);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDynamic>();

    if (arguments->children.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Dynamic data type can have only one optional argument - the maximum number of dynamic types in a form 'Dynamic(max_types=N)");


    const auto * argument = arguments->children[0]->as<ASTFunction>();
    if (!argument || argument->name != "equals")
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Dynamic data type argument should be in a form 'max_types=N'");

    /// The `equals` function expects exactly two children: an identifier and a literal.
    /// Validate the argument list shape before indexing.
    if (!argument->arguments || argument->arguments->children.size() != 2)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Dynamic data type argument should be in a form 'max_types=N'");

    const auto & identifier_node = argument->arguments->children[0];
    const auto * identifier = identifier_node->as<ASTIdentifier>();
    if (!identifier)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected Dynamic type argument: {}. Expected expression 'max_types=N'", identifier_node->formatForErrorMessage());

    auto identifier_name = identifier->name();
    if (identifier_name != "max_types")
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected identifier: {}. Dynamic data type argument should be in a form 'max_types=N'", identifier_name);

    auto * literal = argument->arguments->children[1]->as<ASTLiteral>();

    if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.safeGet<UInt64>() > ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "'max_types' argument for Dynamic type should be a positive integer between 0 and {}", ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT);

    return std::make_shared<DataTypeDynamic>(literal->value.safeGet<UInt64>());
}

void registerDataTypeDynamic(DataTypeFactory & factory)
{
    factory.registerDataType("Dynamic", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
This type allows to store values of any type inside it without knowing all of them in advance.

To declare a column of `Dynamic` type, use the following syntax:

```sql
<column_name> Dynamic(max_types=N)
```

Where `N` is an optional parameter between `0` and `254` indicating how many different data types can be stored as separate subcolumns inside a column with type `Dynamic` across single block of data that is stored separately (for example across single data part for MergeTree table). If this limit is exceeded, all values with new types will be stored together in a special shared data structure in binary form. Default value of `max_types` is `32`.

## Creating Dynamic {#creating-dynamic}

Using `Dynamic` type in table column definition:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d─────────────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ          │ None           │
│ 42            │ Int64          │
│ Hello, World! │ String         │
│ [1,2,3]       │ Array(Int64)   │
└───────────────┴────────────────┘
```

Using CAST from ordinary column:

```sql
SELECT 'Hello, World!'::Dynamic AS d, dynamicType(d);
```

```text
┌─d─────────────┬─dynamicType(d)─┐
│ Hello, World! │ String         │
└───────────────┴────────────────┘
```

Using CAST from `Variant` column:

```sql
SET use_variant_as_common_type = 1;
SELECT multiIf((number % 3) = 0, number, (number % 3) = 1, range(number + 1), NULL)::Dynamic AS d, dynamicType(d) FROM numbers(3)
```

```text
┌─d─────┬─dynamicType(d)─┐
│ 0     │ UInt64         │
│ [0,1] │ Array(UInt64)  │
│ ᴺᵁᴸᴸ  │ None           │
└───────┴────────────────┘
```

## Reading Dynamic nested types as subcolumns {#reading-dynamic-nested-types-as-subcolumns}

`Dynamic` type supports reading a single nested type from a `Dynamic` column using the type name as a subcolumn.
So, if you have column `d Dynamic` you can read a subcolumn of any valid type `T` using syntax `d.T`,
this subcolumn will have type `Nullable(T)` if `T` can be inside `Nullable` and `T` otherwise. This subcolumn will
be the same size as original `Dynamic` column and will contain `NULL` values (or empty values if `T` cannot be inside `Nullable`)
in all rows in which original `Dynamic` column doesn't have type `T`.

`Dynamic` subcolumns can be also read using function `dynamicElement(dynamic_column, type_name)`.

Examples:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, dynamicType(d), d.String, d.Int64, d.`Array(Int64)`, d.Date, d.`Array(String)` FROM test;
```

```text
┌─d─────────────┬─dynamicType(d)─┬─d.String──────┬─d.Int64─┬─d.Array(Int64)─┬─d.Date─┬─d.Array(String)─┐
│ ᴺᵁᴸᴸ          │ None           │ ᴺᵁᴸᴸ          │    ᴺᵁᴸᴸ │ []             │   ᴺᵁᴸᴸ │ []              │
│ 42            │ Int64          │ ᴺᵁᴸᴸ          │      42 │ []             │   ᴺᵁᴸᴸ │ []              │
│ Hello, World! │ String         │ Hello, World! │    ᴺᵁᴸᴸ │ []             │   ᴺᵁᴸᴸ │ []              │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ          │    ᴺᵁᴸᴸ │ [1,2,3]        │   ᴺᵁᴸᴸ │ []              │
└───────────────┴────────────────┴───────────────┴─────────┴────────────────┴────────┴─────────────────┘
```

```sql
SELECT toTypeName(d.String), toTypeName(d.Int64), toTypeName(d.`Array(Int64)`), toTypeName(d.Date), toTypeName(d.`Array(String)`)  FROM test LIMIT 1;
```

```text
┌─toTypeName(d.String)─┬─toTypeName(d.Int64)─┬─toTypeName(d.Array(Int64))─┬─toTypeName(d.Date)─┬─toTypeName(d.Array(String))─┐
│ Nullable(String)     │ Nullable(Int64)     │ Array(Int64)               │ Nullable(Date)     │ Array(String)               │
└──────────────────────┴─────────────────────┴────────────────────────────┴────────────────────┴─────────────────────────────┘
```

```sql
SELECT d, dynamicType(d), dynamicElement(d, 'String'), dynamicElement(d, 'Int64'), dynamicElement(d, 'Array(Int64)'), dynamicElement(d, 'Date'), dynamicElement(d, 'Array(String)') FROM test;```
```

```text
┌─d─────────────┬─dynamicType(d)─┬─dynamicElement(d, 'String')─┬─dynamicElement(d, 'Int64')─┬─dynamicElement(d, 'Array(Int64)')─┬─dynamicElement(d, 'Date')─┬─dynamicElement(d, 'Array(String)')─┐
│ ᴺᵁᴸᴸ          │ None           │ ᴺᵁᴸᴸ                        │                       ᴺᵁᴸᴸ │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ 42            │ Int64          │ ᴺᵁᴸᴸ                        │                         42 │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ Hello, World! │ String         │ Hello, World!               │                       ᴺᵁᴸᴸ │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ                        │                       ᴺᵁᴸᴸ │ [1,2,3]                           │                      ᴺᵁᴸᴸ │ []                                 │
└───────────────┴────────────────┴─────────────────────────────┴────────────────────────────┴───────────────────────────────────┴───────────────────────────┴────────────────────────────────────┘
```

To know what variant is stored in each row function `dynamicType(dynamic_column)` can be used. It returns `String` with value type name for each row (or `'None'` if row is `NULL`).

Example:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT dynamicType(d) FROM test;
```

```text
┌─dynamicType(d)─┐
│ None           │
│ Int64          │
│ String         │
│ Array(Int64)   │
└────────────────┘
```

## Conversion between Dynamic column and other columns {#conversion-between-dynamic-column-and-other-columns}

There are 4 possible conversions that can be performed with `Dynamic` column.

### Converting an ordinary column to a Dynamic column {#converting-an-ordinary-column-to-a-dynamic-column}

```sql
SELECT 'Hello, World!'::Dynamic AS d, dynamicType(d);
```

```text
┌─d─────────────┬─dynamicType(d)─┐
│ Hello, World! │ String         │
└───────────────┴────────────────┘
```

### Converting a String column to a Dynamic column through parsing {#converting-a-string-column-to-a-dynamic-column-through-parsing}

To parse `Dynamic` type values from a `String` column you can enable setting `cast_string_to_dynamic_use_inference`:

```sql
SET cast_string_to_dynamic_use_inference = 1;
SELECT CAST(materialize(map('key1', '42', 'key2', 'true', 'key3', '2020-01-01')), 'Map(String, Dynamic)') as map_of_dynamic, mapApply((k, v) -> (k, dynamicType(v)), map_of_dynamic) as map_of_dynamic_types;
```

```text
┌─map_of_dynamic──────────────────────────────┬─map_of_dynamic_types─────────────────────────┐
│ {'key1':42,'key2':true,'key3':'2020-01-01'} │ {'key1':'Int64','key2':'Bool','key3':'Date'} │
└─────────────────────────────────────────────┴──────────────────────────────────────────────┘
```

### Converting a Dynamic column to an ordinary column {#converting-a-dynamic-column-to-an-ordinary-column}

It is possible to convert a `Dynamic` column to an ordinary column. In this case all nested types will be converted to a destination type:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('42.42'), (true), ('e10');
SELECT d::Nullable(Float64) FROM test;
```

```text
┌─CAST(d, 'Nullable(Float64)')─┐
│                         ᴺᵁᴸᴸ │
│                           42 │
│                        42.42 │
│                            1 │
│                            0 │
└──────────────────────────────┘
```

### Converting a Variant column to Dynamic column {#converting-a-variant-column-to-dynamic-column}

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('String'), ([1, 2, 3]);
SELECT v::Dynamic AS d, dynamicType(d) FROM test;
```

```text
┌─d───────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ    │ None           │
│ 42      │ UInt64         │
│ String  │ String         │
│ [1,2,3] │ Array(UInt64)  │
└─────────┴────────────────┘
```

### Converting a Dynamic(max_types=N) column to another Dynamic(max_types=K) {#converting-a-dynamicmax_typesn-column-to-another-dynamicmax_typesk}

If `K >= N` than during conversion the data doesn't change:

```sql
CREATE TABLE test (d Dynamic(max_types=3)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true);
SELECT d::Dynamic(max_types=5) as d2, dynamicType(d2) FROM test;
```

```text
┌─d─────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ  │ None           │
│ 42    │ Int64          │
│ 43    │ Int64          │
│ 42.42 │ String         │
│ true  │ Bool           │
└───────┴────────────────┘
```

If `K < N`, then the values with the rarest types will be inserted into a single special subcolumn, but still will be accessible:
```text
CREATE TABLE test (d Dynamic(max_types=4)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true), ([1, 2, 3]);
SELECT d, dynamicType(d), d::Dynamic(max_types=2) as d2, dynamicType(d2), isDynamicElementInSharedData(d2) FROM test;
```

```text
┌─d───────┬─dynamicType(d)─┬─d2──────┬─dynamicType(d2)─┬─isDynamicElementInSharedData(d2)─┐
│ ᴺᵁᴸᴸ    │ None           │ ᴺᵁᴸᴸ    │ None            │ false                            │
│ 42      │ Int64          │ 42      │ Int64           │ false                            │
│ 43      │ Int64          │ 43      │ Int64           │ false                            │
│ 42.42   │ String         │ 42.42   │ String          │ false                            │
│ true    │ Bool           │ true    │ Bool            │ true                             │
│ [1,2,3] │ Array(Int64)   │ [1,2,3] │ Array(Int64)    │ true                             │
└─────────┴────────────────┴─────────┴─────────────────┴──────────────────────────────────┘
```

Functions `isDynamicElementInSharedData` returns `true` for rows that are stored in a special shared data structure inside `Dynamic` and as we can see, resulting column contains only 2 types that are not stored in shared data structure.

If `K=0`, all types will be inserted into single special subcolumn:

```text
CREATE TABLE test (d Dynamic(max_types=4)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true), ([1, 2, 3]);
SELECT d, dynamicType(d), d::Dynamic(max_types=0) as d2, dynamicType(d2), isDynamicElementInSharedData(d2) FROM test;
```

```text
┌─d───────┬─dynamicType(d)─┬─d2──────┬─dynamicType(d2)─┬─isDynamicElementInSharedData(d2)─┐
│ ᴺᵁᴸᴸ    │ None           │ ᴺᵁᴸᴸ    │ None            │ false                            │
│ 42      │ Int64          │ 42      │ Int64           │ true                             │
│ 43      │ Int64          │ 43      │ Int64           │ true                             │
│ 42.42   │ String         │ 42.42   │ String          │ true                             │
│ true    │ Bool           │ true    │ Bool            │ true                             │
│ [1,2,3] │ Array(Int64)   │ [1,2,3] │ Array(Int64)    │ true                             │
└─────────┴────────────────┴─────────┴─────────────────┴──────────────────────────────────┘
```

## Reading Dynamic type from the data {#reading-dynamic-type-from-the-data}

All text formats (TSV, CSV, CustomSeparated, Values, JSONEachRow, etc) supports reading `Dynamic` type. During data parsing ClickHouse tries to infer the type of each value and use it during insertion to `Dynamic` column.

Example:

```sql
SELECT
    d,
    dynamicType(d),
    dynamicElement(d, 'String') AS str,
    dynamicElement(d, 'Int64') AS num,
    dynamicElement(d, 'Float64') AS float,
    dynamicElement(d, 'Date') AS date,
    dynamicElement(d, 'Array(Int64)') AS arr
FROM format(JSONEachRow, 'd Dynamic', $$
{"d" : "Hello, World!"},
{"d" : 42},
{"d" : 42.42},
{"d" : "2020-01-01"},
{"d" : [1, 2, 3]}
$$)
```

```text
┌─d─────────────┬─dynamicType(d)─┬─str───────────┬──num─┬─float─┬───────date─┬─arr─────┐
│ Hello, World! │ String         │ Hello, World! │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │       ᴺᵁᴸᴸ │ []      │
│ 42            │ Int64          │ ᴺᵁᴸᴸ          │   42 │  ᴺᵁᴸᴸ │       ᴺᵁᴸᴸ │ []      │
│ 42.42         │ Float64        │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │ 42.42 │       ᴺᵁᴸᴸ │ []      │
│ 2020-01-01    │ Date           │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │ 2020-01-01 │ []      │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │       ᴺᵁᴸᴸ │ [1,2,3] │
└───────────────┴────────────────┴───────────────┴──────┴───────┴────────────┴─────────┘
```

## Using Dynamic type in functions {#using-dynamic-type-in-functions}

Most of the functions support arguments with type `Dynamic`. In this case the function is executed separately on each internal data type stored inside `Dynamic` column.
When the result type of the function depends on the arguments types, the result of such function executed with `Dynamic` arguments will be `Dynamic`. When the result type of the function doesn't depend on the arguments types - the result will be `Nullable(T)` where `T` the usual result type of this function.

Examples:

```sql
CREATE TABLE test (d Dynamic) ENGINE=Memory;
INSERT INTO test VALUES (NULL), (1::Int8), (2::Int16), (3::Int32), (4::Int64);
```

```sql
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ │ None           │
│ 1    │ Int8           │
│ 2    │ Int16          │
│ 3    │ Int32          │
│ 4    │ Int64          │
└──────┴────────────────┘
```

```sql
SELECT d, d + 1 AS res, toTypeName(res), dynamicType(res) FROM test;
```

```text
┌─d────┬─res──┬─toTypeName(res)─┬─dynamicType(res)─┐
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Dynamic         │ None             │
│ 1    │ 2    │ Dynamic         │ Int16            │
│ 2    │ 3    │ Dynamic         │ Int32            │
│ 3    │ 4    │ Dynamic         │ Int64            │
│ 4    │ 5    │ Dynamic         │ Int64            │
└──────┴──────┴─────────────────┴──────────────────┘
```

```sql
SELECT d, d + d AS res, toTypeName(res), dynamicType(res) FROM test;
```

```text
┌─d────┬─res──┬─toTypeName(res)─┬─dynamicType(res)─┐
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Dynamic         │ None             │
│ 1    │ 2    │ Dynamic         │ Int16            │
│ 2    │ 4    │ Dynamic         │ Int32            │
│ 3    │ 6    │ Dynamic         │ Int64            │
│ 4    │ 8    │ Dynamic         │ Int64            │
└──────┴──────┴─────────────────┴──────────────────┘
```

```sql
SELECT d, d < 3 AS res, toTypeName(res) FROM test;
```

```text
┌─d────┬──res─┬─toTypeName(res)─┐
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Nullable(UInt8) │
│ 1    │    1 │ Nullable(UInt8) │
│ 2    │    1 │ Nullable(UInt8) │
│ 3    │    0 │ Nullable(UInt8) │
│ 4    │    0 │ Nullable(UInt8) │
└──────┴──────┴─────────────────┘
```

```sql
SELECT d, exp2(d) AS res, toTypeName(res) FROM test;
```

```sql
┌─d────┬──res─┬─toTypeName(res)───┐
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Nullable(Float64) │
│ 1    │    2 │ Nullable(Float64) │
│ 2    │    4 │ Nullable(Float64) │
│ 3    │    8 │ Nullable(Float64) │
│ 4    │   16 │ Nullable(Float64) │
└──────┴──────┴───────────────────┘
```

```sql
TRUNCATE TABLE test;
INSERT INTO test VALUES (NULL), ('str_1'), ('str_2');
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d─────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ  │ None           │
│ str_1 │ String         │
│ str_2 │ String         │
└───────┴────────────────┘
```

```sql
SELECT d, upper(d) AS res, toTypeName(res) FROM test;
```

```text
┌─d─────┬─res───┬─toTypeName(res)──┐
│ ᴺᵁᴸᴸ  │ ᴺᵁᴸᴸ  │ Nullable(String) │
│ str_1 │ STR_1 │ Nullable(String) │
│ str_2 │ STR_2 │ Nullable(String) │
└───────┴───────┴──────────────────┘
```

```sql
SELECT d, extract(d, '([0-3])') AS res, toTypeName(res) FROM test;
```

```text
┌─d─────┬─res──┬─toTypeName(res)──┐
│ ᴺᵁᴸᴸ  │ ᴺᵁᴸᴸ │ Nullable(String) │
│ str_1 │ 1    │ Nullable(String) │
│ str_2 │ 2    │ Nullable(String) │
└───────┴──────┴──────────────────┘
```

```sql
TRUNCATE TABLE test;
INSERT INTO test VALUES (NULL), ([1, 2]), ([3, 4]);
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d─────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ  │ None           │
│ [1,2] │ Array(Int64)   │
│ [3,4] │ Array(Int64)   │
└───────┴────────────────┘
```

```sql
SELECT d, d[1] AS res, toTypeName(res), dynamicType(res) FROM test;
```

```text
┌─d─────┬─res──┬─toTypeName(res)─┬─dynamicType(res)─┐
│ ᴺᵁᴸᴸ  │ ᴺᵁᴸᴸ │ Dynamic         │ None             │
│ [1,2] │ 1    │ Dynamic         │ Int64            │
│ [3,4] │ 3    │ Dynamic         │ Int64            │
└───────┴──────┴─────────────────┴──────────────────┘
```

If function cannot be executed on some type inside `Dynamic` column, the exception will be thrown:

```sql
INSERT INTO test VALUES (42), (43), ('str_1');
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d─────┬─dynamicType(d)─┐
│ 42    │ Int64          │
│ 43    │ Int64          │
│ str_1 │ String         │
└───────┴────────────────┘
┌─d─────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ  │ None           │
│ [1,2] │ Array(Int64)   │
│ [3,4] │ Array(Int64)   │
└───────┴────────────────┘
```

```sql
SELECT d, d + 1 AS res, toTypeName(res), dynamicType(d) FROM test;
```

```text
Received exception:
Code: 43. DB::Exception: Illegal types Array(Int64) and UInt8 of arguments of function plus: while executing 'FUNCTION plus(__table1.d : 3, 1_UInt8 :: 1) -> plus(__table1.d, 1_UInt8) Dynamic : 0'. (ILLEGAL_TYPE_OF_ARGUMENT)
```

We can filter out unneeded types:

```sql
SELECT d, d + 1 AS res, toTypeName(res), dynamicType(res) FROM test WHERE dynamicType(d) NOT IN ('String', 'Array(Int64)', 'None')
```

```text
┌─d──┬─res─┬─toTypeName(res)─┬─dynamicType(res)─┐
│ 42 │ 43  │ Dynamic         │ Int64            │
│ 43 │ 44  │ Dynamic         │ Int64            │
└────┴─────┴─────────────────┴──────────────────┘
```

Or extract required type as subcolumn:

```sql
SELECT d, d.Int64 + 1 AS res, toTypeName(res) FROM test;
```

```text
┌─d─────┬──res─┬─toTypeName(res)─┐
│ 42    │   43 │ Nullable(Int64) │
│ 43    │   44 │ Nullable(Int64) │
│ str_1 │ ᴺᵁᴸᴸ │ Nullable(Int64) │
└───────┴──────┴─────────────────┘
┌─d─────┬──res─┬─toTypeName(res)─┐
│ ᴺᵁᴸᴸ  │ ᴺᵁᴸᴸ │ Nullable(Int64) │
│ [1,2] │ ᴺᵁᴸᴸ │ Nullable(Int64) │
│ [3,4] │ ᴺᵁᴸᴸ │ Nullable(Int64) │
└───────┴──────┴─────────────────┘
```

### Type mismatch behavior {#dynamic-type-mismatch-behavior}

The setting `dynamic_throw_on_type_mismatch` controls what happens when a function is applied to a `Dynamic` column and the actual stored type of a row is incompatible with the function:

- `true` (default) — throw an exception (`ILLEGAL_TYPE_OF_ARGUMENT`) on the first incompatible row.
- `false` — return `NULL` for incompatible rows and keep the result for compatible rows.

**Example:**

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES ('world'), (123), (456);

-- Default (throw on mismatch): length() does not accept integers, so the query throws.
SELECT length(d) FROM test;  -- throws ILLEGAL_TYPE_OF_ARGUMENT

-- With throw disabled: incompatible rows return NULL.
SET dynamic_throw_on_type_mismatch = false;
SELECT d, length(d) FROM test ORDER BY d::String NULLS LAST;
```

```text
┌─d─────┬─length(d)─┐
│ world │         5 │
│ 123   │      ᴺᵁᴸᴸ │
│ 456   │      ᴺᵁᴸᴸ │
└───────┴───────────┘
```

## Using Dynamic type in ORDER BY and GROUP BY {#using-dynamic-type-in-order-by-and-group-by}

During `ORDER BY` and `GROUP BY` values of `Dynamic` types are compared similar to values of `Variant` type:
The result of operator `<` for values `d1` with underlying type `T1` and `d2` with underlying type `T2`  of a type `Dynamic` is defined as follows:
- If `T1 = T2 = T`, the result will be `d1.T < d2.T` (underlying values will be compared).
- If `T1 != T2`, the result will be `T1 < T2` (type names will be compared).

By default `Dynamic` type is not allowed in `GROUP BY`/`ORDER BY` keys, if you want to use it consider its special comparison rule and enable `allow_suspicious_types_in_group_by`/`allow_suspicious_types_in_order_by` settings.

Examples:
```sql
CREATE TABLE test (d Dynamic) ENGINE=Memory;
INSERT INTO test VALUES (42), (43), ('abc'), ('abd'), ([1, 2, 3]), ([]), (NULL);
```

```sql
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d───────┬─dynamicType(d)─┐
│ 42      │ Int64          │
│ 43      │ Int64          │
│ abc     │ String         │
│ abd     │ String         │
│ [1,2,3] │ Array(Int64)   │
│ []      │ Array(Int64)   │
│ ᴺᵁᴸᴸ    │ None           │
└─────────┴────────────────┘
```

```sql
SELECT d, dynamicType(d) FROM test ORDER BY d SETTINGS allow_suspicious_types_in_order_by=1;
```

```sql
┌─d───────┬─dynamicType(d)─┐
│ []      │ Array(Int64)   │
│ [1,2,3] │ Array(Int64)   │
│ 42      │ Int64          │
│ 43      │ Int64          │
│ abc     │ String         │
│ abd     │ String         │
│ ᴺᵁᴸᴸ    │ None           │
└─────────┴────────────────┘
```

**Note:** values of dynamic types with different numeric types are considered as different values and not compared between each other, their type names are compared instead.

Example:

```sql
CREATE TABLE test (d Dynamic) ENGINE=Memory;
INSERT INTO test VALUES (1::UInt32), (1::Int64), (100::UInt32), (100::Int64);
SELECT d, dynamicType(d) FROM test ORDER BY d SETTINGS allow_suspicious_types_in_order_by=1;
```

```text
┌─v───┬─dynamicType(v)─┐
│ 1   │ Int64          │
│ 100 │ Int64          │
│ 1   │ UInt32         │
│ 100 │ UInt32         │
└─────┴────────────────┘
```

```sql
SELECT d, dynamicType(d) FROM test GROUP BY d SETTINGS allow_suspicious_types_in_group_by=1;
```

```text
┌─d───┬─dynamicType(d)─┐
│ 1   │ Int64          │
│ 100 │ UInt32         │
│ 1   │ UInt32         │
│ 100 │ Int64          │
└─────┴────────────────┘
```

**Note:** the described comparison rule is not applied during execution of comparison functions like `<`/`>`/`=` and others because of [special work](#using-dynamic-type-in-functions) of functions with `Dynamic` type

## Reaching the limit in number of different data types stored inside Dynamic {#reaching-the-limit-in-number-of-different-data-types-stored-inside-dynamic}

`Dynamic` data type can store only limited number of different data types as separate subcolumns. By default, this limit is 32, but you can change it in type declaration using syntax `Dynamic(max_types=N)` where N is between 0 and 254 (due to implementation details, it's impossible to have more than 254 different data types that can be stored as separate subcolumns inside Dynamic).
When the limit is reached, all new data types inserted to `Dynamic` column will be inserted into a single shared data structure that stores values with different data types in binary form.

Let's see what happens when the limit is reached in different scenarios.

### Reaching the limit during data parsing {#reaching-the-limit-during-data-parsing}

During parsing of `Dynamic` values from the data, when the limit is reached for current block of data, all new values will be inserted into shared data structure:

```sql
SELECT d, dynamicType(d), isDynamicElementInSharedData(d) FROM format(JSONEachRow, 'd Dynamic(max_types=3)', '
{"d" : 42}
{"d" : [1, 2, 3]}
{"d" : "Hello, World!"}
{"d" : "2020-01-01"}
{"d" : ["str1", "str2", "str3"]}
{"d" : {"a" : 1, "b" : [1, 2, 3]}}
')
```

```text
┌─d──────────────────────┬─dynamicType(d)─────────────────┬─isDynamicElementInSharedData(d)─┐
│ 42                     │ Int64                          │ false                           │
│ [1,2,3]                │ Array(Int64)                   │ false                           │
│ Hello, World!          │ String                         │ false                           │
│ 2020-01-01             │ Date                           │ true                            │
│ ['str1','str2','str3'] │ Array(String)                  │ true                            │
│ (1,[1,2,3])            │ Tuple(a Int64, b Array(Int64)) │ true                            │
└────────────────────────┴────────────────────────────────┴─────────────────────────────────┘
```

As we can see, after inserting 3 different data types `Int64`, `Array(Int64)` and `String` all new types were inserted into special shared data structure.

### During merges of data parts in MergeTree table engines {#during-merges-of-data-parts-in-mergetree-table-engines}

During merge of several data parts in MergeTree table the `Dynamic` column in the resulting data part can reach the limit of different data types that can be stored in separate subcolumns inside and won't be able to store all types as subcolumns from source parts.
In this case ClickHouse chooses what types will remain as separate subcolumns after merge and what types will be inserted into shared data structure. In most cases ClickHouse tries to keep the most frequent types and store the rarest types in shared data structure, but it depends on the implementation.

Let's see an example of such merge. First, let's create a table with `Dynamic` column, set the limit of different data types to `3` and insert values with `5` different types:

```sql
CREATE TABLE test (id UInt64, d Dynamic(max_types=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, number FROM numbers(5);
INSERT INTO test SELECT number, range(number) FROM numbers(4);
INSERT INTO test SELECT number, toDate(number) FROM numbers(3);
INSERT INTO test SELECT number, map(number, number) FROM numbers(2);
INSERT INTO test SELECT number, 'str_' || toString(number) FROM numbers(1);
```

Each insert will create a separate data pert with `Dynamic` column containing single type:
```sql
SELECT count(), dynamicType(d), isDynamicElementInSharedData(d), _part FROM test GROUP BY _part, dynamicType(d), isDynamicElementInSharedData(d) ORDER BY _part, count();
```

```text
┌─count()─┬─dynamicType(d)──────┬─isDynamicElementInSharedData(d)─┬─_part─────┐
│       5 │ UInt64              │ false                           │ all_1_1_0 │
│       4 │ Array(UInt64)       │ false                           │ all_2_2_0 │
│       3 │ Date                │ false                           │ all_3_3_0 │
│       2 │ Map(UInt64, UInt64) │ false                           │ all_4_4_0 │
│       1 │ String              │ false                           │ all_5_5_0 │
└─────────┴─────────────────────┴─────────────────────────────────┴───────────┘
```

Now, let's merge all parts into one and see what will happen:

```sql
SYSTEM START MERGES test;
OPTIMIZE TABLE test FINAL;
SELECT count(), dynamicType(d), isDynamicElementInSharedData(d), _part FROM test GROUP BY _part, dynamicType(d), isDynamicElementInSharedData(d) ORDER BY _part, count() desc;
```

```text
┌─count()─┬─dynamicType(d)──────┬─isDynamicElementInSharedData(d)─┬─_part─────┐
│       5 │ UInt64              │ false                           │ all_1_5_2 │
│       4 │ Array(UInt64)       │ false                           │ all_1_5_2 │
│       3 │ Date                │ false                           │ all_1_5_2 │
│       2 │ Map(UInt64, UInt64) │ true                            │ all_1_5_2 │
│       1 │ String              │ true                            │ all_1_5_2 │
└─────────┴─────────────────────┴─────────────────────────────────┴───────────┘
```

As we can see, ClickHouse kept the most frequent types `UInt64` and `Array(UInt64)` as subcolumns and inserted all other types into shared data.

## JSONExtract functions with Dynamic {#jsonextract-functions-with-dynamic}

All `JSONExtract*` functions support `Dynamic` type:

```sql
SELECT JSONExtract('{"a" : [1, 2, 3]}', 'a', 'Dynamic') AS dynamic, dynamicType(dynamic) AS dynamic_type;
```

```text
┌─dynamic─┬─dynamic_type───────────┐
│ [1,2,3] │ Array(Nullable(Int64)) │
└─────────┴────────────────────────┘
```

```sql
SELECT JSONExtract('{"obj" : {"a" : 42, "b" : "Hello", "c" : [1,2,3]}}', 'obj', 'Map(String, Dynamic)') AS map_of_dynamics, mapApply((k, v) -> (k, dynamicType(v)), map_of_dynamics) AS map_of_dynamic_types
```

```text
┌─map_of_dynamics──────────────────┬─map_of_dynamic_types────────────────────────────────────┐
│ {'a':42,'b':'Hello','c':[1,2,3]} │ {'a':'Int64','b':'String','c':'Array(Nullable(Int64))'} │
└──────────────────────────────────┴─────────────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractKeysAndValues('{"a" : 42, "b" : "Hello", "c" : [1,2,3]}', 'Dynamic') AS dynamics, arrayMap(x -> (x.1, dynamicType(x.2)), dynamics) AS dynamic_types```
```

```text
┌─dynamics───────────────────────────────┬─dynamic_types─────────────────────────────────────────────────┐
│ [('a',42),('b','Hello'),('c',[1,2,3])] │ [('a','Int64'),('b','String'),('c','Array(Nullable(Int64))')] │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────┘
```

### Binary output format {#binary-output-format}

In RowBinary format values of `Dynamic` type are serialized in the following format:

```text
<binary_encoded_data_type><value_in_binary_format_according_to_the_data_type>
```
)DOCS_MD",
            .syntax = "Dynamic",
            .examples = {},
            .related = {"Variant", "JSON"},
        });
}

namespace
{

/// Split Dynamic subcolumn name into 2 parts: type name and subcolumn of this type.
/// We cannot simply split by '.' because type name can also contain dots. For example: Tuple(`a.b` UInt32).
/// But in all such cases this '.' will be inside back quotes. To split subcolumn name correctly
/// we search for the first '.' that is not inside back quotes.
std::pair<std::string_view, std::string_view> splitSubcolumnName(std::string_view subcolumn_name)
{
    bool inside_quotes = false;
    const char * pos = subcolumn_name.data();
    const char * end = subcolumn_name.data() + subcolumn_name.size();
    while (true)
    {
        pos = find_first_symbols<'`', '.', '\\'>(pos, end);
        if (pos == end)
            break;

        if (*pos == '`')
        {
            inside_quotes = !inside_quotes;
            ++pos;
        }
        else if (*pos == '\\')
        {
            ++pos;
        }
        else if (*pos == '.')
        {
            if (inside_quotes)
                ++pos;
            else
                break;
        }
    }

    if (pos == end)
        return {subcolumn_name, {}};

    return {std::string_view(subcolumn_name.data(), pos), std::string_view(pos + 1, end)}; /// NOLINT(bugprone-suspicious-stringview-data-usage)
}

}

std::unique_ptr<IDataType::SubstreamData> DataTypeDynamic::getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const
{
    auto [type_subcolumn_name, subcolumn_nested_name] = splitSubcolumnName(subcolumn_name);
    /// Check if requested subcolumn is a valid data type.
    auto subcolumn_type = DataTypeFactory::instance().tryGet(String(type_subcolumn_name));
    if (!subcolumn_type)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dynamic type doesn't have subcolumn '{}'", type_subcolumn_name);
        return nullptr;
    }

    const auto & dynamic_serialization = assert_cast<const SerializationDynamic &>(*removeNamedSerialization(data.serialization));
    auto subcolumn_serialization = dynamic_serialization.createSerializationForType(subcolumn_type);
    std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(subcolumn_serialization);
    res->type = subcolumn_type;
    std::optional<ColumnVariant::Discriminator> discriminator;
    ColumnPtr null_map_for_variant_from_shared_variant;
    if (data.column)
    {
        /// If column was provided, we should extract subcolumn from Dynamic column.
        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*data.column);
        const auto & variant_info = dynamic_column.getVariantInfo();
        const auto & variant_column = dynamic_column.getVariantColumn();
        const auto & shared_variant = dynamic_column.getSharedVariant();
        /// Check if provided Dynamic column has subcolumn of this type.
        String subcolumn_type_name = subcolumn_type->getName();
        auto it = variant_info.variant_name_to_discriminator.find(subcolumn_type_name);
        if (it != variant_info.variant_name_to_discriminator.end())
        {
            discriminator = it->second;
            res->column = variant_column.getVariantPtrByGlobalDiscriminator(*discriminator);
        }
        /// Otherwise if there is data in shared variant try to find requested type there.
        else if (!shared_variant.empty())
        {
            /// Create null map for resulting subcolumn to make it Nullable.
            auto null_map_column = ColumnUInt8::create();
            NullMap & null_map = assert_cast<ColumnUInt8 &>(*null_map_column).getData();
            null_map.reserve(variant_column.size());
            auto subcolumn = subcolumn_type->createColumn();
            auto shared_variant_local_discr = variant_column.localDiscriminatorByGlobal(dynamic_column.getSharedVariantDiscriminator());
            const auto & local_discriminators = variant_column.getLocalDiscriminators();
            const auto & offsets = variant_column.getOffsets();
            const FormatSettings format_settings;
            for (size_t i = 0; i != local_discriminators.size(); ++i)
            {
                if (local_discriminators[i] == shared_variant_local_discr)
                {
                    auto value = shared_variant.getDataAt(offsets[i]);
                    ReadBufferFromMemory buf(value);
                    auto type = decodeDataType(buf);
                    if (type->getName() == subcolumn_type_name)
                    {
                        subcolumn_serialization->deserializeBinary(*subcolumn, buf, format_settings);
                        null_map.push_back(static_cast<UInt8>(0));
                    }
                    else
                    {
                        null_map.push_back(static_cast<UInt8>(1));
                    }
                }
                else
                {
                    null_map.push_back(static_cast<UInt8>(1));
                }
            }

            res->column = std::move(subcolumn);
            null_map_for_variant_from_shared_variant = std::move(null_map_column);
        }
    }

    /// Extract nested subcolumn of requested dynamic subcolumn if needed.
    /// If requested subcolumn is null map, it's processed separately as there is no Nullable type yet.
    bool is_null_map_subcolumn = subcolumn_nested_name == "null";
    if (is_null_map_subcolumn)
    {
        if (!canExtractedSubcolumnsBeInsideNullable(subcolumn_type))
        {
            if (throw_if_null)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dynamic type doesn't have subcolumn '{}'", subcolumn_name);
            return nullptr;
        }
        res->type = std::make_shared<DataTypeUInt8>();
    }
    else if (!subcolumn_nested_name.empty())
    {
        res = getSubcolumnData(subcolumn_nested_name, *res, initial_array_level, throw_if_null);
        if (!res)
        {
            if (throw_if_null)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Expected getSubcolumnData() to throw for subcolumn '{}' in throw_if_null mode",
                    subcolumn_name);
            return nullptr;
        }
    }

    res->serialization = SerializationDynamicElement::create(
        res->serialization,
        dynamic_serialization.createSerializationForType(ColumnDynamic::getSharedVariantDataType()),
        subcolumn_type->getName(),
        String(subcolumn_nested_name),
        is_null_map_subcolumn);
    /// Make resulting subcolumn Nullable only if type subcolumn can be inside Nullable or can be LowCardinality(Nullable()).
    bool make_subcolumn_nullable = canExtractedSubcolumnsBeInsideNullableOrLowCardinalityNullable(subcolumn_type);
    if (!is_null_map_subcolumn && make_subcolumn_nullable)
        res->type = makeNullableOrLowCardinalityNullableSafe(res->type);

    if (data.column)
    {
        /// Check if provided Dynamic column has subcolumn of this type. In this case we should use VariantSubcolumnCreator/VariantNullMapSubcolumnCreator to
        /// create full subcolumn from variant according to discriminators.
        if (discriminator)
        {
            const auto & variant_column = assert_cast<const ColumnDynamic &>(*data.column).getVariantColumn();
            std::unique_ptr<ISerialization::ISubcolumnCreator> creator;
            if (is_null_map_subcolumn)
                creator = std::make_unique<SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator>(
                    variant_column.getLocalDiscriminatorsPtr(),
                    "",
                    *discriminator,
                    variant_column.localDiscriminatorByGlobal(*discriminator));
            else
                creator = std::make_unique<SerializationVariantElement::VariantSubcolumnCreator>(
                    variant_column.getLocalDiscriminatorsPtr(),
                    "",
                    *discriminator,
                    variant_column.localDiscriminatorByGlobal(*discriminator),
                    make_subcolumn_nullable);
            res->column = creator->create(res->column);
        }
        /// Check if requested type was extracted from shared variant. In this case we should use
        /// VariantSubcolumnCreator to create full subcolumn from variant according to created null map.
        else if (null_map_for_variant_from_shared_variant)
        {
            if (is_null_map_subcolumn)
            {
                res->column = null_map_for_variant_from_shared_variant;
            }
            else
            {
                SerializationVariantElement::VariantSubcolumnCreator creator(
                    null_map_for_variant_from_shared_variant, "", 0, 0, make_subcolumn_nullable, null_map_for_variant_from_shared_variant);
                res->column = creator.create(res->column);
            }
        }
        /// Provided Dynamic column doesn't have subcolumn of this type, just create column filled with default values.
        else if (is_null_map_subcolumn)
        {
            /// Fill null map with 1 when there is no such Dynamic subcolumn.
            auto column = ColumnUInt8::create();
            assert_cast<ColumnUInt8 &>(*column).getData().resize_fill(data.column->size(), 1);
            res->column = std::move(column);
        }
        else
        {
            auto column = res->type->createColumn();
            column->insertManyDefaults(data.column->size());
            res->column = std::move(column);
        }
    }

    return res;
}

bool hasDynamicType(const DataTypePtr & type)
{
    bool result = false;
    auto check = [&](const IDataType & t) { result |= isDynamic(t); };
    check(*type);
    type->forEachChild(check);
    return result;
}

}
