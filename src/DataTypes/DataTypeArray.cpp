#include <Columns/ColumnArray.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationNamed.h>

#include <Parsers/IAST.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/NamesAndTypes.h>
#include <Columns/ColumnConst.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
using FieldType = Array;


DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_}
{
}


MutableColumnPtr DataTypeArray::createColumn() const
{
    return ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create());
}

Field DataTypeArray::getDefault() const
{
    return Array();
}


bool DataTypeArray::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
}

void DataTypeArray::updateHashImpl(SipHash & hash) const
{
    nested->updateHash(hash);
}

SerializationPtr DataTypeArray::doGetSerialization(const SerializationInfoSettings & settings) const
{
    if (settings.propagate_types_serialization_versions_to_nested_types)
        return SerializationArray::create(nested->getSerialization(settings));
    return SerializationArray::create(nested->getDefaultSerialization());
}

size_t DataTypeArray::getNumberOfDimensions() const
{
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->getNumberOfDimensions();   /// Every modern C++ compiler optimizes tail recursion.
}

String DataTypeArray::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "Array(" << nested->getPrettyName(indent) << ')';
    return s.str();
}

void DataTypeArray::forEachChild(const ChildCallback & callback) const
{
    callback(*nested);
    nested->forEachChild(callback);
}

std::unique_ptr<ISerialization::SubstreamData> DataTypeArray::getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const
{
    auto nested_type = assert_cast<const DataTypeArray &>(*data.type).nested;
    const auto & array_serialization = assert_cast<const SerializationArray &>(*removeNamedSerialization(data.serialization));
    auto nested_data = std::make_unique<ISerialization::SubstreamData>(array_serialization.getNestedSerialization());
    nested_data->type = nested_type;
    nested_data->column = data.column ? assert_cast<const ColumnArray &>(*data.column).getDataPtr() : nullptr;

    auto nested_subcolumn_data = getSubcolumnData(subcolumn_name, *nested_data, initial_array_level + 1, throw_if_null);
    if (!nested_subcolumn_data)
        return nullptr;

    auto creator = SerializationArray::SubcolumnCreator(data.column ? assert_cast<const ColumnArray &>(*data.column).getOffsetsPtr() : nullptr);
    auto res = std::make_unique<ISerialization::SubstreamData>();
    res->serialization = creator.create(nested_subcolumn_data->serialization, nested_subcolumn_data->type);
    res->type = creator.create(nested_subcolumn_data->type);
    if (data.column)
        res->column = creator.create(nested_subcolumn_data->column);

    return res;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Array data type family must have exactly one argument - type of elements");

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]));
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("Array", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
An array of `T`-type items, with the starting array index as 1. `T` can be any data type, including an array.

## Creating an Array {#creating-an-array}

You can use a function to create an array:

```sql
array(T)
```

You can also use `[]`.

```sql
[]
```

Example of creating an array:

```sql
SELECT array(1, 2) AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

```sql
SELECT [1, 2] AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## Working with Data Types {#working-with-data-types}

When creating an array on the fly, ClickHouse automatically defines the argument type as the narrowest data type that can store all the listed arguments. If there are any [Nullable](/sql-reference/data-types/nullable) or literal [NULL](/operations/settings/formats#input_format_null_as_default) values, the type of an array element also becomes [Nullable](../../sql-reference/data-types/nullable.md).

If ClickHouse couldn't determine the data type, it generates an exception. For instance, this happens when trying to create an array with strings and numbers simultaneously (`SELECT array(1, 'a')`).

Examples of automatic data type detection:

```sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

```text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

If you try to create an array of incompatible data types, ClickHouse throws an exception:

```sql
SELECT array(1, 'a')
```

```text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

## Array Size {#array-size}

It is possible to find the size of an array by using the `size0` subcolumn without reading the whole column. For multi-dimensional arrays you can use `sizeN-1`, where `N` is the wanted dimension.

**Example**

```sql title="Query"
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

```text title="Response"
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```

## Reading nested subcolumns from Array {#reading-nested-subcolumns-from-array}

If nested type `T` inside `Array` has subcolumns (for example, if it's a [named tuple](./tuple.md)), you can read its subcolumns from an `Array(T)` type with the same subcolumn names. The type of a subcolumn will be `Array` of the type of original subcolumn.

**Example**

```sql
CREATE TABLE t_arr (arr Array(Tuple(field1 UInt32, field2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT arr.field1, toTypeName(arr.field1), arr.field2, toTypeName(arr.field2) from t_arr;
```

```test
┌─arr.field1─┬─toTypeName(arr.field1)─┬─arr.field2────────────────┬─toTypeName(arr.field2)─┐
│ [1,2]      │ Array(UInt32)          │ ['Hello','World']         │ Array(String)          │
│ [3,4,5]    │ Array(UInt32)          │ ['This','is','subcolumn'] │ Array(String)          │
└────────────┴────────────────────────┴───────────────────────────┴────────────────────────┘
```
)DOCS_MD",
            .syntax = "Array(T)",
            .examples = {},
            .related = {"Tuple", "Map"},
        });
}

}
