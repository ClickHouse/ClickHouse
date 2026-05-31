#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeVariant.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


DataTypeNullable::DataTypeNullable(const DataTypePtr & nested_data_type_)
    : nested_data_type{nested_data_type_}
{
    if (!nested_data_type->canBeInsideNullable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Nested type {} cannot be inside Nullable type", nested_data_type->getName());
}


bool DataTypeNullable::onlyNull() const
{
    return typeid_cast<const DataTypeNothing *>(nested_data_type.get());
}


MutableColumnPtr DataTypeNullable::createColumn() const
{
    return ColumnNullable::create(nested_data_type->createColumn(), ColumnUInt8::create());
}

MutableColumnPtr DataTypeNullable::createUninitializedColumnWithSize(size_t size) const
{
    return ColumnNullable::create(nested_data_type->createUninitializedColumnWithSize(size), ColumnUInt8::create(size));
}

Field DataTypeNullable::getDefault() const
{
    return Null();
}

size_t DataTypeNullable::getSizeOfValueInMemory() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Value of type {} in memory is not of fixed size.", getName());
}


bool DataTypeNullable::equals(const IDataType & rhs) const
{
    return rhs.isNullable() && nested_data_type->equals(*static_cast<const DataTypeNullable &>(rhs).nested_data_type);
}

void DataTypeNullable::updateHashImpl(SipHash & hash) const
{
    nested_data_type->updateHash(hash);
}

ColumnPtr DataTypeNullable::createColumnConst(size_t size, const Field & field) const
{
    if (onlyNull())
    {
        auto column = createColumn();
        column->insert(field);
        return ColumnConst::create(std::move(column), size);
    }

    auto column = nested_data_type->createColumn();
    bool is_null = field.isNull();

    if (is_null)
        nested_data_type->insertDefaultInto(*column);
    else
        column->insert(field);

    auto null_mask = ColumnUInt8::create();
    null_mask->getData().push_back(is_null ? static_cast<UInt8>(1) : static_cast<UInt8>(0));

    auto res = ColumnNullable::create(std::move(column), std::move(null_mask));
    return ColumnConst::create(std::move(res), size);
}

SerializationPtr DataTypeNullable::doGetSerialization(const SerializationInfoSettings & settings) const
{
    if (settings.propagate_types_serialization_versions_to_nested_types)
        return SerializationNullable::create(nested_data_type->getSerialization(settings));
    return SerializationNullable::create(nested_data_type->getDefaultSerialization());
}

void DataTypeNullable::forEachChild(const ChildCallback & callback) const
{
    callback(*nested_data_type);
    nested_data_type->forEachChild(callback);
}


std::unique_ptr<ISerialization::SubstreamData> DataTypeNullable::getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const
{
    auto nested_type = assert_cast<const DataTypeNullable &>(*data.type).nested_data_type;
    const auto & nullable_serialization = assert_cast<const SerializationNullable &>(*removeNamedSerialization(data.serialization));
    ISerialization::SubstreamData nested_data(nullable_serialization.getNested());
    nested_data.type = nested_type;
    nested_data.column = data.column ? assert_cast<const ColumnNullable &>(*data.column).getNestedColumnPtr() : nullptr;

    auto nested_subcolumn_data = DB::IDataType::getSubcolumnData(subcolumn_name, nested_data, initial_array_level, throw_if_null);
    if (!nested_subcolumn_data)
        return nullptr;

    auto creator = NullableSubcolumnCreator(data.column ? assert_cast<const ColumnNullable &>(*data.column).getNullMapColumnPtr() : nullptr);
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
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Nullable data type family must have exactly one argument - nested type");

    DataTypePtr nested_type = DataTypeFactory::instance().get(arguments->children[0]);

    return std::make_shared<DataTypeNullable>(nested_type);
}


void registerDataTypeNullable(DataTypeFactory & factory)
{
    factory.registerDataType("Nullable", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
Allows to store special marker ([NULL](../../sql-reference/syntax.md)) that denotes "missing value" alongside normal values allowed by `T`. For example, a `Nullable(Int8)` type column can store `Int8` type values, and the rows that do not have a value will store `NULL`.

`T` can't be any of the following composite data types:
- [Array](../../sql-reference/data-types/array.md) — Not supported
- [Map](../../sql-reference/data-types/map.md) — Not supported
- [Tuple](../../sql-reference/data-types/tuple.md) — Experimental support available*

However, composite data types **can contain** `Nullable` type values, e.g. `Array(Nullable(Int8))` or `Tuple(Nullable(String), Nullable(Int64))`.

:::note Experimental: Nullable Tuples
* [Nullable(Tuple(...))](../../sql-reference/data-types/tuple.md#nullable-tuple) is supported when `allow_experimental_nullable_tuple_type = 1` is enabled.
:::

A `Nullable` type field can't be included in table indexes.

`NULL` is the default value for any `Nullable` type, unless specified otherwise in the ClickHouse server configuration.

## Storage Features {#storage-features}

To store `Nullable` type values in a table column, ClickHouse uses a separate file with `NULL` masks in addition to normal file with values. Entries in masks file allow ClickHouse to distinguish between `NULL` and a default value of corresponding data type for each table row. Because of an additional file, `Nullable` column consumes additional storage space compared to a similar normal one.

:::note
Using `Nullable` almost always negatively affects performance, keep this in mind when designing your databases.
:::

## Finding NULL {#finding-null}

It is possible to find `NULL` values in a column by using `null` subcolumn without reading the whole column. It returns `1` if the corresponding value is `NULL` and `0` otherwise.

**Example**

```sql title="Query"
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

```text title="Response"
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

## Usage Example {#usage-example}

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

```sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

```sql
SELECT x + y FROM t_null
```

```text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```
)DOCS_MD",
            .syntax = "Nullable(T)",
            .examples = {},
            .related = {},
        });
}


DataTypePtr makeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr makeNullableSafe(const DataTypePtr & type)
{
    if (type->canBeInsideNullable())
        return makeNullable(type);
    return type;
}

DataTypePtr removeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();
    return type;
}

DataTypePtr makeNullableOrLowCardinalityNullable(const DataTypePtr & type)
{
    if (isNullableOrLowCardinalityNullable(type))
        return type;

    if (type->lowCardinality())
    {
        const auto & dictionary_type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        return std::make_shared<DataTypeLowCardinality>(makeNullable(dictionary_type));
    }

    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr makeNullableOrLowCardinalityNullableSafe(const DataTypePtr & type)
{
    if (isNullableOrLowCardinalityNullable(type))
        return type;

    if (type->lowCardinality())
    {
        const auto & dictionary_type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        return std::make_shared<DataTypeLowCardinality>(makeNullable(dictionary_type));
    }

    return makeNullableSafe(type);
}

DataTypePtr removeNullableOrLowCardinalityNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();

    if (type->isLowCardinalityNullable())
    {
        auto dict_type = removeNullable(static_cast<const DataTypeLowCardinality &>(*type).getDictionaryType());
        return std::make_shared<DataTypeLowCardinality>(dict_type);
    }

    return type;

}

bool canContainNull(const IDataType & type)
{
    return type.isNullable() || type.isLowCardinalityNullable() || isDynamic(type) || isVariant(type);
}

}
