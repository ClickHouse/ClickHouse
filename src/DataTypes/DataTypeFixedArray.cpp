#include <DataTypes/DataTypeFixedArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationFixedArray.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

static size_t getNumberOfElements(const IDataType & type)
{
    if (const auto * fixed_array = typeid_cast<const DataTypeFixedArray *>(&type))
        return fixed_array->getArraySize() * getNumberOfElements(*fixed_array->getNestedType());
    return 1;
}

DataTypeFixedArray::DataTypeFixedArray(size_t n_, const DataTypePtr & nested_)
    : n(n_), nested(nested_)
{
    if (n == 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The size of FixedArray cannot be zero");

    size_t nested_elements = getNumberOfElements(*nested);
    if (n > MAX_ELEMENTS / nested_elements)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "FixedArray is too large: it would contain {} * {} elements, the maximum is {}", n, nested_elements, MAX_ELEMENTS);
}

/// The dimensions are listed together: `FixedArray(N, M, T)` instead of `FixedArray(N, FixedArray(M, T))`.
std::string DataTypeFixedArray::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "FixedArray(" << n;
    const IDataType * element = nested.get();
    while (const auto * fixed_array = typeid_cast<const DataTypeFixedArray *>(element))
    {
        s << ", " << fixed_array->n;
        element = fixed_array->nested.get();
    }
    s << ", " << element->getName() << ")";
    return s.str();
}

std::string DataTypeFixedArray::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "FixedArray(" << n;
    const IDataType * element = nested.get();
    while (const auto * fixed_array = typeid_cast<const DataTypeFixedArray *>(element))
    {
        s << ", " << fixed_array->n;
        element = fixed_array->nested.get();
    }
    s << ", " << element->getPrettyName(indent) << ")";
    return s.str();
}

MutableColumnPtr DataTypeFixedArray::createColumn() const
{
    return ColumnFixedArray::create(nested->createColumn(), n);
}

Field DataTypeFixedArray::getDefault() const
{
    return Array(n, nested->getDefault());
}

bool DataTypeFixedArray::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;
    const auto & rhs_fixed_array = static_cast<const DataTypeFixedArray &>(rhs);
    return n == rhs_fixed_array.n && nested->equals(*rhs_fixed_array.nested);
}

void DataTypeFixedArray::updateHashImpl(SipHash & hash) const
{
    hash.update(n);
    nested->updateHash(hash);
}

SerializationPtr DataTypeFixedArray::doGetSerialization(const SerializationInfoSettings & settings) const
{
    if (settings.propagate_types_serialization_versions_to_nested_types)
        return SerializationFixedArray::create(nested->getSerialization(settings), n);
    return SerializationFixedArray::create(nested->getDefaultSerialization(), n);
}

DataTypePtr DataTypeFixedArray::doCloneWithChildren(const DataTypes & new_children) const
{
    return std::make_shared<DataTypeFixedArray>(n, new_children[0]);
}

DataTypePtr recursiveRemoveFixedArray(const DataTypePtr & type)
{
    if (const auto * fixed_array = typeid_cast<const DataTypeFixedArray *>(type.get()))
        return std::make_shared<DataTypeArray>(recursiveRemoveFixedArray(fixed_array->getNestedType()));

    WhichDataType which(type);
    if (!which.isArray() && !which.isTuple() && !which.isMap() && !which.isNullable())
        return type;

    DataTypes children = type->getChildren();
    bool changed = false;
    for (auto & child : children)
    {
        auto converted = recursiveRemoveFixedArray(child);
        changed |= converted.get() != child.get();
        child = std::move(converted);
    }

    return changed ? type->cloneWithChildren(children) : type;
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "FixedArray data type family must have at least two arguments: the size (or several sizes) and the type of elements");

    const auto & children = arguments->children;
    DataTypePtr result = DataTypeFactory::instance().get(children.back());

    /// FixedArray(N, M, T) is FixedArray(N, FixedArray(M, T)), so the type is built from the innermost dimension.
    for (size_t i = children.size() - 1; i > 0; --i)
    {
        const auto * argument = children[i - 1]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                "The sizes of FixedArray must be positive integer literals, got {}", children[i - 1]->formatForErrorMessage());

        result = std::make_shared<DataTypeFixedArray>(argument->value.safeGet<UInt64>(), result);
    }

    return result;
}


void registerDataTypeFixedArray(DataTypeFactory & factory)
{
    factory.registerDataType("FixedArray", create, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(
An array of exactly `N` items of type `T`. The size is a part of the type.

A multidimensional array (a tensor) can be written by listing all its dimensions: `FixedArray(N, M, T)` is the same type as `FixedArray(N, FixedArray(M, T))`.
The name of the type is always written in the first form.

## Use cases {#use-cases}

`FixedArray` is designed for tensors: images, raster data, vector embeddings, and the weights of small models.
For example, a screenshot of 720 rows of 1280 pixels with three color channels is `FixedArray(720, 1280, 3, UInt8)`.
The last dimension changes fastest in memory, so the color components of a pixel are adjacent,
and the pixels of a row are adjacent as well.

## Storage {#storage}

The values are stored as the values of the elements, one after another, without any sizes:
a column of `FixedArray(720, 1280, 3, UInt8)` takes exactly `720 * 1280 * 3` bytes per row, both in memory and in a table before compression.
`Array(Array(Array(UInt8)))` with the same data needs additional 8 bytes for every one of the `720 * 1280` innermost arrays.

## Creating a FixedArray {#creating-a-fixed-array}

A value of `FixedArray` is written in the same way as a value of `Array`, but the number of elements must match the size of the type:

```sql
SELECT [1, 2, 3]::FixedArray(3, UInt8) AS x, toTypeName(x);
```

```text
┌─x───────┬─toTypeName(x)──────────┐
│ [1,2,3] │ FixedArray(3, UInt8)   │
└─────────┴────────────────────────┘
```

```sql
SELECT [[1, 2, 3], [4, 5, 6]]::FixedArray(2, 3, Float32) AS m, m[2, 3], m[2][3], m[2];
```

```text
┌─m─────────────────┬─arrayElement(arrayElement(m, 2), 3)─┬─arrayElement(arrayElement(m, 2), 3)─┬─arrayElement(m, 2)─┐
│ [[1,2,3],[4,5,6]] │                                   6 │                                   6 │ [4,5,6]            │
└───────────────────┴─────────────────────────────────────┴─────────────────────────────────────┴────────────────────┘
```

Converting an array of a different size to `FixedArray` throws an exception.

## Working with FixedArray {#working-with-fixed-array}

Every function that accepts an [Array](/reference/data-types/array) accepts a `FixedArray` as well: it processes the value as an `Array` with the same elements,
and returns the same result as for `Array`. A few functions keep the fixed size:

- the subscript operator `arr[i]` returns the element, which is a `FixedArray` for a multidimensional array, and `arr[i, j]` is the same as `arr[i][j]`;
- [arrayMap](/reference/functions/regular-functions/array-functions#arrayMap) returns a `FixedArray` of the same size, so `arr[][1]`, which is a shorthand for `arrayMap(x -> x[1], arr)`, returns a `FixedArray` as well;
- [einsum](/reference/functions/regular-functions/array-functions#einsum) returns a `FixedArray` when the sizes of all dimensions of the result are known from the types of its arguments.

A `FixedArray` cannot be inside [Nullable](/reference/data-types/nullable), like an `Array`.
)DOCS_MD",
        .syntax = "FixedArray(N, T), FixedArray(N1, N2, ..., T)",
        .examples = {},
        .related = {"Array"},
    });
}

}
