#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationQBit.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeQBit::DataTypeQBit(const DataTypePtr & element_type_, const size_t dimension_)
    : element_type(element_type_)
    , dimension(dimension_)
{
    /// Prevents maliciously crafted byte streams from being deserialized into illegal types, which could be exploited to crash the server
    if (element_type_->getTypeId() != TypeIndex::BFloat16 && element_type_->getTypeId() != TypeIndex::Float32
        && element_type_->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "QBit data type only supports BFloat16, Float32, or Float64 as element type. Got: {}",
            element_type_->getName());

    /// QBit stores data as a Tuple of binary FixedStrings. Setting custom_serialization
    /// ensures that ReplaceQueryParameterVisitor::visitQueryParameter uses the original
    /// string value for query parameters like `SET param_q=[1,2,3,4]; SELECT {q:QBit(Float32,4)};`
    /// instead of extracting this Tuple as a Field that fails to cast back to QBit.
    custom_serialization = getDefaultSerialization();
}

std::string DataTypeQBit::doGetName() const
{
    return "QBit(" + element_type->getName() + ", " + toString(dimension) + ")";
}

/// This is called when values are added, not on CREATE TABLE
MutableColumnPtr DataTypeQBit::createColumn() const
{
    /// Continue with column creation
    MutableColumns tuple_columns(getElementSize());
    size_t bytes = bitsToBytes(dimension);

    for (size_t i = 0; i < getElementSize(); ++i)
        tuple_columns[i] = ColumnFixedString::create(bytes);

    return ColumnQBit::create(IColumn::mutate(ColumnTuple::create(std::move(tuple_columns))), dimension);
}

bool DataTypeQBit::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeQBit & rhsq = static_cast<const DataTypeQBit &>(rhs);

    return element_type.get()->equals(*rhsq.element_type.get()) && getElementSize() == rhsq.getElementSize() && dimension == rhsq.dimension;
}

DataTypePtr DataTypeQBit::getNestedType() const
{
    auto fixed_string_type = getNestedTupleElementType();
    DataTypes tuple_element_types(getElementSize(), fixed_string_type);
    return std::make_shared<DataTypeTuple>(tuple_element_types);
}

DataTypePtr DataTypeQBit::getNestedTupleElementType() const
{
    return std::make_shared<DataTypeFixedString>(bitsToBytes(dimension));
}

SerializationPtr DataTypeQBit::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationQBit::create(getNestedType()->getDefaultSerialization(), getElementSize(), dimension);
}

Field DataTypeQBit::getDefault() const
{
    return getNestedType()->getDefault();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    /// Check if arguments are valid
    if (!arguments || arguments->children.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "QBit data type family must have exactly two argument: type of vector elements and their number");

    const DataTypePtr type = DataTypeFactory::instance().get(arguments->children[0]);
    const auto * argument = arguments->children[1]->as<ASTLiteral>();

    if (type->getTypeId() != TypeIndex::BFloat16 && type->getTypeId() != TypeIndex::Float32 && type->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "QBit data type only supports BFloat16, Float32, or Float64 as element type. Got: {}",
            type->getName());

    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "QBit data type must have a number (positive integer) as its second argument. Got: {}",
            arguments->children[1]->formatForErrorMessage());

    return std::make_shared<DataTypeQBit>(type, argument->value.safeGet<UInt64>());
}


void registerDataTypeQBit(DataTypeFactory & factory)
{
    factory.registerDataType("QBit", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
The `QBit` data type reorganizes vector storage for faster approximate searches. Instead of storing each vector's elements together, it groups the same binary digit positions across all vectors.
This stores vectors at full precision while letting you choose the fine-grained quantization level at search time: read fewer bits for less I/O and faster calculations, or more bits for higher accuracy. You get the speed benefits of reduced data transfer and computation from quantization, but all the original data remains available when needed.

To declare a column of `QBit` type, use the following syntax:

```sql
column_name QBit(element_type, dimension)
```

* `element_type` – the type of each vector element. The allowed types are `BFloat16`, `Float32` and `Float64`
* `dimension` – the number of elements in each vector

## Creating QBit {#creating-qbit}

Using the `QBit` type in table column definition:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

## QBit subcolumns {#qbit-subcolumns}

`QBit` implements a subcolumn access pattern that allows you to access individual bit planes of the stored vectors. Each bit position can be accessed using the `.N` syntax, where `N` is the bit position:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

The number of accessible subcolumns depends on the element type:

* `BFloat16`: 16 subcolumns (1-16)
* `Float32`: 32 subcolumns (1-32)
* `Float64`: 64 subcolumns (1-64)

## Vector search functions {#vector-search-functions}

These are the distance functions for vector similarity search that use `QBit` data type:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
)DOCS_MD",
            .syntax = "QBit(T, dim)",
            .examples = {},
            .related = {},
        });
}

}
