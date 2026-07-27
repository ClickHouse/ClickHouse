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
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNEXPECTED_AST_STRUCTURE;
}

bool DataTypeQBit::isSupportedElementType(const DataTypePtr & type)
{
    const TypeIndex id = type->getTypeId();
    return id == TypeIndex::Int8 || id == TypeIndex::BFloat16 || id == TypeIndex::Float32 || id == TypeIndex::Float64;
}

DataTypeQBit::DataTypeQBit(const DataTypePtr & element_type_, const size_t dimension_, const size_t stride_)
    : element_type(element_type_)
    , dimension(dimension_)
    , stride(stride_)
{
    /// Prevents maliciously crafted byte streams from being deserialized into illegal types, which could be exploited to crash the server
    if (!isSupportedElementType(element_type_))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "QBit data type only supports Int8, BFloat16, Float32, or Float64 as element type. Got: {}",
            element_type_->getName());

    if (stride == 0 || stride > dimension || dimension % stride != 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "QBit stride must be a positive divisor of the dimension {}. Got: {}",
            dimension,
            stride);

    /// When actually strided, each group's bit plane is a FixedString of `stride / 8` bytes. Requiring `stride % 8 == 0` keeps the
    /// groups byte-aligned and contiguous, which simplifies both the storage layout and the partial-dimension reads in distance
    /// functions. The non-strided case (`stride == dimension`) keeps the previous behaviour and allows any dimension.
    if (stride != dimension && stride % 8 != 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "QBit stride must be a multiple of 8 when it is smaller than the dimension. Got: {}",
            stride);

    /// The nested storage is a Tuple of `element size * (dimension / stride)` FixedString streams. Bound the number of stride groups
    /// so that a (possibly maliciously crafted) type with a huge dimension and a small stride cannot make `getNestedType` materialize
    /// an unreasonable number of streams (and exhaust memory). A non-strided QBit always has just `element size` streams regardless of
    /// the dimension, so this only constrains the strided case.
    if (getNumStrides() > MAX_STRIDE_GROUPS)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "QBit has too many stride groups: {} (dimension {} / stride {}). The maximum is {}.",
            getNumStrides(),
            dimension,
            stride,
            MAX_STRIDE_GROUPS);

    /// QBit stores data as a Tuple of binary FixedStrings. Setting custom_serialization
    /// ensures that ReplaceQueryParameterVisitor::visitQueryParameter uses the original
    /// string value for query parameters like `SET param_q=[1,2,3,4]; SELECT {q:QBit(Float32,4)};`
    /// instead of extracting this Tuple as a Field that fails to cast back to QBit.
    custom_serialization = getDefaultSerialization();
}

void DataTypeQBit::updateHashImpl(SipHash & hash) const
{
    /// Hash the defining parameters directly. This is cheaper than materializing the nested tuple and, unlike hashing the
    /// derived tuple structure, it captures the element type identity precisely (the tuple only encodes the element size).
    element_type->updateHashImpl(hash);
    hash.update(dimension);
    hash.update(stride);
}

std::string DataTypeQBit::doGetName() const
{
    if (stride == dimension)
        return "QBit(" + element_type->getName() + ", " + toString(dimension) + ")";
    return "QBit(" + element_type->getName() + ", " + toString(dimension) + ", " + toString(stride) + ")";
}

/// This is called when values are added, not on CREATE TABLE
MutableColumnPtr DataTypeQBit::createColumn() const
{
    /// Continue with column creation. One FixedString per (stride group, bit plane), grouped as [group][bit].
    const size_t num_columns = getElementSize() * getNumStrides();
    MutableColumns tuple_columns(num_columns);
    size_t bytes = bitsToBytes(stride);

    for (size_t i = 0; i < num_columns; ++i)
        tuple_columns[i] = ColumnFixedString::create(bytes);

    return ColumnQBit::create(IColumn::mutate(ColumnTuple::create(std::move(tuple_columns))), dimension, stride);
}

bool DataTypeQBit::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeQBit & rhsq = static_cast<const DataTypeQBit &>(rhs);

    return element_type.get()->equals(*rhsq.element_type.get()) && getElementSize() == rhsq.getElementSize() && dimension == rhsq.dimension
        && stride == rhsq.stride;
}

DataTypePtr DataTypeQBit::getNestedType() const
{
    auto fixed_string_type = getNestedTupleElementType();
    DataTypes tuple_element_types(getElementSize() * getNumStrides(), fixed_string_type);
    return std::make_shared<DataTypeTuple>(tuple_element_types);
}

DataTypePtr DataTypeQBit::getNestedTupleElementType() const
{
    return std::make_shared<DataTypeFixedString>(bitsToBytes(stride));
}

SerializationPtr DataTypeQBit::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationQBit::create(getNestedType()->getDefaultSerialization(), getElementSize(), dimension, stride);
}

Field DataTypeQBit::getDefault() const
{
    return getNestedType()->getDefault();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    /// Check if arguments are valid
    if (!arguments || (arguments->children.size() != 2 && arguments->children.size() != 3))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "QBit data type family must have two or three arguments: type of vector elements, their number, and optionally the stride");

    const DataTypePtr type = DataTypeFactory::instance().get(arguments->children[0]);
    const auto * argument = arguments->children[1]->as<ASTLiteral>();

    if (type->getTypeId() != TypeIndex::Int8 && type->getTypeId() != TypeIndex::BFloat16 && type->getTypeId() != TypeIndex::Float32
        && type->getTypeId() != TypeIndex::Float64)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "QBit data type only supports Int8, BFloat16, Float32, or Float64 as element type. Got: {}",
            type->getName());

    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "QBit data type must have a number (positive integer) as its second argument. Got: {}",
            arguments->children[1]->formatForErrorMessage());

    const UInt64 dimension = argument->value.safeGet<UInt64>();

    /// The optional third argument is the stride. When omitted it defaults to the dimension (no striding).
    UInt64 stride = dimension;
    if (arguments->children.size() == 3)
    {
        const auto * stride_argument = arguments->children[2]->as<ASTLiteral>();
        if (!stride_argument || stride_argument->value.getType() != Field::Types::UInt64 || stride_argument->value.safeGet<UInt64>() == 0)
            throw Exception(
                ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                "QBit data type stride must be a number (positive integer) as its third argument. Got: {}",
                arguments->children[2]->formatForErrorMessage());
        stride = stride_argument->value.safeGet<UInt64>();
    }

    return std::make_shared<DataTypeQBit>(type, dimension, stride);
}


void registerDataTypeQBit(DataTypeFactory & factory)
{
    factory.registerDataType("QBit", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
The `QBit` data type reorganizes vector storage for faster approximate searches. Instead of storing each vector's elements together, it groups the same binary digit positions across all vectors.
This stores vectors at full precision while letting you choose the fine-grained quantization level at search time: read fewer bits for less I/O and faster calculations, or more bits for higher accuracy. You get the speed benefits of reduced data transfer and computation from quantization, but all the original data remains available when needed.

To declare a column of `QBit` type, use the following syntax:

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – the type of each vector element. The allowed types are `Int8`, `BFloat16`, `Float32` and `Float64`
* `dimension` – the number of elements in each vector
* `stride` – optional. The number of dimensions stored together in one group of streams. When omitted it defaults to `dimension` (a single group). When provided, `dimension` must be a multiple of `stride`, and, when `stride` is smaller than `dimension`, `stride` must be a multiple of 8. The `dimension` dimensions are split into `dimension / stride` contiguous groups, and each group's bit planes are stored in separate streams. This lets a search over the first `D` dimensions (with `D` a multiple of `stride`) read only the streams of the groups that cover those dimensions, which is useful for Matryoshka embeddings.

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

## Converting arrays to QBit {#converting-arrays-to-qbit}

Arrays convert to `QBit` when the array length matches the `QBit` dimension. The array's element type does not need to match the `QBit` element type. Any numeric element type is converted to it automatically. This lets you move an existing column of embeddings straight into a `QBit` column:

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

The conversion also works explicitly with `CAST`, for example `CAST(embedding AS QBit(Float32, 8))`.

## Converting QBit to arrays {#converting-qbit-to-arrays}

The reverse conversion reconstructs the original vector from the bit-transposed representation, so casting a `QBit` to an `Array` returns the stored values. This is the inverse of [converting arrays to `QBit`](#converting-arrays-to-qbit):

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

The reconstructed array uses the `QBit`'s element type, and its elements are then converted to the requested array element type. A cast that also changes the element type, such as `QBit(Float32, N)` to `Array(Float64)`, therefore works as well.

An `Array` -> `QBit` -> `Array` round trip is lossless for `Int8`, `Float32` and `Float64`. For `BFloat16` it matches a direct conversion to `BFloat16` — the only precision lost is that of `BFloat16` itself.

When the `dimension` is not a multiple of 8, the trailing padding elements present in the internal representation are dropped, so the result always has exactly `dimension` elements.

## Converting between QBit types {#converting-between-qbit-types}

A `QBit` can be cast to another `QBit` as long as the `dimension` (the number of vector elements) stays the same. The `element_type` and the `stride` may both change; casting to a `QBit` with a different `dimension` raises an exception, because that would change the vector itself.

Changing the `element_type` reconstructs the vector and converts each element to the new type, exactly like the corresponding `Array` conversion: widening (for example `QBit(Float32, N)` to `QBit(Float64, N)`) is exact, while narrowing loses precision in the same way a narrowing `Array` cast would.

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::QBit(Float64, 4) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

Changing only the [`stride`](#strides) (keeping the same `element_type`) re-groups the stored bit planes without touching the values, so it is always lossless:

```sql
SELECT range(16)::Array(Float32)::QBit(Float32, 16)::QBit(Float32, 16, 8)::Array(Float32)
     = range(16)::Array(Float32) AS is_lossless;
```

```text
┌─is_lossless─┐
│           1 │
└─────────────┘
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

The number of accessible subcolumns depends on the element type (and, when strided, on the number of stride groups):

* `Int8`: 8 subcolumns per stride group (1-8)
* `BFloat16`: 16 subcolumns per stride group (1-16)
* `Float32`: 32 subcolumns per stride group (1-32)
* `Float64`: 64 subcolumns per stride group (1-64)

The subcolumns follow a group-major order: in general `vec.N` reads bit plane `(N-1) % element_size` of stride group `(N-1) / element_size`. For example, with `QBit(BFloat16, 4096, 1024)` the 4096 dimensions are split into 4 groups of 1024, so there are 64 subcolumns: `vec.1` … `vec.16` are the bit planes of the first stride group (dimensions 1–1024), `vec.17` … `vec.32` belong to the second group (dimensions 1025–2048), and so on.

## Strides {#strides}

By default a `QBit` stores each bit plane as a single stream spanning all `dimension` dimensions, so a search always reads whole bit planes across the full vector. The optional `stride` parameter partitions the `dimension` dimensions into `dimension / stride` contiguous groups and stores each group's bit planes in separate streams. This lets a search over only the first `D` dimensions (with `D` a multiple of `stride`) read just the streams of the groups that cover those dimensions — useful for [Matryoshka embeddings](https://arxiv.org/abs/2205.13147), where the leading dimensions form a usable lower-dimensional embedding.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

Here the 4096 dimensions are split into 4 groups of 1024. The subcolumns follow a group-major order: with `BFloat16` (16 bit planes), `vec.1` … `vec.16` are the 16 bit planes of the first stride group (dimensions 1–1024), `vec.17` … `vec.32` belong to the second group (dimensions 1025–2048), and so on. In general `vec.N` reads bit plane `(N-1) % element_size` of stride group `(N-1) / element_size`.

To run a reduced-dimension search, pass the number of dimensions to read as the fourth argument of the transposed distance functions (see below). The reference vector must have at least that many elements (any extra trailing elements are ignored), and the value must be a multiple of `stride`.

## Vector search functions {#vector-search-functions}

These are the distance functions for vector similarity search that use `QBit` data type:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

For a strided `QBit`, these functions accept an optional fourth argument `used_dims` — the number of leading dimensions to read — which reads only the stride groups covering those dimensions. The reference vector must have at least `used_dims` elements (any extra trailing elements are ignored, so a full-size query vector can be reused for a reduced-dimension search without slicing it first), and `used_dims` must be a multiple of `stride`.
)DOCS_MD",
            .syntax = "QBit(T, dim[, stride])",
            .examples = {},
            .related = {},
        });
}

}
