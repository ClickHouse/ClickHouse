#pragma once

#include <Processors/Formats/Impl/ORC/Type.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <memory>
#include <vector>

/// Batches of column values filled by the reader. The values of the primitive types are decoded
/// directly into ClickHouse columns, which the conversion to the requested types takes without
/// copying. The structure follows the reference implementation: a batch has the null mask of its
/// values, and the batches of the compound types have the batches of their children.

namespace DB::ORC
{

struct ColumnVectorBatch
{
    ColumnVectorBatch() = default;
    virtual ~ColumnVectorBatch() = default;

    ColumnVectorBatch(const ColumnVectorBatch &) = delete;
    ColumnVectorBatch & operator=(const ColumnVectorBatch &) = delete;

    /// Prepares the batch for `num_values` values.
    void reset(size_t num_values)
    {
        numElements = num_values;
        notNull.resize(num_values);
    }

    size_t numElements = 0;
    /// Non-zero for the non-null values, valid only if `hasNulls`.
    PaddedPODArray<char> notNull;
    bool hasNulls = false;
    /// For string batches read with lazy dictionary decoding.
    bool isEncoded = false;
};

/// The values of a primitive type in a ClickHouse column (`ColumnVector` of `T`). The null values are zeros.
template <typename T>
struct NumericVectorBatch : public ColumnVectorBatch
{
    using Container = PaddedPODArray<T>;

    /// Creates a new column for the next values: the previous column can be still referenced.
    Container & prepareColumn(size_t num_values)
    {
        auto new_column = ColumnVector<T>::create(num_values);
        auto & data = new_column->getData();
        column = std::move(new_column);
        return data;
    }

    const Container & getData() const { return assert_cast<const ColumnVector<T> &>(*column).getData(); }

    ColumnPtr column;
};

/// BOOLEAN: 0 or 1.
using BoolVectorBatch = NumericVectorBatch<UInt8>;
using ByteVectorBatch = NumericVectorBatch<Int8>;
using ShortVectorBatch = NumericVectorBatch<Int16>;
using IntVectorBatch = NumericVectorBatch<Int32>;
/// LONG and DATE (days since the epoch, not narrowed, so that the range can be checked).
using LongVectorBatch = NumericVectorBatch<Int64>;
using FloatVectorBatch = NumericVectorBatch<Float32>;
using DoubleVectorBatch = NumericVectorBatch<Float64>;

/// The values in a `ColumnString`. The null values are empty strings.
struct StringVectorBatch : public ColumnVectorBatch
{
    ColumnPtr column;
};

struct StringDictionary
{
    /// The entries of the dictionary.
    ColumnPtr column;
};

/// A string batch with the dictionary indices, without decoding the values. `column` is not set.
struct EncodedStringVectorBatch : public StringVectorBatch
{
    std::shared_ptr<StringDictionary> dictionary;
    PaddedPODArray<Int64> index;
};

/// Builds a `ColumnString` from the dictionary entries by the indices (the null values are empty strings).
/// Throws `INCORRECT_DATA` if an index is out of range.
ColumnPtr decodeDictionary(const ColumnString & dictionary, const Int64 * index, const char * not_null, size_t num_values);

/// Returns the values of a string batch as a `ColumnString`, decoding the dictionary for an encoded batch.
ColumnPtr getStringColumn(const StringVectorBatch & batch);

struct StructVectorBatch : public ColumnVectorBatch
{
    void addField(std::unique_ptr<ColumnVectorBatch> field)
    {
        fields.push_back(field.get());
        owned_fields.push_back(std::move(field));
    }

    std::vector<ColumnVectorBatch *> fields;

private:
    std::vector<std::unique_ptr<ColumnVectorBatch>> owned_fields;
};

/// LIST and MAP: the end offsets of the values in the children, as in `ColumnArray`.
struct ListVectorBatch : public ColumnVectorBatch
{
    /// `ColumnArray::ColumnOffsets`.
    ColumnPtr offsets;
    std::unique_ptr<ColumnVectorBatch> elements;
};

struct MapVectorBatch : public ColumnVectorBatch
{
    /// `ColumnArray::ColumnOffsets`.
    ColumnPtr offsets;
    std::unique_ptr<ColumnVectorBatch> keys;
    std::unique_ptr<ColumnVectorBatch> elements;
};

struct UnionVectorBatch : public ColumnVectorBatch
{
    void addChild(std::unique_ptr<ColumnVectorBatch> child)
    {
        children.push_back(child.get());
        owned_children.push_back(std::move(child));
    }

    /// The branch of each value.
    PaddedPODArray<UInt8> tags;
    /// The position of each value in its branch.
    PaddedPODArray<UInt64> offsets;
    std::vector<ColumnVectorBatch *> children;

private:
    std::vector<std::unique_ptr<ColumnVectorBatch>> owned_children;
};

/// For DECIMAL with the precision at most 18.
struct Decimal64VectorBatch : public ColumnVectorBatch
{
    Int32 precision = 0;
    Int32 scale = 0;
    PaddedPODArray<Int64> values;
};

/// For DECIMAL with the precision above 18 or unspecified (zero).
struct Decimal128VectorBatch : public ColumnVectorBatch
{
    Int32 precision = 0;
    Int32 scale = 0;
    PaddedPODArray<Int128> values;
};

struct TimestampVectorBatch : public ColumnVectorBatch
{
    /// Seconds since the epoch in UTC.
    PaddedPODArray<Int64> data;
    PaddedPODArray<Int64> nanoseconds;
};

/// Creates a batch tree matching the type. With `encoded`, the string columns get
/// `EncodedStringVectorBatch`, which the reader fills without decoding the dictionaries.
std::unique_ptr<ColumnVectorBatch> createRowBatch(const Type & type, bool encoded = false);

}
