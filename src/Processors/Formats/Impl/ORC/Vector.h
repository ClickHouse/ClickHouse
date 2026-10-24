#pragma once

#include <Processors/Formats/Impl/ORC/Type.h>
#include <Common/PODArray.h>

#include <memory>
#include <vector>

/// Batches of column values, which the reader fills and the writer consumes. The layout follows the
/// reference implementation, so the conversion to and from ClickHouse columns maps one to one.

namespace DB::ORC
{

struct ColumnVectorBatch
{
    /// All values are non-null initially, as in the reference implementation.
    explicit ColumnVectorBatch(size_t capacity_) : capacity(capacity_) { notNull.resize_fill(capacity, 1); }
    virtual ~ColumnVectorBatch() = default;

    ColumnVectorBatch(const ColumnVectorBatch &) = delete;
    ColumnVectorBatch & operator=(const ColumnVectorBatch &) = delete;

    /// Grows the capacity (never shrinks).
    virtual void resize(size_t new_capacity)
    {
        if (capacity < new_capacity)
        {
            capacity = new_capacity;
            notNull.resize_fill(new_capacity, 1);
        }
    }

    size_t capacity;
    size_t numElements = 0;
    /// Non-zero for the non-null values, valid only if `hasNulls`.
    PaddedPODArray<char> notNull;
    bool hasNulls = false;
    /// For string batches read with lazy dictionary decoding.
    bool isEncoded = false;
};

template <typename T>
struct NumericVectorBatch : public ColumnVectorBatch
{
    explicit NumericVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_) { data.resize(capacity_); }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            data.resize(new_capacity);
        }
    }

    PaddedPODArray<T> data;
};

/// For BOOLEAN, BYTE, SHORT, INT, LONG and DATE (days since the epoch).
using LongVectorBatch = NumericVectorBatch<Int64>;
/// For FLOAT and DOUBLE.
using DoubleVectorBatch = NumericVectorBatch<double>;

struct StringVectorBatch : public ColumnVectorBatch
{
    explicit StringVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_)
    {
        data.resize(capacity_);
        length.resize(capacity_);
    }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            data.resize(new_capacity);
            length.resize(new_capacity);
        }
    }

    /// Pointers to the values, which point into `blob` (or into the dictionary) for the batches
    /// filled by the reader, and into the source columns for the batches given to the writer.
    PaddedPODArray<char *> data;
    PaddedPODArray<Int64> length;
    PaddedPODArray<char> blob;
};

struct StringDictionary
{
    PaddedPODArray<char> dictionaryBlob;
    /// `dictionaryOffset[i]` is the start of the i-th entry; the last element is the total size.
    PaddedPODArray<Int64> dictionaryOffset;
};

/// A string batch with the dictionary indices, without decoding the values.
struct EncodedStringVectorBatch : public StringVectorBatch
{
    explicit EncodedStringVectorBatch(size_t capacity_) : StringVectorBatch(capacity_) { index.resize(capacity_); }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            StringVectorBatch::resize(new_capacity);
            index.resize(new_capacity);
        }
    }

    std::shared_ptr<StringDictionary> dictionary;
    PaddedPODArray<Int64> index;
};

struct StructVectorBatch : public ColumnVectorBatch
{
    explicit StructVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_) {}

    void resize(size_t new_capacity) override
    {
        ColumnVectorBatch::resize(new_capacity);
        for (auto * field : fields)
            field->resize(new_capacity);
    }

    void addField(std::unique_ptr<ColumnVectorBatch> field)
    {
        fields.push_back(field.get());
        owned_fields.push_back(std::move(field));
    }

    std::vector<ColumnVectorBatch *> fields;

private:
    std::vector<std::unique_ptr<ColumnVectorBatch>> owned_fields;
};

struct ListVectorBatch : public ColumnVectorBatch
{
    explicit ListVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_) { offsets.resize(capacity_ + 1); }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            offsets.resize(new_capacity + 1);
        }
    }

    /// The elements of the i-th list are `[offsets[i], offsets[i + 1])`.
    PaddedPODArray<Int64> offsets;
    std::unique_ptr<ColumnVectorBatch> elements;
};

struct MapVectorBatch : public ColumnVectorBatch
{
    explicit MapVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_) { offsets.resize(capacity_ + 1); }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            offsets.resize(new_capacity + 1);
        }
    }

    PaddedPODArray<Int64> offsets;
    std::unique_ptr<ColumnVectorBatch> keys;
    std::unique_ptr<ColumnVectorBatch> elements;
};

struct UnionVectorBatch : public ColumnVectorBatch
{
    explicit UnionVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_)
    {
        tags.resize(capacity_);
        offsets.resize(capacity_);
    }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            tags.resize(new_capacity);
            offsets.resize(new_capacity);
        }
    }

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
    explicit Decimal64VectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_) { values.resize(capacity_); }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            values.resize(new_capacity);
        }
    }

    Int32 precision = 0;
    Int32 scale = 0;
    PaddedPODArray<Int64> values;
};

/// For DECIMAL with the precision above 18 or unspecified (zero).
struct Decimal128VectorBatch : public ColumnVectorBatch
{
    explicit Decimal128VectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_) { values.resize(capacity_); }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            values.resize(new_capacity);
        }
    }

    Int32 precision = 0;
    Int32 scale = 0;
    PaddedPODArray<Int128> values;
};

struct TimestampVectorBatch : public ColumnVectorBatch
{
    explicit TimestampVectorBatch(size_t capacity_) : ColumnVectorBatch(capacity_)
    {
        data.resize(capacity_);
        nanoseconds.resize(capacity_);
    }

    void resize(size_t new_capacity) override
    {
        if (capacity < new_capacity)
        {
            ColumnVectorBatch::resize(new_capacity);
            data.resize(new_capacity);
            nanoseconds.resize(new_capacity);
        }
    }

    /// Seconds since the epoch in UTC.
    PaddedPODArray<Int64> data;
    PaddedPODArray<Int64> nanoseconds;
};

/// Creates a batch tree matching the type. With `encoded`, the string columns get
/// `EncodedStringVectorBatch`, which the reader fills without decoding the dictionaries.
std::unique_ptr<ColumnVectorBatch> createRowBatch(const Type & type, size_t capacity, bool encoded = false);

}
