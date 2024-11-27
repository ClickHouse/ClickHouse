#pragma once
#include <Storages/MergeTree/MergeTreePrimaryIndexColumn.h>
#include <Common/PODArray.h>

namespace DB
{

template <typename T>
struct IndexBlockDelta final : public IIndexBlock
{
    T base;
    UInt8 bit_size;
    PaddedPODArray<UInt64> delta;

    IndexBlockDelta() = default;

    size_t bytes() const override
    {
        return sizeof(T) + sizeof(UInt8) + delta.size() * sizeof(delta[0]);
    }

    size_t allocatedBytes() const override
    {
        return sizeof(T) + sizeof(UInt8) + delta.allocated_bytes();
    }

    void get(size_t n, Field & field) const override
    {
        UInt64 delta_value = readBitsPacked(delta.begin(), n * bit_size, bit_size);
        field = base + static_cast<T>(delta_value);
    }
};

template <typename T>
struct IndexBlockConstNumeric final : public IIndexBlock
{
    explicit IndexBlockConstNumeric(T value_) : value(value_) {}

    T value;

    size_t bytes() const override { return sizeof(T); }
    size_t allocatedBytes() const override { return sizeof(T); }
    void get(size_t, Field & field) const override { field = value; }
};

IndexColumnPtr createIndexColumnNumeric(ColumnPtr column, size_t block_size, double max_ratio_to_compress);

}
