#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <span>

namespace DB
{

class Field;

class ArgumentValue
{
public:
    ArgumentValue(const IColumn & column_, size_t row_);

    bool isNull() const;
    UInt64 getUInt() const;
    Int64 getInt() const;
    Float64 getFloat64() const;
    Field getField() const;
    void appendTo(IColumn & result) const;

private:
    const IColumn * column;
    size_t row;
};

class WindowArguments
{
    size_t findBlock(size_t argument, Int64 position) const;

public:
    struct Block
    {
        std::span<const ColumnPtr> columns;
        Int64 first_position = 0;
        Int64 end_position = 0;
    };

    explicit WindowArguments(VectorWithMemoryTracking<size_t> column_indices_);

    void setBlocks(const VectorWithMemoryTracking<Block> & blocks_);

    size_t size() const;
    ArgumentValue at(size_t argument, Int64 position) const;

private:
    VectorWithMemoryTracking<size_t> column_indices;
    VectorWithMemoryTracking<Block> blocks;
    mutable VectorWithMemoryTracking<size_t> last_used_block_for_arg;
};

}
