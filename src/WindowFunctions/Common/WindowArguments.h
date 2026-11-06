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

struct ArgumentsSegment
{
    std::span<const IColumn * const> columns;
    size_t row_begin = 0;
    size_t row_end = 0;
};

class WindowArguments
{
public:
    struct Block
    {
        ColumnRawPtrs columns;
        Int64 begin_position = 0;
        Int64 end_position = 0;
    };

    explicit WindowArguments(Int64 num_arguments_);

    void setBlocks(std::span<const Block> blocks_, Int64 first_row_position_);

    Int64 size() const;
    ArgumentValue at(size_t argument, Int64 row) const;
    ArgumentsSegment segment(Int64 begin_row, Int64 end_row) const;

private:
    std::span<const Block> blocks;
    Int64 first_row_position = 0;
    const Int64 num_arguments;
    mutable VectorWithMemoryTracking<size_t> last_used_block;
};

}
