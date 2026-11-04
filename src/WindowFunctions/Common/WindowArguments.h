#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <Common/DevectorWithMemoryTracking.h>
#include <vector>

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
public:
    struct Block
    {
        ColumnRawPtrs columns;
        Int64 begin_position = 0;
        Int64 end_position = 0;
    };

    WindowArguments(const DevectorWithMemoryTracking<Block> & blocks_, Int64 first_row_position_, Int64 num_arguments_);

    Int64 size() const;
    ArgumentValue at(size_t argument, Int64 row) const;

private:
    const DevectorWithMemoryTracking<Block> & blocks;
    const Int64 first_row_position;
    const Int64 num_arguments;
    mutable std::vector<size_t> last_used_block;
};

}
