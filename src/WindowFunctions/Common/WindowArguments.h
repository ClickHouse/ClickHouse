#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <deque>
#include <span>
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
        std::span<const ColumnPtr> columns;
        Int64 first_position = 0;
        Int64 end_position = 0;
    };

    using Blocks = std::deque<Block>;

    explicit WindowArguments(std::vector<size_t> column_indices_);

    /// `at` takes positions counted from the partition start; `partition_start_` is that row's position in `blocks_`.
    void setBlocks(const Blocks & blocks_, Int64 partition_start_);

    size_t size() const;
    ArgumentValue at(size_t argument, Int64 position) const;

private:
    const Block & findBlock(Int64 position) const;

    std::vector<size_t> column_indices;
    const Blocks * blocks = nullptr;
    Int64 partition_start = 0;
    mutable std::vector<const Block *> last_used_block_for_arg;
};

}
