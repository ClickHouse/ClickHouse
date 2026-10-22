#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <span>
#include <vector>

namespace DB
{

class Field;

struct ArgumentBlock
{
    std::span<const ColumnPtr> columns;
    Int64 first_row = 0;
    Int64 end_row = 0;
};

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

struct ArgumentSegment
{
    std::span<const IColumn *> columns;
    size_t row_begin;
    size_t row_end;
};

class ArgumentSegments
{
    class Iterator
    {
    public:
        Iterator(const ArgumentSegments & segments_, size_t block_index_);

        ArgumentSegment operator*() const;
        Iterator & operator++();
        bool operator!=(const Iterator & other) const;

    private:
        const ArgumentSegments * segments;
        size_t block_index;
    };

public:
    ArgumentSegments(std::span<const ArgumentBlock> blocks_, std::span<const IColumn *> columns_, size_t arguments_count_, Int64 begin_row_, Int64 end_row_);

    Iterator begin() const;
    Iterator end() const;

private:
    std::span<const ArgumentBlock> blocks;
    std::span<const IColumn *> columns;
    size_t arguments_count;
    Int64 begin_row;
    Int64 end_row;
};

class WindowArguments
{
    size_t blockOf(Int64 position) const;

public:
    explicit WindowArguments(std::vector<size_t> argument_column_indices_);

    void rebuild(std::span<const ArgumentBlock> blocks_);

    size_t size() const;
    ArgumentValue at(size_t argument, Int64 position) const;
    ArgumentSegments segments(Int64 begin, Int64 end) const;

private:
    std::vector<size_t> argument_column_indices;
    std::span<const ArgumentBlock> blocks;
    std::vector<const IColumn *> columns;
    mutable size_t resolved = 0;
};

}
