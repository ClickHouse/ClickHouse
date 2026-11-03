#pragma once

#include <WindowFunctions/Common/WindowArguments.h>

#include <Interpreters/WindowDescription.h>

#include <Columns/IColumn_fwd.h>

#include <Common/DevectorWithMemoryTracking.h>

#include <vector>

namespace DB
{

class Block;

class SlidingArguments
{
public:
    SlidingArguments(const Block & input_header, const std::vector<WindowFunctionDescription> & functions);

    void add(const Columns & materialized_columns, Int64 rows_count);
    void pop();
    void startPartition(Int64 closed_partition_length);

    WindowArguments view(size_t function) const;

private:
    std::vector<std::vector<size_t>> argument_positions;
    std::vector<DevectorWithMemoryTracking<WindowArguments::Block>> function_blocks;
    Int64 partition_begin_position = 0;
    Int64 end_position = 0;
};

}
