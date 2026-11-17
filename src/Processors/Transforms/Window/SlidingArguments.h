#pragma once

#include <WindowFunctions/Common/WindowArguments.h>

#include <Interpreters/WindowDescription.h>

#include <Columns/IColumn_fwd.h>

#include <Common/DevectorWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class Block;

class SlidingArguments
{
public:
    SlidingArguments(const Block & input_header, const VectorWithMemoryTracking<WindowFunctionDescription> & functions);

    void add(const Columns & materialized_columns, Int64 rows_count);
    void pop();
    void startPartition(Int64 closed_partition_length);

    const WindowArguments & view(size_t function);

private:
    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> argument_positions;
    VectorWithMemoryTracking<DevectorWithMemoryTracking<WindowArguments::Block>> function_blocks;
    VectorWithMemoryTracking<WindowArguments> views;
    Int64 partition_begin_position = 0;
    Int64 end_position = 0;
};

}
