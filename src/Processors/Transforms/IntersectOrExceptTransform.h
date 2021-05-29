#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/SetVariants.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class IntersectOrExceptTransform : public IProcessor
{
public:
    IntersectOrExceptTransform(bool is_except_, const Block & header_);

    Status prepare() override;
    void work() override;

    String getName() const override { return is_except ? "Except" : "Intersect"; }

private:

    bool push_empty_chunk = false;
    Chunk empty_chunk;

    bool is_except;
    ColumnNumbers key_columns_pos;
    SetVariants data;
    Sizes key_sizes;
    Chunk current_input_chunk;
    Chunk current_output_chunk;
    bool finished_second_input = false;
    bool has_input = false;
    OutputPort & output;

    void accumulate(Chunk chunk);
    void filter(Chunk & chunk);
    template <typename Method>
    void addToSet(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants) const;

    template <typename Method>
    size_t buildFilter(
        Method & method,
        const ColumnRawPtrs & columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants) const;
};

}
