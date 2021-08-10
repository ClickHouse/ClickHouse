#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/SetVariants.h>
#include <Core/ColumnNumbers.h>
#include <Parsers/ASTIntersectOrExcept.h>


namespace DB
{

class IntersectOrExceptTransform : public IProcessor
{
using Modes = ASTIntersectOrExcept::Modes;

public:
    IntersectOrExceptTransform(const Block & header_, const Modes & modes);

    String getName() const override { return "IntersectExcept"; }

protected:
    Status prepare() override;

    void work() override;

private:
    Modes modes;
    InputPorts::iterator first_input;
    InputPorts::iterator second_input;
    size_t current_operator_pos = 0;

    bool push_empty_chunk = false;
    Chunk empty_chunk;

    ColumnNumbers key_columns_pos;
    std::optional<SetVariants> data;
    Sizes key_sizes;

    Chunk current_input_chunk;
    Chunk current_output_chunk;
    bool more = false;

    bool finished_second_input = false;
    bool has_input = false;

    void accumulate(Chunk chunk);

    void filter(Chunk & chunk);

    template <typename Method>
    void addToSet(Method & method, const ColumnRawPtrs & key_columns, size_t rows, SetVariants & variants) const;

    template <typename Method>
    size_t buildFilter(Method & method, const ColumnRawPtrs & columns,
        IColumn::Filter & filter, size_t rows, SetVariants & variants) const;
};

}
