#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/SetVariants.h>
#include <Core/ColumnNumbers.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class IntersectOrExceptTransform : public IProcessor
{
using Operator = ASTSelectIntersectExceptQuery::Operator;

public:
    IntersectOrExceptTransform(const Block & header_, Operator operator_);

    String getName() const override { return "IntersectOrExcept"; }

protected:
    Status prepare() override;

    void work() override;

private:
    Operator current_operator;

    ColumnNumbers key_columns_pos;
    std::optional<SetVariants> data;
    Sizes key_sizes;

    Chunk current_input_chunk;
    Chunk current_output_chunk;

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
