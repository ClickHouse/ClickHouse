#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Interpreters/SetVariants.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class Block;

class IntersectOrExceptTransform final : public IProcessor
{
using Operator = ASTSelectIntersectExceptQuery::Operator;

public:
    IntersectOrExceptTransform(SharedHeader header_, Operator operator_);

    String getName() const override { return "IntersectOrExcept"; }

protected:
    Status prepare() override;

    void work() override;

private:
    enum class Stage
    {
        ReadLeftInput,
        ReadRightInput,
        ReadRemainingLeftInput,
    };

    Operator current_operator;

    std::optional<SetVariants> data;
    Sizes key_sizes;

    /// For ALL variants: a multiset keyed on the row value, tracking occurrence counts.
    std::optional<CountingSetVariants> counts_data;

    Chunk current_input_chunk;
    Chunk current_output_chunk;
    Chunk left_input_chunk;

    Stage stage = Stage::ReadLeftInput;
    bool has_left_input_chunk = false;
    bool has_right_input_rows = false;
    bool has_input = false;

    bool isAllOperator() const
    {
        return current_operator == Operator::EXCEPT_ALL
            || current_operator == Operator::INTERSECT_ALL;
    }

    bool isIntersectOperator() const
    {
        return current_operator == Operator::INTERSECT_ALL
            || current_operator == Operator::INTERSECT_DISTINCT;
    }

    void accumulate(Chunk chunk);

    void filter(Chunk & chunk);

    template <typename Method>
    void addToSet(Method & method, const ColumnRawPtrs & key_columns, size_t rows, SetVariants & variants) const;

    template <typename Method>
    size_t buildFilter(Method & method, const ColumnRawPtrs & columns,
        IColumn::Filter & filter, size_t rows, SetVariants & variants) const;

    template <typename Method>
    void addToCounts(Method & method, const ColumnRawPtrs & columns, size_t rows, CountingSetVariants & variants) const;

    template <typename Method>
    size_t filterWithCounts(Method & method, const ColumnRawPtrs & columns,
        IColumn::Filter & filter, size_t rows, CountingSetVariants & variants) const;
};

}
