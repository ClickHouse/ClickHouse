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
    /// right_rows_estimate_ is an optional estimate of the number of rows in the right (accumulated)
    /// input; used to pre-size the ALL counting map. 0 means unknown.
    IntersectOrExceptTransform(SharedHeader header_, Operator operator_, size_t right_rows_estimate_ = 0);

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

    /// Estimated rows of the right input (0 = unknown); used once to pre-size counts_data.
    size_t right_rows_estimate = 0;
    bool counts_reserved = false;

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

    /// After the first accumulated chunk, pre-size counts_data using right_rows_estimate scaled by
    /// the observed distinct ratio, to avoid the resize cascade on large inputs.
    void reserveCountsFromEstimate(size_t rows_in_first_chunk);

    template <typename Method>
    void addToSet(Method & method, const ColumnRawPtrs & key_columns, size_t rows, SetVariants & variants) const;

    template <typename Method>
    size_t buildFilter(Method & method, const ColumnRawPtrs & columns,
        IColumn::Filter & filter, size_t rows, SetVariants & variants) const;

    template <typename Method>
    void addToCounts(Method & method, const ColumnRawPtrs & columns, size_t rows, CountingSetVariants & variants) const;

    /// Reserve the method's table for `target` distinct keys, where supported (no-op for fixed maps).
    template <typename Method>
    static void reserveCounts(Method & method, size_t target);

    template <typename Method>
    size_t filterWithCounts(Method & method, const ColumnRawPtrs & columns,
        IColumn::Filter & filter, size_t rows, CountingSetVariants & variants) const;
};

}
