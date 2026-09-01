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
    /// `read_left_input_first_` waits for the left input to produce a row, or to end, before the
    /// right input is touched, so an empty left input avoids reading a possibly unbounded right one.
    /// Transforms that share a pair of scatters must not wait: one that stops reading its left port
    /// while it drains the right one blocks the left scatter for every other partition, and those
    /// partitions in turn never drain the right scatter, which deadlocks the pipeline. They sample
    /// the left port from `ReadRightInput` instead, which short-circuits without ever waiting.
    IntersectOrExceptTransform(SharedHeader header_, Operator operator_, bool read_left_input_first_ = true);

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

    Stage stage;
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
