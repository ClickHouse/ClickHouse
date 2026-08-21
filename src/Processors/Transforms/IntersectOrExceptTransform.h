#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Interpreters/SetVariants.h>
#include <Core/ColumnNumbers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class Block;

class IntersectOrExceptTransform : public IProcessor
{
using Operator = ASTSelectIntersectExceptQuery::Operator;

public:
    IntersectOrExceptTransform(SharedHeader header_, Operator operator_);

    String getName() const override { return "IntersectOrExcept"; }

protected:
    Status prepare() override;

    void work() override;

private:
    Operator current_operator;

    ColumnNumbers key_columns_pos;
    std::optional<SetVariants> data;
    Sizes key_sizes;

    /// For ALL variants: tracks row occurrence counts instead of just presence.
    HashMap<UInt128, UInt64, UInt128TrivialHash> counts;

    Chunk current_input_chunk;
    Chunk current_output_chunk;

    bool finished_second_input = false;
    bool has_input = false;

    bool isAllOperator() const
    {
        return current_operator == Operator::EXCEPT_ALL
            || current_operator == Operator::INTERSECT_ALL;
    }

    static UInt128 hashRow(const ColumnRawPtrs & columns, size_t row);

    void accumulate(Chunk chunk);

    void filter(Chunk & chunk);

    template <typename Method>
    void addToSet(Method & method, const ColumnRawPtrs & key_columns, size_t rows, SetVariants & variants) const;

    template <typename Method>
    size_t buildFilter(Method & method, const ColumnRawPtrs & columns,
        IColumn::Filter & filter, size_t rows, SetVariants & variants) const;
};

}
