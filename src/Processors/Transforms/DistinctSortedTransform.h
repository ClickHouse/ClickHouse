#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/SetVariants.h>
#include <Core/SortDescription.h>
#include <Core/ColumnNumbers.h>


namespace DB
{

/** This class is intended for implementation of SELECT DISTINCT clause and
  * leaves only unique rows in the stream.
  *
  * Implementation for case, when input stream has rows for same DISTINCT key or at least its prefix,
  *  grouped together (going consecutively).
  *
  * To optimize the SELECT DISTINCT ... LIMIT clause we can
  * set limit_hint to non zero value. So we stop emitting new rows after
  * count of already emitted rows will reach the limit_hint.
  */
class DistinctSortedTransform : public ISimpleTransform
{
public:
    /// Empty columns_ means all columns.
    DistinctSortedTransform(
        const Block & header_,
        const SortDescription & sort_description,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & column_names);

    String getName() const override { return "DistinctSortedTransform"; }

    static bool isApplicable(const Block & header, const SortDescription & sort_description, const Names & column_names);

protected:
    void transform(Chunk & chunk) override;

private:
    bool rowsEqual(const ColumnRawPtrs & lhs, size_t n, const ColumnRawPtrs & rhs, size_t m) const;

    /// return true if has new data
    template <typename Method>
    bool buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        const ColumnRawPtrs & clearing_hint_columns,
        IColumn::Filter & filter,
        size_t rows,
        ClearableSetVariants & variants) const;

    std::vector<int> sorted_columns_nulls_direction;

    struct PreviousChunk
    {
        Chunk chunk;
        ColumnRawPtrs clearing_hint_columns;
    };
    PreviousChunk prev_chunk;

    ColumnNumbers column_positions;      /// DISTINCT columns positions in header
    ColumnNumbers sort_prefix_positions; /// DISTINCT columns positions which form sort prefix of sort description
    ColumnRawPtrs column_ptrs;           /// DISTINCT columns from chunk
    ColumnRawPtrs sort_prefix_columns;   /// DISTINCT columns from chunk which form sort prefix of sort description

    ClearableSetVariants data;
    Sizes key_sizes;
    UInt64 limit_hint;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;
    bool all_columns_const;
};

}
