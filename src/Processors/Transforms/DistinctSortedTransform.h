#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/SetVariants.h>
#include <Core/SortDescription.h>


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
        Block header_, SortDescription sort_description, const SizeLimits & set_size_limits_, UInt64 limit_hint_, const Names & columns);

    String getName() const override { return "DistinctSortedTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnRawPtrs getKeyColumns(const Chunk & chunk) const;
    /// When clearing_columns changed, we can clean HashSet to memory optimization
    /// clearing_columns is a left-prefix of SortDescription exists in key_columns
    ColumnRawPtrs getClearingColumns(const ColumnRawPtrs & key_columns) const;
    static bool rowsEqual(const ColumnRawPtrs & lhs, size_t n, const ColumnRawPtrs & rhs, size_t m);

    /// return true if has new data
    template <typename Method>
    bool buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        const ColumnRawPtrs & clearing_hint_columns,
        IColumn::Filter & filter,
        size_t rows,
        ClearableSetVariants & variants) const;

    Block header;
    SortDescription description;

    struct PreviousChunk
    {
        Chunk chunk;
        ColumnRawPtrs clearing_hint_columns;
    };
    PreviousChunk prev_chunk;

    Names columns_names;
    ClearableSetVariants data;
    Sizes key_sizes;
    UInt64 limit_hint;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;
};

}
