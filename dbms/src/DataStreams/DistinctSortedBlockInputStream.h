#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Limits.h>
#include <Interpreters/SetVariants.h>

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
class DistinctSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// Empty columns_ means all collumns.
    DistinctSortedBlockInputStream(const BlockInputStreamPtr & input, const Limits & limits, size_t limit_hint_, const Names & columns);

    String getName() const override { return "DistinctSorted"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    bool checkLimits() const;

    ColumnRawPtrs getKeyColumns(const Block & block) const;
    /// When clearing_columns changed, we can clean HashSet to memory optimization
    /// clearing_columns is a left-prefix of SortDescription exists in key_columns
    ColumnRawPtrs getClearingColumns(const Block & block, const ColumnRawPtrs & key_columns) const;
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

    const SortDescription & description;

    struct PreviousBlock
    {
        Block block;
        ColumnRawPtrs clearing_hint_columns;
    };
    PreviousBlock prev_block;

    Names columns_names;
    ClearableSetVariants data;
    Sizes key_sizes;
    size_t limit_hint;

    /// Restrictions on the maximum size of the output data.
    size_t max_rows;
    size_t max_bytes;
    OverflowMode overflow_mode;
};

}
