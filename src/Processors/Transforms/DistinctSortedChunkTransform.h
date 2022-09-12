#pragma once

#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Interpreters/SetVariants.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
///
/// DISTINCT optimization for sorted chunks
///
/// (1) distinct columns are split into two groups - sorted i.e. belong to sorting prefix,
/// and non-sorted (other columns w/o sorting guarantees).
///
/// (2) Rows are split into ranges. Range is a set of rows where the sorting prefix value is the same.
/// If there are no non-sorted columns, then we just skip all rows in range except one.
/// If there are non-sorted columns, then for each range, we use a hash table to find unique rows in a range.
///
/// (3) The implementation also checks if current chunks is continuation of previous one,
/// i.e. sorting prefix value of last row in previous chunk is the same as of first row in current one,
/// so it can correctly process sorted stream as well.
/// For this, we don't clear sorting prefix value and hash table after a range is processed,
/// only right before a new range processing
///
class DistinctSortedChunkTransform : public ISimpleTransform
{
public:
    DistinctSortedChunkTransform(
        const Block & header_,
        const SizeLimits & output_size_limits_,
        UInt64 limit_hint_,
        const SortDescription & sorted_columns_descr_,
        const Names & source_columns_);

    String getName() const override { return "DistinctSortedChunkTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    void initChunkProcessing(const Columns & input_columns);
    std::pair<size_t, size_t> continueWithPrevRange(size_t chunk_rows, IColumn::Filter & filter);
    size_t ordinaryDistinctOnRange(IColumn::Filter & filter, size_t range_begin, size_t range_end, bool clear_data);
    inline void setCurrentKey(size_t row_pos);
    inline bool isCurrentKey(size_t row_pos) const;
    inline size_t getRangeEnd(size_t range_begin, size_t range_end) const;

    template <typename Method>
    size_t buildFilterForRange(Method & method, IColumn::Filter & filter, size_t range_begin, size_t range_end, bool clear_data);


    ClearableSetVariants data;
    const size_t limit_hint;
    size_t total_output_rows = 0;

    /// Restrictions on the maximum size of the output data.
    const SizeLimits output_size_limits;

    const SortDescription sorted_columns_descr;
    ColumnNumbers sorted_columns_pos;
    ColumnRawPtrs sorted_columns; // used during processing

    ColumnNumbers other_columns_pos;
    Sizes other_columns_sizes;
    ColumnRawPtrs other_columns; // used during processing

    MutableColumns current_key;
};

}
