#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/SetVariants.h>
#include <Core/SortDescription.h>

namespace DB
{

class DistinctSortedTransform : public ISimpleTransform
{
public:
    DistinctSortedTransform(
        const Block &header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const SortDescription & sorted_columns_descr_,
        const Names & other_columns_
        );

    String getName() const override { return "DistinctSortedTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ClearableSetVariants data;
    size_t limit_hint;
    size_t cur_block_rows = 0;
    size_t total_rows = 0;

    bool is_new_key = true;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    SortDescription sorted_columns_descr;
    ColumnNumbers sorted_columns_pos;

    ColumnNumbers other_columns_pos;
    Sizes other_columns_sizes;

    MutableColumns current_key;

    static ColumnRawPtrs getColumnsByPositions(const Columns & source_columns, const ColumnNumbers & positions);
    void executeOnInterval(const ColumnRawPtrs & other_columns, IColumn::Filter & filter, size_t key_begin, size_t key_end);
    void setCurrentKey(const ColumnRawPtrs & sorted_columns, IColumn::Filter & filter, size_t key_pos);

    template <typename Method>
    void buildFilterForInterval(
        Method & method,
        const ColumnRawPtrs & other_columns,
        IColumn::Filter & filter,
        size_t key_begin,
        size_t key_end,
        ClearableSetVariants & variants);
};

}
