#include "MergeTreeBaseBlockInputStream.h"
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/ColumnConst.h>
#include <ext/range.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


MergeTreeBaseBlockInputStream::MergeTreeBaseBlockInputStream(
    MergeTreeData & storage,
    const ExpressionActionsPtr & prewhere_actions,
    const String & prewhere_column,
    size_t max_block_size_rows,
    size_t preferred_block_size_bytes,
    size_t min_bytes_to_use_direct_io,
    size_t max_read_buffer_size,
    bool use_uncompressed_cache,
    bool save_marks_in_cache,
    const Names & virt_column_names)
:
    storage(storage),
    prewhere_actions(prewhere_actions),
    prewhere_column(prewhere_column),
    max_block_size_rows(max_block_size_rows),
    preferred_block_size_bytes(preferred_block_size_bytes),
    min_bytes_to_use_direct_io(min_bytes_to_use_direct_io),
    max_read_buffer_size(max_read_buffer_size),
    use_uncompressed_cache(use_uncompressed_cache),
    save_marks_in_cache(save_marks_in_cache),
    virt_column_names(virt_column_names),
    max_block_size_marks(max_block_size_rows / storage.index_granularity)
{
}


Block MergeTreeBaseBlockInputStream::readImpl()
{
    Block res;

    while (!res && !isCancelled())
    {
        if (!task && !getNewTask())
            break;

        res = readFromPart();

        if (res)
            injectVirtualColumns(res);

        if (task->mark_ranges.empty())
            task.reset();
    }

    return res;
}


Block MergeTreeBaseBlockInputStream::readFromPart()
{
    Block res;

    if (task->size_predictor)
        task->size_predictor->startBlock();

    if (prewhere_actions)
    {
        do
        {
            /// Let's read the full block of columns needed to calculate the expression in PREWHERE.
            size_t space_left = std::max(1LU, max_block_size_marks);
            MarkRanges ranges_to_read;

            if (task->size_predictor)
            {
                /// FIXME: size prediction model is updated by filtered rows, but it predicts size of unfiltered rows also

                size_t recommended_marks = task->size_predictor->estimateNumMarks(preferred_block_size_bytes, storage.index_granularity);
                if (res && recommended_marks < 1)
                    break;

                space_left = std::min(space_left, std::max(1LU, recommended_marks));
            }

            while (!task->mark_ranges.empty() && space_left && !isCancelled())
            {
                auto & range = task->mark_ranges.back();
                size_t marks_to_read = std::min(range.end - range.begin, space_left);

                pre_reader->readRange(range.begin, range.begin + marks_to_read, res);

                ranges_to_read.emplace_back(range.begin, range.begin + marks_to_read);
                space_left -= marks_to_read;
                range.begin += marks_to_read;
                if (range.begin == range.end)
                    task->mark_ranges.pop_back();
            }

            /// In case of isCancelled.
            if (!res)
                return res;

            progressImpl({ res.rows(), res.bytes() });
            pre_reader->fillMissingColumns(res, task->ordered_names, task->should_reorder);

            /// Compute the expression in PREWHERE.
            prewhere_actions->execute(res);

            ColumnPtr column = res.getByName(prewhere_column).column;
            if (task->remove_prewhere_column)
                res.erase(prewhere_column);

            const auto pre_bytes = res.bytes();

            ColumnPtr observed_column;
            if (column->isNullable())
            {
                ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*column);
                observed_column = nullable_col.getNestedColumn();
            }
            else
                observed_column = column;

            /** If the filter is a constant (for example, it says PREWHERE 1),
                * then either return an empty block, or return the block unchanged.
                */
            if (const auto column_const = typeid_cast<const ColumnConstUInt8 *>(observed_column.get()))
            {
                if (!column_const->getData())
                {
                    res.clear();
                    return res;
                }

                for (const auto & range : ranges_to_read)
                    reader->readRange(range.begin, range.end, res);

                progressImpl({ 0, res.bytes() - pre_bytes });
            }
            else if (const auto column_vec = typeid_cast<const ColumnUInt8 *>(observed_column.get()))
            {
                size_t index_granularity = storage.index_granularity;

                const auto & pre_filter = column_vec->getData();
                IColumn::Filter post_filter(pre_filter.size());

                /// Let's read the rest of the columns in the required segments and compose our own filter for them.
                size_t pre_filter_pos = 0;
                size_t post_filter_pos = 0;

                for (const auto & range : ranges_to_read)
                {
                    auto begin = range.begin;
                    auto pre_filter_begin_pos = pre_filter_pos;

                    for (auto mark = range.begin; mark <= range.end; ++mark)
                    {
                        UInt8 nonzero = 0;

                        if (mark != range.end)
                        {
                            const size_t limit = std::min(pre_filter.size(), pre_filter_pos + index_granularity);
                            for (size_t row = pre_filter_pos; row < limit; ++row)
                                nonzero |= pre_filter[row];
                        }

                        if (!nonzero)
                        {
                            if (mark > begin)
                            {
                                memcpy(
                                    &post_filter[post_filter_pos],
                                    &pre_filter[pre_filter_begin_pos],
                                    pre_filter_pos - pre_filter_begin_pos);
                                post_filter_pos += pre_filter_pos - pre_filter_begin_pos;
                                reader->readRange(begin, mark, res);
                            }
                            begin = mark + 1;
                            pre_filter_begin_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
                        }

                        if (mark < range.end)
                            pre_filter_pos = std::min(pre_filter_pos + index_granularity, pre_filter.size());
                    }
                }

                if (!post_filter_pos)
                {
                    res.clear();
                    continue;
                }

                progressImpl({ 0, res.bytes() - pre_bytes });

                post_filter.resize(post_filter_pos);

                /// Filter the columns related to PREWHERE using pre_filter,
                ///  other columns - using post_filter.
                size_t rows = 0;
                for (const auto i : ext::range(0, res.columns()))
                {
                    auto & col = res.safeGetByPosition(i);
                    if (col.name == prewhere_column && res.columns() > 1)
                        continue;
                    col.column =
                        col.column->filter(task->column_name_set.count(col.name) ? post_filter : pre_filter, -1);
                    rows = col.column->size();
                }

                /// Replace column with condition value from PREWHERE to a constant.
                if (!task->remove_prewhere_column)
                    res.getByName(prewhere_column).column = std::make_shared<ColumnConstUInt8>(rows, 1);
            }
            else
                throw Exception{
                    "Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.",
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER
                };

            if (res)
            {
                if (task->size_predictor)
                    task->size_predictor->update(res);

                reader->fillMissingColumnsAndReorder(res, task->ordered_names);
            }
        }
        while (!task->mark_ranges.empty() && !res && !isCancelled());
    }
    else
    {
        size_t space_left = std::max(1LU, max_block_size_marks);

        while (!task->mark_ranges.empty() && space_left && !isCancelled())
        {
            auto & range = task->mark_ranges.back();

            size_t marks_to_read = std::min(range.end - range.begin, space_left);
            if (task->size_predictor)
            {
                size_t recommended_marks = task->size_predictor->estimateNumMarks(preferred_block_size_bytes, storage.index_granularity);
                if (res && recommended_marks < 1)
                    break;

                marks_to_read = std::min(marks_to_read, std::max(1LU, recommended_marks));
            }

            reader->readRange(range.begin, range.begin + marks_to_read, res);

            if (task->size_predictor)
                task->size_predictor->update(res);

            space_left -= marks_to_read;
            range.begin += marks_to_read;
            if (range.begin == range.end)
                task->mark_ranges.pop_back();
        }

        /// In the case of isCancelled.
        if (!res)
            return res;

        progressImpl({ res.rows(), res.bytes() });
        reader->fillMissingColumns(res, task->ordered_names, task->should_reorder);
    }

    return res;
}


void MergeTreeBaseBlockInputStream::injectVirtualColumns(Block & block)
{
    const auto rows = block.rows();

    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virt_column_names.empty())
    {
        for (const auto & virt_column_name : virt_column_names)
        {
            if (virt_column_name == "_part")
            {
                block.insert(ColumnWithTypeAndName{
                    ColumnConst<String>{rows, task->data_part->name}.convertToFullColumn(),
                    std::make_shared<DataTypeString>(),
                    virt_column_name
                });
            }
            else if (virt_column_name == "_part_index")
            {
                block.insert(ColumnWithTypeAndName{
                    ColumnConst<UInt64>{rows, task->part_index_in_query}.convertToFullColumn(),
                    std::make_shared<DataTypeUInt64>(),
                    virt_column_name
                });
            }
        }
    }
}


MergeTreeBaseBlockInputStream::~MergeTreeBaseBlockInputStream() = default;

}
