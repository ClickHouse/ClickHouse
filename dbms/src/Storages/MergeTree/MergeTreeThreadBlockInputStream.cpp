#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadBlockInputStream.h>
#include <Columns/ColumnNullable.h>
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
    max_block_size_marks(max_block_size_rows / storage.index_granularity)
{
}


MergeTreeThreadBlockInputStream::~MergeTreeThreadBlockInputStream() = default;


MergeTreeThreadBlockInputStream::MergeTreeThreadBlockInputStream(
    const size_t thread,
    const MergeTreeReadPoolPtr & pool,
    const size_t min_marks_to_read_,
    const size_t max_block_size_rows,
    //size_t preferred_block_size_bytes,
    MergeTreeData & storage,
    const bool use_uncompressed_cache,
    const ExpressionActionsPtr & prewhere_actions,
    const String & prewhere_column,
    const Settings & settings,
    const Names & virt_column_names)
    :
    MergeTreeBaseBlockInputStream{storage, prewhere_actions, prewhere_column, max_block_size_rows, 0 /* preferred_block_size_bytes */,
        settings.min_bytes_to_use_direct_io, settings.max_read_buffer_size, use_uncompressed_cache, true, virt_column_names},
    thread{thread},
    pool{pool}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    if (max_block_size_rows)
    {
        min_marks_to_read = (min_marks_to_read_ * storage.index_granularity + max_block_size_rows - 1)
                            / max_block_size_rows * max_block_size_rows / storage.index_granularity;
    }
    else
        min_marks_to_read = min_marks_to_read_;

    log = &Logger::get("MergeTreeThreadBlockInputStream");
}


String MergeTreeThreadBlockInputStream::getID() const
{
    std::stringstream res;
    /// @todo print some meaningful information
    res << static_cast<const void *>(this);
    return res.str();
}


Block MergeTreeThreadBlockInputStream::readImpl()
{
    Block res;

    while (!res && !isCancelled())
    {
        if (!task && !getNewTask())
            break;

        res = readFromPart(task.get());

        if (res)
            injectVirtualColumns(res, task.get());

        if (task->mark_ranges.empty())
            task = {};
    }

    return res;
}


/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadBlockInputStream::getNewTask()
{
    task = pool->getTask(min_marks_to_read, thread);

    if (!task)
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        reader = {};
        pre_reader = {};
        return false;
    }

    const std::string path = task->data_part->getFullPath();

    /// Allows pool to reduce number of threads in case of too slow reads.
    auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info) { pool->profileFeedback(info); };

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.context.getUncompressedCache();

        owned_mark_cache = storage.context.getMarkCache();

        reader = std::make_unique<MergeTreeReader>(
            path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
            storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);

        if (prewhere_actions)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                storage, task->mark_ranges, min_bytes_to_use_direct_io,
                max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);
    }
    else
    {
        /// retain avg_value_size_hints
        reader = std::make_unique<MergeTreeReader>(
            path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
            storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size,
            reader->getAvgValueSizeHints(), profile_callback);

        if (prewhere_actions)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                storage, task->mark_ranges, min_bytes_to_use_direct_io,
                max_read_buffer_size, pre_reader->getAvgValueSizeHints(), profile_callback);
    }

    return true;
}


Block MergeTreeBaseBlockInputStream::readFromPart(MergeTreeReadTask * task)
{
    Block res;

    LOG_TRACE(log, "Try Read block with ~" << max_block_size_marks * storage.index_granularity << " rows");

    // For preferred_block_size_bytes
    size_t res_block_size_bytes = 0;
    bool bytes_exceeded = false;

    if (prewhere_actions)
    {
        do
        {
            /// Let's read the full block of columns needed to calculate the expression in PREWHERE.
            size_t space_left = std::max(1LU, max_block_size_marks);
            MarkRanges ranges_to_read;

            while (!task->mark_ranges.empty() && space_left && !isCancelled())
            {
                res_block_size_bytes = res.bytes();
                if (preferred_block_size_bytes && res_block_size_bytes >= preferred_block_size_bytes)
                {
                    bytes_exceeded = true;
                    break;
                }

                auto & range = task->mark_ranges.back();

                size_t marks_to_read = std::min(range.end - range.begin, space_left);
                if (preferred_block_size_bytes)
                {
                    size_t recommended_marks = task->size_predictor->estimateNumMarks(preferred_block_size_bytes - res_block_size_bytes);
                    marks_to_read = std::min(marks_to_read, std::max(1UL, recommended_marks));
                }

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
                reader->fillMissingColumnsAndReorder(res, task->ordered_names);
        }
        while (!task->mark_ranges.empty() && !res && !isCancelled());
    }
    else
    {
        size_t space_left = std::max(1LU, max_block_size_marks);

        while (!task->mark_ranges.empty() && space_left && !isCancelled())
        {
            res_block_size_bytes = res.bytes();
            if (preferred_block_size_bytes && res_block_size_bytes >= preferred_block_size_bytes)
            {
                bytes_exceeded = true;
                break;
            }

            auto & range = task->mark_ranges.back();

            size_t marks_to_read = std::min(range.end - range.begin, space_left);
            if (preferred_block_size_bytes)
            {
                size_t recommended_marks = task->size_predictor->estimateNumMarks(preferred_block_size_bytes - res_block_size_bytes);
                marks_to_read = std::min(marks_to_read, std::max(1UL, recommended_marks));
            }

            reader->readRange(range.begin, range.begin + marks_to_read, res);

            if (preferred_block_size_bytes)
                task->size_predictor->update(res, marks_to_read);

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

    LOG_TRACE(log, "Read block with " << res.rows() << " rows");

    if (preferred_block_size_bytes && bytes_exceeded)
    {
        res_block_size_bytes = res.bytes();
        double accuracy = static_cast<double>(res_block_size_bytes) / preferred_block_size_bytes - 1.;
        LOG_TRACE(log, "Read block with " << res_block_size_bytes << " bytes (" << task->size_predictor->block_size_bytes << "), " << res.rows() << " rows, accuracy " << accuracy * 100 << " percent");
    }

    return res;
}


void MergeTreeBaseBlockInputStream::injectVirtualColumns(Block & block, const MergeTreeReadTask * task)
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
