#include <Storages/MergeTree/MergeTreeBaseBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
}


MergeTreeBaseBlockInputStream::MergeTreeBaseBlockInputStream(
    MergeTreeData & storage,
    const ExpressionActionsPtr & prewhere_actions,
    const String & prewhere_column_name,
    size_t max_block_size_rows,
    size_t preferred_block_size_bytes,
    size_t preferred_max_column_in_block_size_bytes,
    size_t min_bytes_to_use_direct_io,
    size_t max_read_buffer_size,
    bool use_uncompressed_cache,
    bool save_marks_in_cache,
    const Names & virt_column_names)
:
    storage(storage),
    prewhere_actions(prewhere_actions),
    prewhere_column_name(prewhere_column_name),
    max_block_size_rows(max_block_size_rows),
    preferred_block_size_bytes(preferred_block_size_bytes),
    preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes),
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

        if (task->isFinished())
            task.reset();
    }

    return res;
}


Block MergeTreeBaseBlockInputStream::readFromPart()
{
    Block res;

    if (task->size_predictor)
        task->size_predictor->startBlock();

    const auto max_block_size_rows = this->max_block_size_rows;
    const auto preferred_block_size_bytes = this->preferred_block_size_bytes;
    const auto preferred_max_column_in_block_size_bytes = this->preferred_max_column_in_block_size_bytes;
    const auto index_granularity = storage.index_granularity;
    const double min_filtration_ratio = 0.00001;

    auto estimateNumRows = [preferred_block_size_bytes, max_block_size_rows,
        index_granularity, preferred_max_column_in_block_size_bytes, min_filtration_ratio](
        MergeTreeReadTask & task, MergeTreeRangeReader & reader)
    {
        if (!task.size_predictor)
            return max_block_size_rows;

        /// Calculates number of rows will be read using preferred_block_size_bytes.
        /// Can't be less than index_granularity.
        size_t rows_to_read = task.size_predictor->estimateNumRows(preferred_block_size_bytes);
        if (!rows_to_read)
            return rows_to_read;
        rows_to_read = std::max(index_granularity, rows_to_read);

        if (preferred_max_column_in_block_size_bytes)
        {
            /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
            size_t rows_to_read_for_max_size_column
                = task.size_predictor->estimateNumRowsForMaxSizeColumn(preferred_max_column_in_block_size_bytes);
            double filtration_ratio = std::max(min_filtration_ratio, 1.0 - task.size_predictor->filtered_rows_ratio);
            size_t rows_to_read_for_max_size_column_with_filtration
                = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

            /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than index_granularity.
            rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
        }

        size_t unread_rows_in_current_granule = reader.numPendingRowsInCurrentGranule();
        if (unread_rows_in_current_granule >= rows_to_read)
            return rows_to_read;

        size_t granule_to_read = (rows_to_read + reader.numReadRowsInCurrentGranule() + index_granularity / 2) / index_granularity;
        return index_granularity * granule_to_read - reader.numReadRowsInCurrentGranule();
    };

    // read rows from reader and clear columns
    auto skipRows = [&estimateNumRows](
        Block & block, MergeTreeRangeReader & reader, MergeTreeReadTask & task, size_t rows)
    {
        size_t recommended_rows = rows;
        if (task.size_predictor)
            recommended_rows = std::max<size_t>(1, estimateNumRows(task, reader));
        while (rows)
        {
            size_t rows_to_skip = std::min(rows, recommended_rows);
            rows -= rows_to_skip;
            reader.read(block, rows_to_skip);
            for (const auto i : ext::range(0, block.columns()))
            {
                auto & col = block.getByPosition(i);
                if (task.column_name_set.count(col.name))
                {
                    if (const ColumnArray * column_array = typeid_cast<const ColumnArray *>(col.column.get()))
                    {
                        /// ColumnArray columns in block could have common offset column, which is used while reading.
                        /// This is in case of nested data structures.

                        /// TODO Very dangerous and unclear. Get rid of this after implemented full-featured Nested data type.

                        /// Have to call resize(0) instead of cloneEmpty to save structure.
                        /// (To keep offsets possibly shared between different arrays.)
                        static_cast<ColumnArray &>(column_array->assumeMutableRef()).getOffsets().resize(0);
                        /// It's ok until multidimensional arrays are not stored in MergeTree.
                        static_cast<ColumnArray &>(column_array->assumeMutableRef()).getDataPtr() = column_array->getDataPtr()->cloneEmpty();
                    }
                    else
                        col.column = col.column->cloneEmpty();
                }
            }
        }
    };

    if (prewhere_actions)
    {
        do
        {
            /// Let's read the full block of columns needed to calculate the expression in PREWHERE.
            MarkRanges ranges_to_read;
            /// Last range may be partl read. The same number of rows we need to read after prewhere
            size_t rows_was_read_in_last_range = 0;
            std::optional<MergeTreeRangeReader> pre_range_reader;

            auto processNextRange = [& ranges_to_read, & rows_was_read_in_last_range, & pre_range_reader](
                MergeTreeReadTask & task, MergeTreeReader & pre_reader)
            {
                auto & range = task.mark_ranges.back();
                pre_range_reader = pre_reader.readRange(range.begin, range.end);
                ranges_to_read.push_back(range);
                rows_was_read_in_last_range = 0;
                task.mark_ranges.pop_back();
            };

            if (task->current_range_reader)
            {
                /// Havn't finihsed reading at last step. Copy state for prewhere columns
                pre_range_reader = task->current_range_reader->copyForReader(*pre_reader);
                if (task->number_of_rows_to_skip)
                {
                    /// number_of_rows_to_skip already was read for prewhere columns. skip them.
                    pre_range_reader = pre_range_reader->getFutureState(task->number_of_rows_to_skip);
                    pre_range_reader->disableNextSeek();
                }
            }
            else
                processNextRange(*task, *pre_reader);

            /// FIXME: size prediction model is updated by filtered rows, but it predicts size of unfiltered rows also
            size_t recommended_rows = estimateNumRows(*task, *pre_range_reader);
            if (res && recommended_rows < 1)
                break;
            size_t space_left = std::max(static_cast<decltype(max_block_size_rows)>(1), std::min(max_block_size_rows, recommended_rows));

            while ((pre_range_reader || !task->mark_ranges.empty()) && space_left && !isCancelled())
            {
                if (!pre_range_reader)
                    processNextRange(*task, *pre_reader);

                size_t rows_to_read = std::min(pre_range_reader->numPendingRows(), space_left);
                size_t read_rows = pre_range_reader->read(res, rows_to_read);
                rows_was_read_in_last_range += read_rows;
                if (pre_range_reader->isReadingFinished())
                    pre_range_reader.reset();

                space_left -= read_rows;
            }

            /// In case of isCancelled.
            if (!res)
            {
                task->current_range_reader.reset();
                return res;
            }

            progressImpl({ res.rows(), res.bytes() });
            pre_reader->fillMissingColumns(res, task->ordered_names, task->should_reorder, res.rows());

            /// Compute the expression in PREWHERE.
            prewhere_actions->execute(res);

            ColumnPtr prewhere_column = res.getByName(prewhere_column_name).column;
            if (task->remove_prewhere_column)
                res.erase(prewhere_column_name);

            const auto pre_bytes = res.bytes();

            ConstantFilterDescription constant_filter_description(*prewhere_column);

            /** If the filter is a constant (for example, it says PREWHERE 0),
              * then either return an empty block, or return the block unchanged.
              */
            if (constant_filter_description.always_false)
            {
	            /*
                  If this filter is PREWHERE 0, MergeTree Stream can be marked as done,
                  and this task can be clear.
                  If we don't mark this task finished here, readImpl could
                  jump into endless loop.
                  Error scenario:
                  select * from table where isNull(NOT_NULLABLE_COLUMN) AND OTHER PRED;
                  and isNull pred is promoted to PREWHERE.
                  (though it is difficult to reproduce)
                */
                task->current_range_reader.reset();
                task->mark_ranges.clear();
                res.clear();
                return res;
            }
            else if (constant_filter_description.always_true)
            {
                if (task->current_range_reader)
                {
                    if (task->number_of_rows_to_skip)
                        skipRows(res, *task->current_range_reader, *task, task->number_of_rows_to_skip);
                    size_t rows_to_read = ranges_to_read.empty()
                        ? rows_was_read_in_last_range : task->current_range_reader->numPendingRows();
                    task->current_range_reader->read(res, rows_to_read);
                }

                for (auto range_idx : ext::range(0, ranges_to_read.size()))
                {
                    const auto & range = ranges_to_read[range_idx];
                    task->current_range_reader = reader->readRange(range.begin, range.end);
                    size_t rows_to_read = range_idx + 1 == ranges_to_read.size()
                        ? rows_was_read_in_last_range : task->current_range_reader->numPendingRows();
                    task->current_range_reader->read(res, rows_to_read);
                }

                if (!pre_range_reader)
                    task->current_range_reader.reset();
                task->number_of_rows_to_skip = 0;

                progressImpl({ 0, res.bytes() - pre_bytes });
            }
            else
            {
                FilterDescription filter_and_holder(*prewhere_column);

                const auto & pre_filter = *filter_and_holder.data;
                auto & number_of_rows_to_skip = task->number_of_rows_to_skip;
                if (!task->current_range_reader)
                    number_of_rows_to_skip = 0;
                IColumn::Filter post_filter(pre_filter.size());

                /// Let's read the rest of the columns in the required segments and compose our own filter for them.
                size_t pre_filter_pos = 0;
                size_t post_filter_pos = 0;

                size_t next_range_idx = 0;
                while (pre_filter_pos < pre_filter.size())
                {
                    if (!task->current_range_reader)
                    {
                        if (next_range_idx == ranges_to_read.size())
                            throw Exception("Not enough ranges to read after prewhere.", ErrorCodes::LOGICAL_ERROR);
                        const auto & range = ranges_to_read[next_range_idx++];
                        task->current_range_reader = reader->readRange(range.begin, range.end);
                    }
                    MergeTreeRangeReader & range_reader = *task->current_range_reader;
                    size_t current_range_rows_read = 0;
                    auto pre_filter_begin_pos = pre_filter_pos;

                    /// Now we need to read the same number of rows as in prewhere.
                    size_t rows_to_read = next_range_idx == ranges_to_read.size()
                        ? rows_was_read_in_last_range : (task->current_range_reader->numPendingRows() - number_of_rows_to_skip);

                    auto readRows = [&]()
                    {
                        if (pre_filter_pos != pre_filter_begin_pos)
                        {
                            /// Fulfilling the promise to read (pre_filter_pos - pre_filter_begin_pos) rows
                            auto rows = pre_filter_pos - pre_filter_begin_pos;
                            memcpy(&post_filter[post_filter_pos], &pre_filter[pre_filter_begin_pos], rows);
                            post_filter_pos += rows;
                            current_range_rows_read += rows;
                            if (number_of_rows_to_skip)
                            {
                                /** Wasn't able to skip 'number_of_rows_to_skip' with false prewhere conditon
                                 * Just read them and throw away. */
                                skipRows(res, range_reader, *task, number_of_rows_to_skip);
                                number_of_rows_to_skip = 0;
                            }
                            range_reader.read(res, rows);
                        }
                    };

                    /** (pre_filter_pos - pre_filter_begin_pos) here is the number of rows we promies to read, but
                        haven't read yet to merge consecutive nonempy granulas. */
                    while (current_range_rows_read + (pre_filter_pos - pre_filter_begin_pos) < rows_to_read)
                    {
                        auto rows_should_be_copied = pre_filter_pos - pre_filter_begin_pos;
                        auto range_reader_with_skipped_rows = range_reader.getFutureState(number_of_rows_to_skip + rows_should_be_copied);
                        auto unread_rows_in_current_granule = range_reader_with_skipped_rows.numPendingRowsInCurrentGranule();

                        const size_t limit = std::min(pre_filter.size(), pre_filter_pos + unread_rows_in_current_granule);
                        bool will_read_until_mark = unread_rows_in_current_granule == limit - pre_filter_pos;

                        if (memoryIsZero(&pre_filter[pre_filter_pos], (limit - pre_filter_pos) * sizeof(pre_filter[0])))
                        {
                            /// Zero! Prewhere condition is false for all (limit - pre_filter_pos) rows.
                            readRows();

                            if (will_read_until_mark)
                            {
                                /// Can skip the rest of granule with false prewhere conditon right now.
                                do
                                {
                                    size_t rows_was_skipped = range_reader.skipToNextMark();
                                    if (number_of_rows_to_skip < rows_was_skipped)
                                    {
                                        current_range_rows_read += rows_was_skipped - number_of_rows_to_skip;
                                        number_of_rows_to_skip = 0;
                                    }
                                    else
                                        number_of_rows_to_skip -= rows_was_skipped;
                                }
                                while (number_of_rows_to_skip);
                            }
                            else
                            {
                                /// Here reading seems to be done. It's still possible to skip rows during next reading.
                                number_of_rows_to_skip += limit - pre_filter_pos;
                                current_range_rows_read += limit - pre_filter_pos;
                            }

                            pre_filter_begin_pos = limit;
                        }
                        pre_filter_pos = limit;
                    }

                    readRows();

                    if (next_range_idx != ranges_to_read.size())
                        task->current_range_reader.reset();
                }

                if (!pre_range_reader)
                    task->current_range_reader.reset();

                if (!post_filter_pos)
                {
                    if (task->size_predictor)
                        task->size_predictor->updateFilteredRowsRation(pre_filter.size(), pre_filter.size());
                    res.clear();
                    continue;
                }

                progressImpl({ 0, res.bytes() - pre_bytes });

                post_filter.resize(post_filter_pos);

                /// At this point we may have arrays with non-zero offsets but with empty data,
                ///  as a result of reading components of Nested data structures with no data in filesystem.
                /// We must fill these arrays to filter them correctly.

                reader->fillMissingColumns(res, task->ordered_names, task->should_reorder, post_filter.size(), true);

                /// Filter the columns related to PREWHERE using pre_filter,
                ///  other columns - using post_filter.
                size_t rows = 0;
                for (const auto i : ext::range(0, res.columns()))
                {
                    auto & col = res.getByPosition(i);
                    if (col.name == prewhere_column_name && res.columns() > 1)
                        continue;
                    col.column = col.column->filter(task->column_name_set.count(col.name) ? post_filter : pre_filter, -1);
                    rows = col.column->size();
                }
                if (task->size_predictor)
                    task->size_predictor->updateFilteredRowsRation(pre_filter.size(), pre_filter.size() - rows);

                /// Replace column with condition value from PREWHERE to a constant.
                if (!task->remove_prewhere_column)
                    res.getByName(prewhere_column_name).column = DataTypeUInt8().createColumnConst(rows, UInt64(1))->convertToFullColumnIfConst();
            }

            if (res)
            {
                if (task->size_predictor)
                    task->size_predictor->update(res);

                reader->fillMissingColumns(res, task->ordered_names, true, res.rows());

                res.checkNumberOfRows();
            }
        }
        while (!task->isFinished() && !res && !isCancelled());
    }
    else
    {
        size_t space_left = std::max(static_cast<decltype(max_block_size_rows)>(1), max_block_size_rows);
        while (!task->isFinished() && space_left && !isCancelled())
        {
            if (!task->current_range_reader)
            {
                auto & range = task->mark_ranges.back();
                task->current_range_reader = reader->readRange(range.begin, range.end);
                task->mark_ranges.pop_back();
            }

            size_t rows_to_read = space_left;
            size_t recommended_rows = estimateNumRows(*task, *task->current_range_reader);
            if (res && recommended_rows < 1)
                break;
            rows_to_read = std::min(rows_to_read, std::max(static_cast<decltype(recommended_rows)>(1), recommended_rows));

            size_t rows_was_read = task->current_range_reader->read(res, rows_to_read);
            if (task->current_range_reader->isReadingFinished())
                task->current_range_reader.reset();

            if (res && task->size_predictor)
            {
                task->size_predictor->update(res);
            }

            space_left -= rows_was_read;
        }

        /// In the case of isCancelled.
        if (!res)
            return res;

        progressImpl({ res.rows(), res.bytes() });
        reader->fillMissingColumns(res, task->ordered_names, task->should_reorder, res.rows());
    }

    return res;
}


void MergeTreeBaseBlockInputStream::injectVirtualColumns(Block & block) const
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
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, task->data_part->name)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                block.insert({ column, std::make_shared<DataTypeString>(), virt_column_name});
            }
            else if (virt_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, static_cast<UInt64>(task->part_index_in_query))->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                block.insert({ column, std::make_shared<DataTypeUInt64>(), virt_column_name});
            }
        }
    }
}


MergeTreeBaseBlockInputStream::~MergeTreeBaseBlockInputStream() = default;

}
