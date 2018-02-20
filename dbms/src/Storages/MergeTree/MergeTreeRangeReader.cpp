#include <Storages/MergeTree/MergeTreeReader.h>
#include <Columns/FilterDescription.h>
#include <ext/range.h>
#include <Columns/ColumnsCommon.h>

#if __SSE2__
#include <emmintrin.h>
#endif

namespace DB
{

MergeTreeRangeReader::DelayedStream::DelayedStream(
        size_t from_mark, size_t index_granularity, MergeTreeReader * merge_tree_reader)
        : current_mark(from_mark), current_offset(0), num_delayed_rows(0)
        , index_granularity(index_granularity), merge_tree_reader(merge_tree_reader)
        , continue_reading(false), is_finished(false)
{
}

size_t MergeTreeRangeReader::DelayedStream::position() const
{
    return current_mark * index_granularity + current_offset + num_delayed_rows;
}

size_t MergeTreeRangeReader::DelayedStream::readRows(Block & block, size_t num_rows)
{
    if (num_rows)
    {
        size_t rows_read = merge_tree_reader->readRows(current_mark, continue_reading, num_rows, block);
        continue_reading = true;

        /// Zero rows_read my be either because reading has finished
        ///  or because there is no columns we can read in current part (for example, all columns are default).
        /// In the last case we can't finish reading, but it's also ok for the first case
        ///  because we can finish reading by calculation the number of pending rows.
        if (0 < rows_read && rows_read < num_rows)
            is_finished = true;

        return rows_read;
    }

    return 0;
}

size_t MergeTreeRangeReader::DelayedStream::read(Block & block, size_t from_mark, size_t offset, size_t num_rows)
{
    if (position() == from_mark * index_granularity + offset)
    {
        num_delayed_rows += num_rows;
        return 0;
    }
    else
    {
        size_t read_rows = finalize(block);

        continue_reading = false;
        current_mark = from_mark;
        current_offset = offset;
        num_delayed_rows = num_rows;

        return read_rows;
    }
}

size_t MergeTreeRangeReader::DelayedStream::finalize(Block & block)
{
    if (current_offset && !continue_reading)
    {
        size_t granules_to_skip = current_offset / index_granularity;
        current_mark += granules_to_skip;
        current_offset -= granules_to_skip * index_granularity;

        if (current_offset)
        {
            Block temp_block;
            readRows(temp_block, current_offset);
        }
    }

    size_t rows_to_read = num_delayed_rows;
    current_offset += num_delayed_rows;
    num_delayed_rows = 0;

    return readRows(block, rows_to_read);
}


MergeTreeRangeReader::Stream::Stream(
        size_t from_mark, size_t to_mark, size_t index_granularity, MergeTreeReader * merge_tree_reader)
        : current_mark(from_mark), offset_after_current_mark(0)
        , index_granularity(index_granularity), last_mark(to_mark)
        , stream(from_mark, index_granularity, merge_tree_reader)
{
}

void MergeTreeRangeReader::Stream::checkNotFinished() const
{
    if (isFinished())
        throw Exception("Cannot read out of marks range.", ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeRangeReader::Stream::checkEnoughSpaceInCurrentGranule(size_t num_rows) const
{
    if (num_rows + offset_after_current_mark > index_granularity)
        throw Exception("Cannot read from granule more than index_granularity.", ErrorCodes::LOGICAL_ERROR);
}

size_t MergeTreeRangeReader::Stream::readRows(Block & block, size_t num_rows)
{
    size_t rows_read = stream.read(block, current_mark, offset_after_current_mark, num_rows);

    if (stream.isFinished())
        finish();

    return rows_read;
}

size_t MergeTreeRangeReader::Stream::read(Block & block, size_t num_rows, bool skip_remaining_rows_in_current_granule)
{
    checkEnoughSpaceInCurrentGranule(num_rows);

    if (num_rows)
    {
        checkNotFinished();

        size_t read_rows = readRows(block, num_rows);
        offset_after_current_mark += num_rows;

        if (offset_after_current_mark == index_granularity || skip_remaining_rows_in_current_granule)
        {
            /// Start new granule; skipped_rows_after_offset is already zero.
            ++current_mark;
            offset_after_current_mark = 0;
        }

        return read_rows;
    }
    else
    {
        /// Nothing to read.
        if (skip_remaining_rows_in_current_granule)
        {
            /// Skip the rest of the rows in granule and start new one.
            checkNotFinished();

            ++current_mark;
            offset_after_current_mark = 0;
        }

        return 0;
    }
}

void MergeTreeRangeReader::Stream::skip(size_t num_rows)
{
    if (num_rows)
    {
        checkNotFinished();
        checkEnoughSpaceInCurrentGranule(num_rows);

        offset_after_current_mark += num_rows;

        if (offset_after_current_mark == index_granularity)
        {
            /// Start new granule; skipped_rows_after_offset is already zero.
            ++current_mark;
            offset_after_current_mark = 0;
        }
    }
}

size_t MergeTreeRangeReader::Stream::finalize(Block & block)
{
    size_t read_rows = stream.finalize(block);

    if (stream.isFinished())
        finish();

    return read_rows;
}


void MergeTreeRangeReader::ReadResult::addGranule(size_t num_rows)
{
    rows_per_granule.push_back(num_rows);
    num_read_rows += num_rows;
}

void MergeTreeRangeReader::ReadResult::adjustLastGranule(size_t num_rows_to_subtract)
{
    if (rows_per_granule.empty())
        throw Exception("Can't adjust last granule because no granules were added.", ErrorCodes::LOGICAL_ERROR);

    if (num_rows_to_subtract > rows_per_granule.back())
        throw Exception("Can't adjust last granule because it has " + toString(rows_per_granule.back())
                        + "rows, but try to subtract " + toString(num_rows_to_subtract) + " rows.",
                        ErrorCodes::LOGICAL_ERROR);

    rows_per_granule.back() -= num_rows_to_subtract;
    num_read_rows -= num_rows_to_subtract;
}

void MergeTreeRangeReader::ReadResult::clear()
{
    /// Need to save information about the number of granules.
    rows_per_granule.assign(rows_per_granule.size(), 0);
    num_filtered_rows += num_read_rows - num_zeros_in_filter;
    num_read_rows = 0;
    num_added_rows = 0;
    num_zeros_in_filter = 0;
    filter = nullptr;
    block.clear();
}

void MergeTreeRangeReader::ReadResult::optimize()
{
    if (num_read_rows == 0 || !filter)
        return;

    ConstantFilterDescription constant_filter_description(*filter);

    if (constant_filter_description.always_false)
        clear();
    else if (constant_filter_description.always_true)
        filter = nullptr;
    else
    {
        ColumnPtr prev_filter = std::move(filter);
        FilterDescription prev_description(*prev_filter);

        MutableColumnPtr new_filter_ptr = ColumnUInt8::create(prev_description.data->size());
        auto & new_filter = static_cast<ColumnUInt8 &>(*new_filter_ptr);
        IColumn::Filter & new_data = new_filter.getData();

        collapseZeroTails(*prev_description.data, new_data);

        size_t num_removed_zeroes = new_filter.size() - num_read_rows;
        num_read_rows = new_filter.size();
        num_zeros_in_filter -= num_removed_zeroes;

        filter = std::move(new_filter_ptr);
    }
}

void MergeTreeRangeReader::ReadResult::collapseZeroTails(const IColumn::Filter & filter, IColumn::Filter & new_filter)
{
    auto filter_data = filter.data();
    auto new_filter_data = new_filter.data();

    size_t rows_in_filter_from_prev_iteration = filter.size() - num_read_rows;
    if (rows_in_filter_from_prev_iteration)
    {
        memcpySmallAllowReadWriteOverflow15(new_filter_data, filter_data, rows_in_filter_from_prev_iteration);
        filter_data += rows_in_filter_from_prev_iteration;
        new_filter_data += rows_in_filter_from_prev_iteration;
    }

    for (auto & rows_to_read : rows_per_granule)
    {
        /// Count the number of zeros at the end of filter for rows were read from current granule.
        size_t filtered_rows_num_at_granule_end = numZerosInTail(filter_data, filter_data + rows_to_read);
        rows_to_read -= filtered_rows_num_at_granule_end;

        memcpySmallAllowReadWriteOverflow15(new_filter_data, filter_data, rows_to_read);
        filter_data += rows_to_read;
        new_filter_data += rows_to_read;

        filter_data += filtered_rows_num_at_granule_end;
    }

    new_filter.resize(new_filter_data - new_filter.data());
}

size_t MergeTreeRangeReader::ReadResult::numZerosInTail(const UInt8 * begin, const UInt8 * end)
{
    size_t count = 0;

#if __SSE2__ && __POPCNT__
    const __m128i zero16 = _mm_setzero_si128();
    while (end - begin >= 64)
    {
        end -= 64;
        auto pos = end;
        UInt64 val =
                static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos)),
                        zero16)))
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 16)),
                        zero16))) << 16)
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 32)),
                        zero16))) << 32)
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 48)),
                        zero16))) << 48);
        if (val == 0)
            count += 64;
        else
        {
            count += __builtin_clzll(val);
            return count;
        }
    }
#endif

    while (end > begin && *(--end) == 0)
    {
        ++count;
    }
    return count;
}

size_t MergeTreeRangeReader::ReadResult::numZerosInFilter() const
{
    if (!filter)
        return 0;

    {
        ConstantFilterDescription constant_filter_description(*filter);
        if (constant_filter_description.always_false)
            return filter->size();
        if (constant_filter_description.always_true)
            return 0;
    }

    FilterDescription description(*filter);

    auto data = description.data;
    auto size = description.data->size();

    return size - countBytesInFilter(*data);
}

void MergeTreeRangeReader::ReadResult::setFilter(ColumnPtr filter_)
{
    if (!filter_ && filter)
        throw Exception("Can't remove exising filter with empty.", ErrorCodes::LOGICAL_ERROR);

    if (!filter_)
        return;

    if (filter_->size() < num_read_rows)
        throw Exception("Can't set filter because it's size is " + toString(filter_->size()) + " but "
                        + toString(num_read_rows) + " rows was read.", ErrorCodes::LOGICAL_ERROR);

    if (filter && filter_->size() != filter->size())
        throw Exception("Can't set filter because it's size is " + toString(filter_->size()) + " but previous filter"
                        + " has size " + toString(filter->size()) + ".", ErrorCodes::LOGICAL_ERROR);

    filter = std::move(filter_);
    size_t num_zeros = numZerosInFilter();

    if (num_zeros < num_zeros_in_filter)
        throw Exception("New filter has less zeros than previous.", ErrorCodes::LOGICAL_ERROR);

    size_t added_zeros = num_zeros - num_zeros_in_filter;
    num_added_rows -= added_zeros;
    num_filtered_rows += added_zeros;
    num_zeros_in_filter = num_zeros;
}


MergeTreeRangeReader::MergeTreeRangeReader(
        MergeTreeReader * merge_tree_reader, size_t index_granularity,
        MergeTreeRangeReader * prev_reader, ExpressionActionsPtr prewhere_actions,
        const String * prewhere_column_name, const Names * ordered_names, bool always_reorder)
        : index_granularity(index_granularity), merge_tree_reader(merge_tree_reader)
        , prev_reader(prev_reader), prewhere_actions(std::move(prewhere_actions))
        , prewhere_column_name(prewhere_column_name), ordered_names(ordered_names)
        , always_reorder(always_reorder), is_initialized(true)
{
}

bool MergeTreeRangeReader::isReadingFinished() const
{
    return prev_reader ? prev_reader->isReadingFinished() : stream.isFinished();
}

size_t MergeTreeRangeReader::numReadRowsInCurrentGranule() const
{
    return prev_reader ? prev_reader->numReadRowsInCurrentGranule() : stream.numReadRowsInCurrentGranule();
}
size_t MergeTreeRangeReader::numPendingRowsInCurrentGranule() const
{
    return prev_reader ? prev_reader->numPendingRowsInCurrentGranule() : stream.numPendingRowsInCurrentGranule();
}

size_t MergeTreeRangeReader::numPendingRows() const
{
    return prev_reader ? prev_reader->numPendingRows() : stream.numPendingRows();
}

bool MergeTreeRangeReader::isCurrentRangeFinished() const
{
    return prev_reader ? prev_reader->isCurrentRangeFinished() : stream.isFinished();
}

MergeTreeRangeReader::ReadResult MergeTreeRangeReader::read(size_t max_rows, MarkRanges & ranges)
{
    if (max_rows == 0)
        throw Exception("Expected at least 1 row to read, got 0.", ErrorCodes::LOGICAL_ERROR);

    ReadResult read_result;

    if (prev_reader)
    {
        read_result = prev_reader->read(max_rows, ranges);
        continueReadingChain(read_result);
    }
    else
        read_result = startReadingChain(max_rows, ranges);

    if (!read_result.block)
        return read_result;

    executePrewhereActionsAndFilterColumns(read_result);
    return read_result;
}

MergeTreeRangeReader::ReadResult MergeTreeRangeReader::startReadingChain(size_t max_rows, MarkRanges & ranges)
{
    ReadResult result;

    /// Stream is lazy. result.num_added_rows is the number of rows added to block which is not equal to
    /// result.num_rows_read until call to stream.finalize(). Also result.num_added_rows may be less than
    /// result.num_rows_read if the last granule in range also the last in part (so we have to adjust last granule).
    {
        size_t space_left = max_rows;
        while (space_left && (!stream.isFinished() || !ranges.empty()))
        {
            if (stream.isFinished())
            {
                result.addRows(stream.finalize(result.block));
                stream = Stream(ranges.back().begin, ranges.back().end, index_granularity, merge_tree_reader);
                result.addRange(ranges.back());
                ranges.pop_back();
            }

            auto rows_to_read = std::min(space_left, stream.numPendingRowsInCurrentGranule());
            bool last = rows_to_read == space_left;
            result.addRows(stream.read(result.block, rows_to_read, !last));
            result.addGranule(rows_to_read);
            space_left -= rows_to_read;
        }
    }

    result.addRows(stream.finalize(result.block));

    auto last_granule = result.rowsPerGranule().back();
    auto added_rows = result.numAddedRows();
    auto num_read_rows = result.numReadRows();

    if (num_read_rows - last_granule > added_rows)
        throw Exception("RangeReader expected reading of at least " + toString(num_read_rows - last_granule) +
                        " rows, but only " + toString(added_rows) + " was read.", ErrorCodes::LOGICAL_ERROR);

    /// Last granule may be incomplete.
    size_t adjustment = max_rows - added_rows;
    result.adjustLastGranule(adjustment);

    return result;
}

void MergeTreeRangeReader::continueReadingChain(ReadResult & result)
{
    if (result.numReadRows() == 0)
    {
        /// If zero rows were read on prev step, than there is no more rows to read.
        /// Last granule may have less rows than index_granularity, so finish reading manually.
        stream.finish();
        return;
    }

    size_t rows_to_skip_in_last_granule = 0;

    {
        size_t rows_in_last_granule = result.rowsPerGranule().back();
        result.optimize();
        rows_to_skip_in_last_granule = rows_in_last_granule - result.rowsPerGranule().back();

        if (auto & filter = result.getFilter())
        {
            if (ConstantFilterDescription(*filter).always_false)
                throw Exception("Shouldn't read rows with constant zero prewhere result.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    auto & rows_per_granule = result.rowsPerGranule();
    auto & started_ranges = result.startedRanges();

    size_t added_rows = 0;
    size_t next_range_to_start = 0;

    auto size = rows_per_granule.size();
    for (auto i : ext::range(0, size))
    {
        if (next_range_to_start < started_ranges.size()
            && i == started_ranges[next_range_to_start].num_granules_read_before_start)
        {
            added_rows += stream.finalize(result.block);
            auto & range = started_ranges[next_range_to_start].range;
            stream = Stream(range.begin, range.end, index_granularity, merge_tree_reader);
        }

        bool last = i + 1 == size;
        added_rows += stream.read(result.block, rows_per_granule[i], !last);
    }

    stream.skip(rows_to_skip_in_last_granule);
    added_rows += stream.finalize(result.block);

    /// added_rows may be zero if all columns were read in prewhere and it's ok.
    if (added_rows && added_rows != result.numReadRows())
        throw Exception("RangeReader read " + toString(added_rows) + " rows, but "
                        + toString(result.numReadRows()) + " expected.", ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeRangeReader::executePrewhereActionsAndFilterColumns(ReadResult & result)
{

    const auto & columns = merge_tree_reader->getColumns();

    auto filterColumns = [&result, &columns](const IColumn::Filter & filter)
    {
        for (const auto & column : columns)
        {
            if (result.block.has(column.name))
            {
                auto & column_with_type_and_name = result.block.getByName(column.name);
                if (column_with_type_and_name.column)
                    column_with_type_and_name.column = column_with_type_and_name.column->filter(filter, -1);
            }
        }
    };

    auto filterBlock = [&result](const IColumn::Filter & filter)
    {
        for (const auto i : ext::range(0, result.block.columns()))
        {
            auto & col = result.block.safeGetByPosition(i);

            if (col.column && col.column->size() == filter.size())
                col.column = col.column->filter(filter, -1);
        }
    };

    if (auto & filter = result.getFilter())
    {
        ConstantFilterDescription constant_filter_description(*filter);
        if (constant_filter_description.always_false)
            throw Exception("RangeReader mustn't execute prewhere actions with const zero prewhere result.",
                            ErrorCodes::LOGICAL_ERROR);
        if (!constant_filter_description.always_true)
        {
            FilterDescription filter_and_holder(*filter);
            filterColumns(*filter_and_holder.data);
        }
    }

    merge_tree_reader->fillMissingColumns(result.block, *ordered_names, always_reorder);

    ColumnPtr filter;
    if (prewhere_actions)
    {
        prewhere_actions->execute(result.block);
        auto & prewhere_column = result.block.getByName(*prewhere_column_name);

        ConstantFilterDescription constant_filter_description(*prewhere_column.column);
        if (constant_filter_description.always_false)
        {
            result.clear();
            return;
        }
        else if (!constant_filter_description.always_true)
        {
            filter = std::move(prewhere_column.column);
            FilterDescription filter_and_holder(*filter);
            filterBlock(*filter_and_holder.data);
        }

        prewhere_column.column = prewhere_column.type->createColumnConst(result.block.rows(), UInt64(1));
    }

    if (filter && result.getFilter())
    {
        /// TODO: implement for prewhere chain.
        /// In order to do it we need combine filter and result.filter, where filter filters only '1' in result.filter.
        throw Exception("MergeTreeRangeReader chain with several prewhere actions in not implemented.",
                        ErrorCodes::LOGICAL_ERROR);
    }

    if (filter)
        result.setFilter(filter);
}

}
