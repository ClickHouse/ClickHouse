#include <Storages/MergeTree/MergeTreeReader.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnNothing.h>
#include <ext/range.h>
#include <DataTypes/DataTypeNothing.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace DB
{

MergeTreeRangeReader::DelayedStream::DelayedStream(
        size_t from_mark, MergeTreeReader * merge_tree_reader)
        : current_mark(from_mark), current_offset(0), num_delayed_rows(0)
        , merge_tree_reader(merge_tree_reader)
        , index_granularity(&(merge_tree_reader->data_part->index_granularity))
        , continue_reading(false), is_finished(false)
{
}

size_t MergeTreeRangeReader::DelayedStream::position() const
{
    size_t num_rows_before_current_mark = index_granularity->getMarkStartingRow(current_mark);
    return num_rows_before_current_mark + current_offset + num_delayed_rows;
}

size_t MergeTreeRangeReader::DelayedStream::readRows(Block & block, size_t num_rows)
{
    if (num_rows)
    {
        size_t rows_read = merge_tree_reader->readRows(current_mark, continue_reading, num_rows, block);
        continue_reading = true;

        /// Zero rows_read maybe either because reading has finished
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
    size_t num_rows_before_from_mark = index_granularity->getMarkStartingRow(from_mark);
    /// We already stand accurately in required position,
    /// so because stream is lazy, we don't read anything
    /// and only increment amount delayed_rows
    if (position() == num_rows_before_from_mark + offset)
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
    /// We need to skip some rows before reading
    if (current_offset && !continue_reading)
    {
        for (size_t mark_num : ext::range(current_mark, index_granularity->getMarksCount()))
        {
            size_t mark_index_granularity = index_granularity->getMarkRows(mark_num);
            if (current_offset >= mark_index_granularity)
            {
                current_offset -= mark_index_granularity;
                current_mark++;
            }
            else
                break;

        }

        /// Skip some rows from beging of granule
        /// We don't know size of rows in compressed granule,
        /// so have to read them and throw out
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
        size_t from_mark, size_t to_mark,  MergeTreeReader * merge_tree_reader)
        : current_mark(from_mark), offset_after_current_mark(0)
        , last_mark(to_mark)
        , merge_tree_reader(merge_tree_reader)
        , index_granularity(&(merge_tree_reader->data_part->index_granularity))
        , current_mark_index_granularity(index_granularity->getMarkRows(from_mark))
        , stream(from_mark, merge_tree_reader)
{
    size_t marks_count = index_granularity->getMarksCount();
    if (from_mark >= marks_count)
        throw Exception("Trying create stream to read from mark №"+ toString(current_mark) + " but total marks count is "
            + toString(marks_count), ErrorCodes::LOGICAL_ERROR);

    if (last_mark > marks_count)
        throw Exception("Trying create stream to read to mark №"+ toString(current_mark) + " but total marks count is "
            + toString(marks_count), ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeRangeReader::Stream::checkNotFinished() const
{
    if (isFinished())
        throw Exception("Cannot read out of marks range.", ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeRangeReader::Stream::checkEnoughSpaceInCurrentGranule(size_t num_rows) const
{
    if (num_rows + offset_after_current_mark > current_mark_index_granularity)
        throw Exception("Cannot read from granule more than index_granularity.", ErrorCodes::LOGICAL_ERROR);
}

size_t MergeTreeRangeReader::Stream::readRows(Block & block, size_t num_rows)
{
    size_t rows_read = stream.read(block, current_mark, offset_after_current_mark, num_rows);

    if (stream.isFinished())
        finish();

    return rows_read;
}

void MergeTreeRangeReader::Stream::toNextMark()
{
    ++current_mark;

    size_t total_marks_count = index_granularity->getMarksCount();
    if (current_mark < total_marks_count)
        current_mark_index_granularity = index_granularity->getMarkRows(current_mark);
    else if (current_mark == total_marks_count)
        current_mark_index_granularity = 0; /// HACK?
    else
        throw Exception("Trying to read from mark " + toString(current_mark) + ", but total marks count " + toString(total_marks_count), ErrorCodes::LOGICAL_ERROR);

    offset_after_current_mark = 0;
}

size_t MergeTreeRangeReader::Stream::read(Block & block, size_t num_rows, bool skip_remaining_rows_in_current_granule)
{
    checkEnoughSpaceInCurrentGranule(num_rows);

    if (num_rows)
    {
        checkNotFinished();

        size_t read_rows = readRows(block, num_rows);

        offset_after_current_mark += num_rows;

        /// Start new granule; skipped_rows_after_offset is already zero.
        if (offset_after_current_mark == current_mark_index_granularity || skip_remaining_rows_in_current_granule)
            toNextMark();

        return read_rows;
    }
    else
    {
        /// Nothing to read.
        if (skip_remaining_rows_in_current_granule)
        {
            /// Skip the rest of the rows in granule and start new one.
            checkNotFinished();
            toNextMark();
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

        if (offset_after_current_mark == current_mark_index_granularity)
        {
            /// Start new granule; skipped_rows_after_offset is already zero.
            toNextMark();
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
    total_rows_per_granule += num_rows;
}

void MergeTreeRangeReader::ReadResult::adjustLastGranule()
{
    size_t num_rows_to_subtract = total_rows_per_granule - num_read_rows;

    if (rows_per_granule.empty())
        throw Exception("Can't adjust last granule because no granules were added.", ErrorCodes::LOGICAL_ERROR);

    if (num_rows_to_subtract > rows_per_granule.back())
        throw Exception("Can't adjust last granule because it has " + toString(rows_per_granule.back())
                        + " rows, but try to subtract " + toString(num_rows_to_subtract) + " rows.",
                        ErrorCodes::LOGICAL_ERROR);

    rows_per_granule.back() -= num_rows_to_subtract;
    total_rows_per_granule -= num_rows_to_subtract;
}

void MergeTreeRangeReader::ReadResult::clear()
{
    /// Need to save information about the number of granules.
    num_rows_to_skip_in_last_granule += rows_per_granule.back();
    rows_per_granule.assign(rows_per_granule.size(), 0);
    total_rows_per_granule = 0;
    filter_holder = nullptr;
    filter = nullptr;
}

void MergeTreeRangeReader::ReadResult::optimize()
{
    if (total_rows_per_granule == 0 || filter == nullptr)
        return;

    NumRows zero_tails;
    auto total_zero_rows_in_tails = countZeroTails(filter->getData(), zero_tails);

    if (total_zero_rows_in_tails == filter->size())
    {
        clear();
        return;
    }
    else if (total_zero_rows_in_tails == 0 && countBytesInFilter(filter->getData()) == filter->size())
    {
        filter_holder = nullptr;
        filter = nullptr;
        return;
    }

    /// Just a guess. If only a few rows may be skipped, it's better not to skip at all.
    if (2 * total_zero_rows_in_tails > filter->size())
    {

        auto new_filter = ColumnUInt8::create(filter->size() - total_zero_rows_in_tails);
        IColumn::Filter & new_data = new_filter->getData();

        size_t rows_in_last_granule = rows_per_granule.back();

        collapseZeroTails(filter->getData(), new_data, zero_tails);

        total_rows_per_granule = new_filter->size();
        num_rows_to_skip_in_last_granule += rows_in_last_granule - rows_per_granule.back();

        filter = new_filter.get();
        filter_holder = std::move(new_filter);
    }
}

size_t MergeTreeRangeReader::ReadResult::countZeroTails(const IColumn::Filter & filter_vec, NumRows & zero_tails) const
{
    zero_tails.resize(0);
    zero_tails.reserve(rows_per_granule.size());

    auto filter_data = filter_vec.data();

    size_t total_zero_rows_in_tails = 0;

    for (auto rows_to_read : rows_per_granule)
    {
        /// Count the number of zeros at the end of filter for rows were read from current granule.
        zero_tails.push_back(numZerosInTail(filter_data, filter_data + rows_to_read));
        total_zero_rows_in_tails += zero_tails.back();
        filter_data += rows_to_read;
    }

    return total_zero_rows_in_tails;
}

void MergeTreeRangeReader::ReadResult::collapseZeroTails(const IColumn::Filter & filter_vec, IColumn::Filter & new_filter_vec,
                                                         const NumRows & zero_tails)
{
    auto filter_data = filter_vec.data();
    auto new_filter_data = new_filter_vec.data();

    for (auto i : ext::range(0, rows_per_granule.size()))
    {
        auto & rows_to_read = rows_per_granule[i];
        auto filtered_rows_num_at_granule_end = zero_tails[i];

        rows_to_read -= filtered_rows_num_at_granule_end;

        memcpySmallAllowReadWriteOverflow15(new_filter_data, filter_data, rows_to_read);
        filter_data += rows_to_read;
        new_filter_data += rows_to_read;

        filter_data += filtered_rows_num_at_granule_end;
    }

    new_filter_vec.resize(new_filter_data - new_filter_vec.data());
}

size_t MergeTreeRangeReader::ReadResult::numZerosInTail(const UInt8 * begin, const UInt8 * end)
{
    size_t count = 0;

#if defined(__SSE2__) && defined(__POPCNT__)
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

void MergeTreeRangeReader::ReadResult::setFilter(const ColumnPtr & new_filter)
{
    if (!new_filter && filter)
        throw Exception("Can't replace existing filter with empty.", ErrorCodes::LOGICAL_ERROR);

    if (filter)
    {
        size_t new_size = new_filter->size();

        if (new_size != total_rows_per_granule)
            throw Exception("Can't set filter because it's size is " + toString(new_size) + " but "
                            + toString(total_rows_per_granule) + " rows was read.", ErrorCodes::LOGICAL_ERROR);
    }

    ConstantFilterDescription const_description(*new_filter);
    if (const_description.always_false)
        clear();
    else if (!const_description.always_true)
    {
        FilterDescription filter_description(*new_filter);
        filter_holder = filter_description.data_holder ? filter_description.data_holder : new_filter;
        filter = typeid_cast<const ColumnUInt8 *>(filter_holder.get());
        if (!filter)
            throw Exception("setFilter function expected ColumnUInt8.", ErrorCodes::LOGICAL_ERROR);
    }
}


MergeTreeRangeReader::MergeTreeRangeReader(
        MergeTreeReader * merge_tree_reader, MergeTreeRangeReader * prev_reader,
        ExpressionActionsPtr alias_actions, ExpressionActionsPtr prewhere_actions,
        const String * prewhere_column_name, const Names * ordered_names,
        bool always_reorder, bool remove_prewhere_column, bool last_reader_in_chain)
        : merge_tree_reader(merge_tree_reader), index_granularity(&(merge_tree_reader->data_part->index_granularity))
        , prev_reader(prev_reader), prewhere_column_name(prewhere_column_name)
        , ordered_names(ordered_names), alias_actions(alias_actions), prewhere_actions(std::move(prewhere_actions))
        , always_reorder(always_reorder), remove_prewhere_column(remove_prewhere_column)
        , last_reader_in_chain(last_reader_in_chain), is_initialized(true)
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
    if (prev_reader)
        return prev_reader->numPendingRowsInCurrentGranule();

    auto pending_rows = stream.numPendingRowsInCurrentGranule();

    if (pending_rows)
        return pending_rows;

    return numRowsInCurrentGranule();
}


size_t MergeTreeRangeReader::numRowsInCurrentGranule() const
{
    /// If pending_rows is zero, than stream is not initialized.
    if (stream.current_mark_index_granularity)
        return stream.current_mark_index_granularity;

    /// We haven't read anything, return first
    size_t first_mark = merge_tree_reader->getFirstMarkToRead();
    return index_granularity->getMarkRows(first_mark);
}

size_t MergeTreeRangeReader::currentMark() const
{
    return stream.currentMark();
}

size_t MergeTreeRangeReader::Stream::numPendingRows() const
{
    size_t rows_between_marks = index_granularity->getRowsCountInRange(current_mark, last_mark);
    return rows_between_marks - offset_after_current_mark;
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
    size_t prev_bytes = 0;
    bool should_reorder = false;

    if (prev_reader)
    {
        read_result = prev_reader->read(max_rows, ranges);
        prev_bytes = read_result.block.bytes();
        Block block = continueReadingChain(read_result);

        bool should_evaluate_missing_defaults = false;
        if (block)
        {
            /// block.rows() <= read_result.block. We must filter block before adding columns to read_result.block

            /// Fill missing columns before filtering because some arrays from Nested may have empty data.
            merge_tree_reader->fillMissingColumns(block, should_reorder, should_evaluate_missing_defaults, block.rows());

            if (read_result.getFilter())
                filterBlock(block, read_result.getFilter()->getData());
        }
        else
        {
            size_t num_rows = read_result.block.rows();
            if (!read_result.block)
            {
                if (auto * filter = read_result.getFilter())
                    num_rows = countBytesInFilter(filter->getData()); /// All columns were removed and filter is not always true.
                else if (read_result.totalRowsPerGranule())
                    num_rows = read_result.numReadRows();   /// All columns were removed and filter is always true.
                /// else filter is always false.
            }

            /// If block is empty, we still may need to add missing columns.
            /// In that case use number of rows in result block and don't filter block.
            if (num_rows)
                merge_tree_reader->fillMissingColumns(block, should_reorder, should_evaluate_missing_defaults, num_rows);
        }

        for (auto i : ext::range(0, block.columns()))
            read_result.block.insert(std::move(block.getByPosition(i)));

        if (read_result.block)
        {
            if (should_evaluate_missing_defaults)
                merge_tree_reader->evaluateMissingDefaults(read_result.block);
        }
    }
    else
    {
        read_result = startReadingChain(max_rows, ranges);
        if (read_result.block)
        {
            bool should_evaluate_missing_defaults;
            merge_tree_reader->fillMissingColumns(read_result.block, should_reorder, should_evaluate_missing_defaults,
                                                  read_result.block.rows());

            if (should_evaluate_missing_defaults)
                merge_tree_reader->evaluateMissingDefaults(read_result.block);
        }
    }

    if (!read_result.block)
        return read_result;

    read_result.addNumBytesRead(read_result.block.bytes() - prev_bytes);

    executePrewhereActionsAndFilterColumns(read_result);

    if (last_reader_in_chain && (should_reorder || always_reorder))
        merge_tree_reader->reorderColumns(read_result.block, *ordered_names, prewhere_column_name);

    return read_result;
}

void MergeTreeRangeReader::filterBlock(Block & block, const IColumn::Filter & filter) const
{
    for (const auto i : ext::range(0, block.columns()))
    {
        auto & col = block.getByPosition(i);

        if (col.column)
        {
            col.column = col.column->filter(filter, -1);

            if (col.column->empty())
            {
                block.clear();
                return;
            }
        }
    }
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
                stream = Stream(ranges.back().begin, ranges.back().end, merge_tree_reader);
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

    /// Last granule may be incomplete.
    result.adjustLastGranule();

    return result;
}

Block MergeTreeRangeReader::continueReadingChain(ReadResult & result)
{
    Block block;

    if (result.rowsPerGranule().empty())
    {
        /// If zero rows were read on prev step, than there is no more rows to read.
        /// Last granule may have less rows than index_granularity, so finish reading manually.
        stream.finish();
        return block;
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
            added_rows += stream.finalize(block);
            auto & range = started_ranges[next_range_to_start].range;
            ++next_range_to_start;
            stream = Stream(range.begin, range.end, merge_tree_reader);
        }

        bool last = i + 1 == size;
        added_rows += stream.read(block, rows_per_granule[i], !last);
    }

    stream.skip(result.numRowsToSkipInLastGranule());
    added_rows += stream.finalize(block);

    /// added_rows may be zero if all columns were read in prewhere and it's ok.
    if (added_rows && added_rows != result.totalRowsPerGranule())
        throw Exception("RangeReader read " + toString(added_rows) + " rows, but "
                        + toString(result.totalRowsPerGranule()) + " expected.", ErrorCodes::LOGICAL_ERROR);

    return block;
}

void MergeTreeRangeReader::executePrewhereActionsAndFilterColumns(ReadResult & result)
{
    if (!prewhere_actions)
        return;

    if (alias_actions)
        alias_actions->execute(result.block);

    prewhere_actions->execute(result.block);
    auto & prewhere_column = result.block.getByName(*prewhere_column_name);
    size_t prev_rows = result.block.rows();
    ColumnPtr filter = prewhere_column.column;
    prewhere_column.column = nullptr;

    if (result.getFilter())
    {
        /// TODO: implement for prewhere chain.
        /// In order to do it we need combine filter and result.filter, where filter filters only '1' in result.filter.
        throw Exception("MergeTreeRangeReader chain with several prewhere actions in not implemented.",
                        ErrorCodes::LOGICAL_ERROR);
    }

    result.setFilter(filter);
    if (!last_reader_in_chain)
        result.optimize();

    bool filter_always_true = !result.getFilter() && result.totalRowsPerGranule() == filter->size();

    if (result.totalRowsPerGranule() == 0)
        result.block.clear();
    else if (!filter_always_true)
    {
        FilterDescription filter_description(*filter);

        if (last_reader_in_chain)
        {
            size_t num_bytes_in_filter = countBytesInFilter(*filter_description.data);
            if (num_bytes_in_filter == 0)
                result.block.clear();
            else if (num_bytes_in_filter == filter->size())
                filter_always_true = true;
        }

        if (!filter_always_true)
            filterBlock(result.block, *filter_description.data);
    }

    if (!result.block)
        return;

    auto getNumRows = [&]()
    {
        /// If block has single column, it's filter. We need to count bytes in it in order to get the number of rows.
        if (result.block.columns() > 1)
            return result.block.rows();
        else if (result.getFilter())
            return countBytesInFilter(result.getFilter()->getData());
        else
            return prev_rows;
    };

    if (remove_prewhere_column)
        result.block.erase(*prewhere_column_name);
    else
        prewhere_column.column = prewhere_column.type->createColumnConst(getNumRows(), 1u);

    /// If block is empty, create column in order to store rows number.
    if (last_reader_in_chain && result.block.columns() == 0)
        result.block.insert({ColumnNothing::create(getNumRows()), std::make_shared<DataTypeNothing>(), "_nothing"});
}

}
