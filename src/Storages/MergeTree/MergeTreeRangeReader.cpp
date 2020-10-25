#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <ext/range.h>
#include <DataTypes/DataTypeNothing.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


static void filterColumns(Columns & columns, const IColumn::Filter & filter)
{
    for (auto & column : columns)
    {
        if (column)
        {
            column = column->filter(filter, -1);

            if (column->empty())
            {
                columns.clear();
                return;
            }
        }
    }
}


MergeTreeRangeReader::DelayedStream::DelayedStream(
        size_t from_mark, IMergeTreeReader * merge_tree_reader_)
        : current_mark(from_mark), current_offset(0), num_delayed_rows(0)
        , merge_tree_reader(merge_tree_reader_)
        , index_granularity(&(merge_tree_reader->data_part->index_granularity))
        , continue_reading(false), is_finished(false)
{
}

size_t MergeTreeRangeReader::DelayedStream::position() const
{
    size_t num_rows_before_current_mark = index_granularity->getMarkStartingRow(current_mark);
    return num_rows_before_current_mark + current_offset + num_delayed_rows;
}

size_t MergeTreeRangeReader::DelayedStream::readRows(Columns & columns, size_t num_rows)
{
    if (num_rows)
    {
        size_t rows_read = merge_tree_reader->readRows(current_mark, continue_reading, num_rows, columns);
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

size_t MergeTreeRangeReader::DelayedStream::read(Columns & columns, size_t from_mark, size_t offset, size_t num_rows)
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
        size_t read_rows = finalize(columns);

        continue_reading = false;
        current_mark = from_mark;
        current_offset = offset;
        num_delayed_rows = num_rows;

        return read_rows;
    }
}

size_t MergeTreeRangeReader::DelayedStream::finalize(Columns & columns)
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

        /// Skip some rows from begin of granule.
        /// We don't know size of rows in compressed granule,
        /// so have to read them and throw out.
        if (current_offset)
        {
            Columns tmp_columns;
            tmp_columns.resize(columns.size());
            readRows(tmp_columns, current_offset);
        }
    }

    size_t rows_to_read = num_delayed_rows;
    current_offset += num_delayed_rows;
    num_delayed_rows = 0;

    return readRows(columns, rows_to_read);
}


MergeTreeRangeReader::Stream::Stream(
        size_t from_mark, size_t to_mark, IMergeTreeReader * merge_tree_reader_)
        : current_mark(from_mark), offset_after_current_mark(0)
        , last_mark(to_mark)
        , merge_tree_reader(merge_tree_reader_)
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

size_t MergeTreeRangeReader::Stream::readRows(Columns & columns, size_t num_rows)
{
    size_t rows_read = stream.read(columns, current_mark, offset_after_current_mark, num_rows);

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

size_t MergeTreeRangeReader::Stream::read(Columns & columns, size_t num_rows, bool skip_remaining_rows_in_current_granule)
{
    checkEnoughSpaceInCurrentGranule(num_rows);

    if (num_rows)
    {
        checkNotFinished();

        size_t read_rows = readRows(columns, num_rows);

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

size_t MergeTreeRangeReader::Stream::finalize(Columns & columns)
{
    size_t read_rows = stream.finalize(columns);

    if (stream.isFinished())
        finish();

    return read_rows;
}


void MergeTreeRangeReader::ReadResult::addGranule(size_t num_rows_)
{
    rows_per_granule.push_back(num_rows_);
    total_rows_per_granule += num_rows_;
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

void MergeTreeRangeReader::ReadResult::shrink(Columns & old_columns)
{
    for (auto & column : old_columns)
    {
        if (!column)
            continue;
        auto new_column = column->cloneEmpty();
        new_column->reserve(total_rows_per_granule);
        for (size_t j = 0, pos = 0; j < rows_per_granule_original.size(); pos += rows_per_granule_original[j++])
        {
            if (rows_per_granule[j])
                new_column->insertRangeFrom(*column, pos, rows_per_granule[j]);
        }
        column = std::move(new_column);
    }
}

void MergeTreeRangeReader::ReadResult::setFilterConstTrue()
{
    clearFilter();
    filter_holder = DataTypeUInt8().createColumnConst(num_rows, 1u);
}

void MergeTreeRangeReader::ReadResult::setFilterConstFalse()
{
    clearFilter();
    columns.clear();
    num_rows = 0;
}

void MergeTreeRangeReader::ReadResult::optimize(bool can_read_incomplete_granules)
{
    if (total_rows_per_granule == 0 || filter == nullptr)
        return;

    NumRows zero_tails;
    auto total_zero_rows_in_tails = countZeroTails(filter->getData(), zero_tails, can_read_incomplete_granules);

    if (total_zero_rows_in_tails == filter->size())
    {
        clear();
        return;
    }
    else if (total_zero_rows_in_tails == 0 && countBytesInResultFilter(filter->getData()) == filter->size())
    {
        setFilterConstTrue();
        return;
    }
    /// Just a guess. If only a few rows may be skipped, it's better not to skip at all.
    else if (2 * total_zero_rows_in_tails > filter->size())
    {
        for (auto i : ext::range(0, rows_per_granule.size()))
        {
            rows_per_granule_original.push_back(rows_per_granule[i]);
            rows_per_granule[i] -= zero_tails[i];
        }
        num_rows_to_skip_in_last_granule += rows_per_granule_original.back() - rows_per_granule.back();

        filter_original = filter;
        filter_holder_original = std::move(filter_holder);

        /// Check if const 1 after shrink
        if (countBytesInResultFilter(filter->getData()) + total_zero_rows_in_tails == total_rows_per_granule)
        {
            total_rows_per_granule = total_rows_per_granule - total_zero_rows_in_tails;
            num_rows = total_rows_per_granule;
            setFilterConstTrue();
            shrink(columns); /// shrink acts as filtering in such case
        }
        else
        {
            auto new_filter = ColumnUInt8::create(filter->size() - total_zero_rows_in_tails);
            IColumn::Filter & new_data = new_filter->getData();

            collapseZeroTails(filter->getData(), new_data);
            total_rows_per_granule = new_filter->size();
            num_rows = total_rows_per_granule;
            filter = new_filter.get();
            filter_holder = std::move(new_filter);
        }
        need_filter = true;
    }
    /// Another guess, if it's worth filtering at PREWHERE
    else if (countBytesInResultFilter(filter->getData()) < 0.6 * filter->size())
        need_filter = true;
}

size_t MergeTreeRangeReader::ReadResult::countZeroTails(const IColumn::Filter & filter_vec, NumRows & zero_tails, bool can_read_incomplete_granules) const
{
    zero_tails.resize(0);
    zero_tails.reserve(rows_per_granule.size());

    const auto * filter_data = filter_vec.data();

    size_t total_zero_rows_in_tails = 0;

    for (auto rows_to_read : rows_per_granule)
    {
        /// Count the number of zeros at the end of filter for rows were read from current granule.
        size_t zero_tail = numZerosInTail(filter_data, filter_data + rows_to_read);
        if (!can_read_incomplete_granules && zero_tail != rows_to_read)
            zero_tail = 0;
        zero_tails.push_back(zero_tail);
        total_zero_rows_in_tails += zero_tails.back();
        filter_data += rows_to_read;
    }

    return total_zero_rows_in_tails;
}

void MergeTreeRangeReader::ReadResult::collapseZeroTails(const IColumn::Filter & filter_vec, IColumn::Filter & new_filter_vec)
{
    const auto * filter_data = filter_vec.data();
    auto * new_filter_data = new_filter_vec.data();

    for (auto i : ext::range(0, rows_per_granule.size()))
    {
        memcpySmallAllowReadWriteOverflow15(new_filter_data, filter_data, rows_per_granule[i]);
        filter_data += rows_per_granule_original[i];
        new_filter_data += rows_per_granule[i];
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
        const auto * pos = end;
        UInt64 val =
                static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos)),
                        zero16)))
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 16)),
                        zero16))) << 16u)
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 32)),
                        zero16))) << 32u)
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 48)),
                        zero16))) << 48u);
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
    if (const_description.always_true)
        setFilterConstTrue();
    else if (const_description.always_false)
        clear();
    else
    {
        FilterDescription filter_description(*new_filter);
        filter_holder = filter_description.data_holder ? filter_description.data_holder : new_filter;
        filter = typeid_cast<const ColumnUInt8 *>(filter_holder.get());
        if (!filter)
            throw Exception("setFilter function expected ColumnUInt8.", ErrorCodes::LOGICAL_ERROR);
    }
}


size_t MergeTreeRangeReader::ReadResult::countBytesInResultFilter(const IColumn::Filter & filter_)
{
    auto it = filter_bytes_map.find(&filter_);
    if (it == filter_bytes_map.end())
    {
        auto bytes = countBytesInFilter(filter_);
        filter_bytes_map[&filter_] = bytes;
        return bytes;
    }
    else
        return it->second;
}

MergeTreeRangeReader::MergeTreeRangeReader(
    IMergeTreeReader * merge_tree_reader_,
    MergeTreeRangeReader * prev_reader_,
    const PrewhereInfoPtr & prewhere_,
    bool last_reader_in_chain_)
    : merge_tree_reader(merge_tree_reader_)
    , index_granularity(&(merge_tree_reader->data_part->index_granularity)), prev_reader(prev_reader_)
    , prewhere(prewhere_), last_reader_in_chain(last_reader_in_chain_), is_initialized(true)
{
    if (prev_reader)
        sample_block = prev_reader->getSampleBlock();

    for (const auto & name_and_type : merge_tree_reader->getColumns())
        sample_block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});

    if (prewhere)
    {
        if (prewhere->alias_actions)
            prewhere->alias_actions->execute(sample_block, true);

        if (prewhere->prewhere_actions)
            prewhere->prewhere_actions->execute(sample_block, true);

        if (prewhere->remove_prewhere_column)
            sample_block.erase(prewhere->prewhere_column_name);
    }
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


size_t MergeTreeRangeReader::Stream::ceilRowsToCompleteGranules(size_t rows_num) const
{
    /// FIXME suboptimal
    size_t result = 0;
    size_t from_mark = current_mark;
    while (result < rows_num && from_mark < last_mark)
        result += index_granularity->getMarkRows(from_mark++);

    return result;
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

        size_t num_read_rows;
        Columns columns = continueReadingChain(read_result, num_read_rows);

        /// Nothing to do. Return empty result.
        if (read_result.num_rows == 0)
            return read_result;

        bool has_columns = false;
        for (auto & column : columns)
        {
            if (column)
                has_columns = true;
        }

        size_t total_bytes = 0;
        for (auto & column : columns)
        {
            if (column)
                total_bytes += column->byteSize();
        }

        read_result.addNumBytesRead(total_bytes);

        bool should_evaluate_missing_defaults = false;

        if (has_columns)
        {
            /// num_read_rows >= read_result.num_rows
            /// We must filter block before adding columns to read_result.block

            /// Fill missing columns before filtering because some arrays from Nested may have empty data.
            merge_tree_reader->fillMissingColumns(columns, should_evaluate_missing_defaults, num_read_rows);

            if (read_result.getFilter())
                filterColumns(columns, read_result.getFilter()->getData());
        }
        else
        {
            size_t num_rows = read_result.num_rows;

            /// If block is empty, we still may need to add missing columns.
            /// In that case use number of rows in result block and don't filter block.
            if (num_rows)
                merge_tree_reader->fillMissingColumns(columns, should_evaluate_missing_defaults, num_rows);
        }

        if (!columns.empty() && should_evaluate_missing_defaults)
        {
            auto block = prev_reader->sample_block.cloneWithColumns(read_result.columns);
            auto block_before_prewhere = read_result.block_before_prewhere;
            for (auto & ctn : block)
            {
                if (block_before_prewhere.has(ctn.name))
                    block_before_prewhere.erase(ctn.name);
            }

            if (block_before_prewhere)
            {
                if (read_result.need_filter)
                {
                    auto old_columns = block_before_prewhere.getColumns();
                    filterColumns(old_columns, read_result.getFilterOriginal()->getData());
                    block_before_prewhere.setColumns(std::move(old_columns));
                }

                for (auto && ctn : block_before_prewhere)
                    block.insert(std::move(ctn));
            }

            merge_tree_reader->evaluateMissingDefaults(block, columns);
            merge_tree_reader->performRequiredConversions(columns);
        }

        read_result.columns.reserve(read_result.columns.size() + columns.size());
        for (auto & column : columns)
            read_result.columns.emplace_back(std::move(column));
    }
    else
    {
        read_result = startReadingChain(max_rows, ranges);
        read_result.num_rows = read_result.numReadRows();

        if (read_result.num_rows)
        {
            bool should_evaluate_missing_defaults;
            merge_tree_reader->fillMissingColumns(read_result.columns, should_evaluate_missing_defaults,
                                                  read_result.num_rows);

            if (should_evaluate_missing_defaults)
                merge_tree_reader->evaluateMissingDefaults({}, read_result.columns);

            merge_tree_reader->performRequiredConversions(read_result.columns);
        }
        else
            read_result.columns.clear();

        size_t total_bytes = 0;
        for (auto & column : read_result.columns)
            total_bytes += column->byteSize();

        read_result.addNumBytesRead(total_bytes);
    }

    if (read_result.num_rows == 0)
        return read_result;

    executePrewhereActionsAndFilterColumns(read_result);

    return read_result;
}


MergeTreeRangeReader::ReadResult MergeTreeRangeReader::startReadingChain(size_t max_rows, MarkRanges & ranges)
{
    ReadResult result;
    result.columns.resize(merge_tree_reader->getColumns().size());

    /// Stream is lazy. result.num_added_rows is the number of rows added to block which is not equal to
    /// result.num_rows_read until call to stream.finalize(). Also result.num_added_rows may be less than
    /// result.num_rows_read if the last granule in range also the last in part (so we have to adjust last granule).
    {
        size_t space_left = max_rows;
        while (space_left && (!stream.isFinished() || !ranges.empty()))
        {
            if (stream.isFinished())
            {
                result.addRows(stream.finalize(result.columns));
                stream = Stream(ranges.front().begin, ranges.front().end, merge_tree_reader);
                result.addRange(ranges.front());
                ranges.pop_front();
            }

            size_t current_space = space_left;

            /// If reader can't read part of granule, we have to increase number of reading rows
            ///  to read complete granules and exceed max_rows a bit.
            if (!merge_tree_reader->canReadIncompleteGranules())
                current_space = stream.ceilRowsToCompleteGranules(space_left);

            auto rows_to_read = std::min(current_space, stream.numPendingRowsInCurrentGranule());

            bool last = rows_to_read == space_left;
            result.addRows(stream.read(result.columns, rows_to_read, !last));
            result.addGranule(rows_to_read);
            space_left = (rows_to_read > space_left ? 0 : space_left - rows_to_read);
        }
    }

    result.addRows(stream.finalize(result.columns));

    /// Last granule may be incomplete.
    result.adjustLastGranule();

    return result;
}

Columns MergeTreeRangeReader::continueReadingChain(ReadResult & result, size_t & num_rows)
{
    Columns columns;
    num_rows = 0;

    if (result.rowsPerGranule().empty())
    {
        /// If zero rows were read on prev step, than there is no more rows to read.
        /// Last granule may have less rows than index_granularity, so finish reading manually.
        stream.finish();
        return columns;
    }

    columns.resize(merge_tree_reader->numColumnsInResult());

    const auto & rows_per_granule = result.rowsPerGranule();
    const auto & started_ranges = result.startedRanges();

    size_t next_range_to_start = 0;

    auto size = rows_per_granule.size();
    for (auto i : ext::range(0, size))
    {
        if (next_range_to_start < started_ranges.size()
            && i == started_ranges[next_range_to_start].num_granules_read_before_start)
        {
            num_rows += stream.finalize(columns);
            const auto & range = started_ranges[next_range_to_start].range;
            ++next_range_to_start;
            stream = Stream(range.begin, range.end, merge_tree_reader);
        }

        bool last = i + 1 == size;
        num_rows += stream.read(columns, rows_per_granule[i], !last);
    }

    stream.skip(result.numRowsToSkipInLastGranule());
    num_rows += stream.finalize(columns);

    /// added_rows may be zero if all columns were read in prewhere and it's ok.
    if (num_rows && num_rows != result.totalRowsPerGranule())
        throw Exception("RangeReader read " + toString(num_rows) + " rows, but "
                        + toString(result.totalRowsPerGranule()) + " expected.", ErrorCodes::LOGICAL_ERROR);

    return columns;
}

void MergeTreeRangeReader::executePrewhereActionsAndFilterColumns(ReadResult & result)
{
    if (!prewhere)
        return;

    const auto & header = merge_tree_reader->getColumns();
    size_t num_columns = header.size();

    if (result.columns.size() != num_columns)
        throw Exception("Invalid number of columns passed to MergeTreeRangeReader. "
                        "Expected " + toString(num_columns) + ", "
                        "got " + toString(result.columns.size()), ErrorCodes::LOGICAL_ERROR);

    ColumnPtr filter;
    size_t prewhere_column_pos;

    {
        /// Restore block from columns list.
        Block block;
        size_t pos = 0;

        if (prev_reader)
        {
            for (const auto & col : prev_reader->getSampleBlock())
            {
                block.insert({result.columns[pos], col.type, col.name});
                ++pos;
            }
        }

        for (auto name_and_type = header.begin(); pos < num_columns; ++pos, ++name_and_type)
            block.insert({result.columns[pos], name_and_type->type, name_and_type->name});

        if (prewhere->alias_actions)
            prewhere->alias_actions->execute(block);

        /// Columns might be projected out. We need to store them here so that default columns can be evaluated later.
        result.block_before_prewhere = block;
        prewhere->prewhere_actions->execute(block);

        prewhere_column_pos = block.getPositionByName(prewhere->prewhere_column_name);

        result.columns.clear();
        result.columns.reserve(block.columns());
        for (auto & col : block)
            result.columns.emplace_back(std::move(col.column));

        filter.swap(result.columns[prewhere_column_pos]);
    }

    if (result.getFilter())
    {
        /// TODO: implement for prewhere chain.
        /// In order to do it we need combine filter and result.filter, where filter filters only '1' in result.filter.
        throw Exception("MergeTreeRangeReader chain with several prewhere actions in not implemented.",
                        ErrorCodes::LOGICAL_ERROR);
    }

    result.setFilter(filter);

    /// If there is a WHERE, we filter in there, and only optimize IO and shrink columns here
    if (!last_reader_in_chain)
        result.optimize(merge_tree_reader->canReadIncompleteGranules());

    /// If we read nothing or filter gets optimized to nothing
    if (result.totalRowsPerGranule() == 0)
        result.setFilterConstFalse();
    /// If we need to filter in PREWHERE
    else if (prewhere->need_filter || result.need_filter)
    {
        /// If there is a filter and without optimized
        if (result.getFilter() && last_reader_in_chain)
        {
            const auto * result_filter = result.getFilter();
            /// optimize is not called, need to check const 1 and const 0
            size_t bytes_in_filter = result.countBytesInResultFilter(result_filter->getData());
            if (bytes_in_filter == 0)
                result.setFilterConstFalse();
            else if (bytes_in_filter == result.num_rows)
                result.setFilterConstTrue();
        }

        /// If there is still a filter, do the filtering now
        if (result.getFilter())
        {
            /// filter might be shrinked while columns not
            const auto * result_filter = result.getFilterOriginal();
            filterColumns(result.columns, result_filter->getData());
            result.need_filter = true;

            bool has_column = false;
            for (auto & column : result.columns)
            {
                if (column)
                {
                    has_column = true;
                    result.num_rows = column->size();
                    break;
                }
            }

            /// There is only one filter column. Record the actual number
            if (!has_column)
                result.num_rows = result.countBytesInResultFilter(result_filter->getData());
        }

        /// Check if the PREWHERE column is needed
        if (!result.columns.empty())
        {
            if (prewhere->remove_prewhere_column)
                result.columns.erase(result.columns.begin() + prewhere_column_pos);
            else
                result.columns[prewhere_column_pos] =
                        getSampleBlock().getByName(prewhere->prewhere_column_name).type->
                                createColumnConst(result.num_rows, 1u)->convertToFullColumnIfConst();
        }
    }
    /// Filter in WHERE instead
    else
    {
        result.columns[prewhere_column_pos] = result.getFilterHolder()->convertToFullColumnIfConst();
        if (getSampleBlock().getByName(prewhere->prewhere_column_name).type->isNullable())
            result.columns[prewhere_column_pos] = makeNullable(std::move(result.columns[prewhere_column_pos]));
        result.clearFilter(); // Acting as a flag to not filter in PREWHERE
    }
}

}
