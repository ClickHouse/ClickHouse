#include <Storages/MergeTree/MergeTreeReaderCompactMultiBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

void MergeTreeReaderCompactMultiBuffer::addStreams()
{
    if (!settings.read_settings.local_fs_settings.buffer_size || !settings.read_settings.remote_fs_settings.buffer_size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read to empty buffer.");

    try
    {
        for (size_t i = 0; i < columns_to_read.size(); ++i)
        {
            if (column_positions[i])
            {
                if (has_substream_marks)
                {
                    auto create_stream = [&]()
                    {
                        return std::make_unique<MergeTreeReaderStreamRangeOfMultipleColumns>(
                            columns_substreams.getFirstSubstreamPosition(*column_positions[i]),
                            columns_substreams.getLastSubstreamPosition(*column_positions[i]),
                            data_part_info_for_read->getDataPartStorage(),
                            MergeTreeDataPartCompact::DATA_FILE_NAME, MergeTreeDataPartCompact::DATA_FILE_EXTENSION,
                            data_part_info_for_read->getMarksCount(), all_mark_ranges, settings,
                            uncompressed_cache, data_part_info_for_read->getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION),
                            marks_loader, profile_callback, clock_type);
                    };

                    /// Create a single stream for all subcolumns of a single column. Reading from separate streams makes sense for separate columns
                    /// as their data from several granules are grouped together inside a single stripe, but subcolumns are not grouped like that
                    /// and reading subcolumns of the same column from separate streams makes it slower.
                    if (columns_to_read[i].isSubcolumn())
                    {
                        if (!subcolumns_streams.contains(columns_to_read[i].getNameInStorage()))
                            subcolumns_streams.emplace(columns_to_read[i].getNameInStorage(), create_stream());
                    }
                    else
                    {
                        streams.emplace(columns_to_read[i].name, create_stream());
                    }
                }
                else
                {
                    streams.emplace(columns_to_read[i].name, std::make_unique<MergeTreeReaderStreamOneOfMultipleColumns>(
                                                                 *column_positions[i], data_part_info_for_read->getDataPartStorage(),
                                                                 MergeTreeDataPartCompact::DATA_FILE_NAME, MergeTreeDataPartCompact::DATA_FILE_EXTENSION,
                                                                 data_part_info_for_read->getMarksCount(), all_mark_ranges, settings,
                                                                 uncompressed_cache, data_part_info_for_read->getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION),
                                                                 marks_loader, profile_callback, clock_type));
                }
            }
        }
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()) && !isQueryCancellation(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}

MergeTreeReaderStream & MergeTreeReaderCompactMultiBuffer::getStream(const DB::NameAndTypePair & column)
{
    if (!has_substream_marks)
        return *streams.at(column.name);

    return column.isSubcolumn() ? *subcolumns_streams.at(column.getNameInStorage()) : *streams.at(column.name);
}

void MergeTreeReaderCompactMultiBuffer::seekToMarkAndDeserializePrefix(size_t from_mark, bool seek_to_mark, const std::vector<bool> * columns_mask)
{
    auto is_selected = [&](size_t pos) { return column_positions[pos] && (!columns_mask || (*columns_mask)[pos]); };

    deserialize_binary_bulk_state_map.clear();
    deserialize_binary_bulk_state_map_for_subcolumns.clear();

    /// Bound the ranged read requests by the end of the current stripe (`last_right_mark`),
    /// not by the task-wide `last_mark_to_read`: each column is read from its own buffer
    /// and its data beyond the current stripe is interleaved with the other columns' data.
    /// Without a selected subcolumn the prefixes are not read, not to read the streams of the columns
    /// served from the columns cache.
    bool any_subcolumn_selected = !columns_mask;
    for (size_t i = 0; !any_subcolumn_selected && i < columns_to_read.size(); ++i)
        any_subcolumn_selected = is_selected(i) && columns_to_read[i].isSubcolumn();
    if (any_subcolumn_selected)
        readSubcolumnsPrefixes(from_mark, *last_right_mark);

    std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (!is_selected(i))
            continue;

        auto & stream = getStream(columns_to_read[i]);
        if (seek_to_mark || columns_for_offsets[i])
        {
            stream.adjustRightMark(*last_right_mark);
            /// If it's a subcolumn and we have substream marks, we will seek to the specific substream mark during deserialization later.
            if (!has_substream_marks || !columns_to_read[i].isSubcolumn())
                stream.seekToMark(from_mark);
        }

        readPrefix(i, from_mark, stream, &deserialize_states_caches[columns_to_read[i].getNameInStorage()]);
    }

    /// The order is determined from the prefixes of the subcolumns, so not before they are read.
    if (any_subcolumn_selected)
        initSubcolumnsDeserializationOrder();
}

size_t MergeTreeReaderCompactMultiBuffer::readRows(
    size_t from_mark,
    size_t current_range_last_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    MutableColumns & res_columns, Columns & shared_columns)
try
{
    if (continue_reading)
        from_mark = next_mark;

    /// The stripe is found again when the ranges to read end before the current stripe,
    /// as the prefetched data is not valid anymore, and when the read does not continue
    /// from the previous call, as the position is then set anew by `from_mark`.
    if ((last_right_mark && last_mark_to_read < *last_right_mark) || !continue_reading)
        last_right_mark.reset();

    /// With the columns cache the granules are read one by one by `readGranule`, for the columns not served from the cache.
    if (columns_cache_reads_possible || columns_cache_writes_possible)
        return readGranules(from_mark, current_range_last_mark, continue_reading, max_rows_to_read, res_columns, shared_columns);

    size_t read_rows = 0;
    size_t num_columns = columns_to_read.size();

    checkNumberOfColumns(num_columns);
    createColumnsForReading(res_columns);

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark);
        readGranuleImpl(from_mark, rows_to_read, res_columns, nullptr, /*seek_to_mark=*/ !continue_reading);
        ++from_mark;
        read_rows += rows_to_read;
    }

    next_mark = from_mark;
    return read_rows;
}
catch (...)
{
    if (!isRetryableException(std::current_exception()) && !isQueryCancellation(std::current_exception()))
        data_part_info_for_read->reportBroken();

    /// Better diagnostics.
    try
    {
        rethrow_exception(std::current_exception());
    }
    catch (Exception & e)
    {
        e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read));
    }

    throw;
}

void MergeTreeReaderCompactMultiBuffer::readGranule(
    size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask)
{
    readGranuleImpl(mark, rows_to_read, res_columns, columns_mask, /*seek_to_mark=*/ true);
}

void MergeTreeReaderCompactMultiBuffer::readGranuleImpl(
    size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask, bool seek_to_mark)
{
    const size_t num_columns = columns_to_read.size();
    auto is_selected = [&](size_t pos) { return res_columns[pos] && (!columns_mask || (*columns_mask)[pos]); };

    bool is_start_of_new_stripe = !last_right_mark || *last_right_mark <= mark;
    if (is_start_of_new_stripe)
        adjustLastRightMark(mark);

    seekToMarkAndDeserializePrefix(mark, seek_to_mark || is_start_of_new_stripe, columns_mask);

    if (use_prefetch && (!settings.filesystem_prefetches_limit || num_columns <= settings.filesystem_prefetches_limit))
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (column_positions[i] && (!columns_mask || (*columns_mask)[i]))
                getStream(columns_to_read[i]).getDataBuffer()->prefetch(DEFAULT_PREFETCH_PRIORITY);
        }
    }

    /// Use cache to avoid reading the column with the same name twice.
    /// It may happen if there are empty array Nested in the part.
    std::unordered_map<String, ColumnPtr> columns_cache;
    /// If we don't have substream marks and we need to read multiple subcolumns from a single column in storage,
    /// we will read the whole column only once and then reuse to extract all subcolumns.
    /// We cannot use SubstreamsCache for it, because we may also read the full column itself
    /// and it might be not empty inside res_columns (and SubstreamsCache contains the whole columns).
    std::unordered_map<String, ColumnPtr> columns_cache_for_subcolumns;

    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        if (!is_selected(pos))
            continue;

        /// If we have substream marks, subcolumns are deserialized later according to the subcolumns_deserialization_order.
        if (has_substream_marks && columns_to_read[pos].isSubcolumn())
            continue;

        auto & stream = getStream(columns_to_read[pos]);
        auto * cache_for_subcolumns = columns_for_offsets[pos] ? nullptr : &columns_cache_for_subcolumns;
        readData(pos, *res_columns[pos], rows_to_read, mark, res_columns[pos]->size(), stream, columns_cache, cache_for_subcolumns, nullptr);
    }

    if (has_substream_marks)
    {
        /// Deserialize all subcolumns according to subcolumns_deserialization_order.
        for (const auto & [storage_column_name, subcolumns_order] : subcolumns_deserialization_order)
        {
            /// The selected subcolumns of a column hold the same number of rows, as each earlier granule
            /// was read or served for all of them.
            auto first_selected = std::find_if(subcolumns_order.begin(), subcolumns_order.end(), is_selected);
            if (first_selected == subcolumns_order.end())
                continue;

            ISerialization::SubstreamsCache substreams_cache;
            size_t subcolumns_size_before_reading = res_columns[*first_selected]->size();
            for (size_t pos : subcolumns_order)
            {
                if (!is_selected(pos))
                    continue;

                auto & stream = getStream(columns_to_read[pos]);
                readData(pos, *res_columns[pos], rows_to_read, mark, subcolumns_size_before_reading, stream, columns_cache, nullptr, &substreams_cache);
            }
        }
    }
}

void MergeTreeReaderCompactMultiBuffer::prefetchBeginOfRange(Priority priority)
try
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeReaderCompactMultiBuffer::prefetchBeginOfRange");
    if (!use_prefetch || all_mark_ranges.getNumberOfMarks() == 0)
        return;

    adjustLastRightMark(all_mark_ranges.front().begin);
    std::unordered_set<MergeTreeReaderStream *> prefetched_streams;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (!column_positions[i])
            continue;

        auto & stream = getStream(columns_to_read[i]);
        if (prefetched_streams.contains(&stream))
            continue;

        stream.adjustRightMark(*last_right_mark);
        stream.seekToMark(all_mark_ranges.front().begin);
        stream.getDataBuffer()->prefetch(priority);
        prefetched_streams.insert(&stream);
    }
}
catch (...)
{
    if (!isRetryableException(std::current_exception()) && !isQueryCancellation(std::current_exception()))
        data_part_info_for_read->reportBroken();
    throw;
}

void MergeTreeReaderCompactMultiBuffer::updateReadRequestMap(MarkRangesPtr request_map)
{
    MergeTreeReaderCompact::updateReadRequestMap(request_map);
    for (auto * file_streams : {&streams, &subcolumns_streams})
        for (auto & [_, stream] : *file_streams)
            stream->updateReadRequestMap(request_map);
}

void MergeTreeReaderCompactMultiBuffer::adjustLastRightMark(size_t current_mark)
{
    size_t num_columns = data_part_info_for_read->getColumns().size();
    if (num_columns == 1)
    {
        last_right_mark = last_mark_to_read;
        return;
    }

    if (!marks_getter)
        marks_getter = marks_loader->loadMarks();

    /// The stripe ends at the first granule whose first column is written after the second column of `current_mark`.
    const auto & next_mark_in_file = marks_getter->getMark(current_mark, has_substream_marks ? columns_substreams.getFirstSubstreamPosition(1) : 1);
    for (; current_mark < last_mark_to_read; ++current_mark)
    {
        const auto & current_mark_in_file = marks_getter->getMark(current_mark, 0);
        if (current_mark_in_file > next_mark_in_file)
            break;
    }

    last_right_mark = current_mark;
}

}
