#include <Storages/MergeTree/MergeTreeReaderCompactMultipleBuffers.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

size_t MergeTreeReaderCompactMultipleBuffers::readRows(
    size_t from_mark,
    size_t current_range_last_mark,
    bool continue_reading, size_t max_rows_to_read,
    MutableColumns & res_columns, Columns & shared_columns)
try
{
    init();

    if (continue_reading)
        from_mark = next_mark;

    return readGranules(from_mark, current_range_last_mark, continue_reading, max_rows_to_read, res_columns, shared_columns);
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

void MergeTreeReaderCompactMultipleBuffers::readGranule(
    size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask)
{
    const size_t num_columns = columns_to_read.size();
    auto is_selected = [&](size_t pos) { return res_columns[pos] && (!columns_mask || (*columns_mask)[pos]); };

    deserialize_binary_bulk_state_map.clear();
    deserialize_binary_bulk_state_map_for_subcolumns.clear();

    /// Use cache to avoid reading the column with the same name twice.
    /// It may happen if there are empty array Nested in the part.
    std::unordered_map<String, ColumnPtr> columns_cache;
    std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;

    /// If we don't have substream marks and we need to read multiple subcolumns from a single column in storage,
    /// we will read the whole column only once and then reuse to extract all subcolumns.
    /// We cannot use SubstreamsCache for it, because we may also read the full column itself
    /// and it might be not empty inside res_columns (and SubstreamsCache contains the whole columns).
    std::unordered_map<String, ColumnPtr> columns_cache_for_subcolumns;

    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        if (!is_selected(pos))
            continue;

        /// If we have substream marks, subcolumns will be read separately.
        if (columns_to_read[pos].isSubcolumn() && has_substream_marks)
            continue;

        auto & stream = *streams[pos];
        stream.adjustRightMark(last_mark_to_read); /// Must go before seek.
        stream.seekToMarkAndColumn(mark, has_substream_marks ? columns_substreams.getFirstSubstreamPosition(*column_positions[pos]) : *column_positions[pos]);

        auto * cache_for_subcolumns = columns_for_offsets[pos] ? nullptr : &columns_cache_for_subcolumns;
        auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
        readPrefix(pos, mark, stream, &deserialize_states_cache);
        readData(pos, *res_columns[pos], rows_to_read, mark, res_columns[pos]->size(), stream, columns_cache, cache_for_subcolumns, nullptr);
    }

    /// If we have subcolumns and substreams marks, we read subcolumns separately, because we want to
    /// use deserialization prefixes cache and substreams cache during deserialization of subcolumns of the same column.
    if (has_substream_marks && has_subcolumns)
    {
        readSubcolumnsPrefixes(mark);
        initSubcolumnsDeserializationOrder();
        for (const auto & [column, subcolumns_order] : subcolumns_deserialization_order)
        {
            /// The selected subcolumns of the column hold the same number of rows: the granules
            /// before this one were read or served for all of them.
            auto first_selected = std::find_if(subcolumns_order.begin(), subcolumns_order.end(), is_selected);
            if (first_selected == subcolumns_order.end())
                continue;

            ISerialization::SubstreamsCache substreams_cache;
            size_t subcolumns_size_before_reading = res_columns[*first_selected]->size();
            for (size_t pos : subcolumns_order)
            {
                if (!is_selected(pos))
                    continue;

                readData(pos, *res_columns[pos], rows_to_read, mark, subcolumns_size_before_reading, *streams[pos], columns_cache, &columns_cache_for_subcolumns, &substreams_cache);
            }
        }
    }
}

void MergeTreeReaderCompactMultipleBuffers::prefetchBeginOfRange(Priority priority)
{
    bool do_prefetch = data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_settings.prefetch
        : settings.read_settings.local_fs_settings.prefetch;

    if (!do_prefetch || all_mark_ranges.getNumberOfMarks() == 0)
        return;

    /// `readRows` serves a range that is in the columns cache without reading the part, so a
    /// prefetch would spend the IO, and on remote storage the requests, that the cache avoids.
    /// The entries can be evicted before the read: then the range is read without a prefetch.
    if (canServeFirstRangeFromCache())
        return;

    try
    {
        init();

        size_t from_mark = all_mark_ranges.front().begin;
        auto prefetch = [&](Stream & stream)
        {
            stream.stream->adjustRightMark(last_mark_to_read);
            stream.stream->seekToMarkAndColumn(from_mark, stream.first_position);
            stream.stream->getDataBuffer()->prefetch(priority);
        };

        if (shared_stream)
            prefetch(*shared_stream);
        else
            for (auto & [_, stream] : streams_by_position)
                prefetch(stream);
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()) && !isQueryCancellation(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderCompactMultipleBuffers::updateReadRequestMap(MarkRangesPtr request_map)
{
    MergeTreeReaderCompact::updateReadRequestMap(std::move(request_map));
    if (initialized)
        updateStreamsReadRequestMap();
}

void MergeTreeReaderCompactMultipleBuffers::updateStreamsReadRequestMap()
{
    /// Every stream converts the marks to the byte ranges of its own column.
    if (shared_stream)
        shared_stream->stream->updateReadRequestMap(read_request_map);
    for (auto & [_, stream] : streams_by_position)
        stream.stream->updateReadRequestMap(read_request_map);
}

MergeTreeReaderStream & MergeTreeReaderCompactMultipleBuffers::getStream(const NameAndTypePair & column)
{
    init();

    auto it = streams_by_name.find(column.getNameInStorage());
    if (it == streams_by_name.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no stream for column {} in part {}", column.name, data_part_info_for_read->getDataPartStorage()->getFullPath());

    return *it->second;
}

bool MergeTreeReaderCompactMultipleBuffers::shouldUseSharedStream()
{
    /// A separate buffer for every column pays off when the columns are read over enough granules: the buffers of the
    /// columns then read fewer bytes than the single buffer and can be prefetched in parallel, while for a few granules
    /// the single buffer makes fewer read requests than one request per column.
    /// The trade-off depends on the granules this reader will actually touch, not on the size of the whole part:
    /// a point read of a few granules from a large part is cheaper with the single buffer.
    /// The buffers are created per physical column of the part, so subcolumns of one column (e.g. paths of `JSON`) share a
    /// buffer, and columns missing in the part have none: count the distinct columns of the part to read.
    /// The reader is reused for the later read tasks of the same part (see `updateAllMarkRanges`), so the choice is made
    /// for the current mark ranges and is made again when they change.
    std::unordered_set<size_t> physical_columns_to_read;
    for (const auto & position : column_positions)
        if (position)
            physical_columns_to_read.insert(*position);

    size_t num_granules = all_mark_ranges.getNumberOfMarks();
    if (num_granules < settings.compact_parts_min_granules_to_multibuffer_read && num_granules < physical_columns_to_read.size())
        return true;

    /// A part is written in stripes if, for some granule, the next granule of the first column precedes this granule
    /// of the second column: both granules are in the same stripe of more than one granule. Otherwise all columns of
    /// every granule are adjacent, and separate buffers for the columns would read the whole file each, so one buffer
    /// is used for all columns.
    /// The stripes of one part can have different numbers of granules: e.g. a granule bigger than
    /// `compact_parts_max_bytes_to_buffer` is written as a separate stripe. So the granules to read are checked, not
    /// only the first one. Only these granules are checked, not the whole part: separate buffers are useful only if
    /// the granules to read are in stripes, and the cost of the check stays proportional to the amount of data to
    /// read, e.g. a point query does not walk all marks of a big part.
    /// The granules of a column are compared by the mark of its first substream: with substream marks, the marks
    /// of all substreams of one column of one granule are adjacent in both layouts.
    /// Every granule to read is compared with the next one, and the first granule of a range also with the previous
    /// one: otherwise a range of only the last granule of a stripe would not be recognized as striped.
    size_t marks_count = data_part_info_for_read->getMarksCount();
    size_t num_columns = data_part_info_for_read->getColumns().size();
    if (marks_count < 2 || num_columns < 2)
        return true;

    size_t second_column_position = has_substream_marks ? columns_substreams.getFirstSubstreamPosition(1) : 1;
    if (!marks_getter)
        marks_getter = marks_loader->loadMarks();
    for (const auto & range : all_mark_ranges)
        for (size_t mark = range.begin > 0 ? range.begin - 1 : 0; mark < range.end && mark + 1 < marks_count; ++mark)
            if (marks_getter->getMark(mark + 1, 0) < marks_getter->getMark(mark, second_column_position))
                return false;

    return true;
}

void MergeTreeReaderCompactMultipleBuffers::updateAllMarkRanges(const MarkRanges & ranges)
try
{
    MergeTreeReaderCompact::updateAllMarkRanges(ranges);

    /// The streams were created for the mark ranges of a previous task. If the choice between one buffer for all
    /// columns and a buffer per column is different for the new ranges, the streams are created again on the next read.
    /// A reader without columns of the part has no streams, and the choice does not matter.
    if (!initialized || (!shared_stream && streams_by_position.empty()))
        return;

    if (shared_stream.has_value() == shouldUseSharedStream())
        return;

    initialized = false;
    streams.clear();
    streams_by_name.clear();
    streams_by_position.clear();
    shared_stream.reset();
}
catch (...)
{
    if (!isRetryableException(std::current_exception()) && !isQueryCancellation(std::current_exception()))
        data_part_info_for_read->reportBroken();
    throw;
}

void MergeTreeReaderCompactMultipleBuffers::init()
try
{
    if (initialized)
        return;

    size_t marks_count = data_part_info_for_read->getMarksCount();

    auto create_stream = [&]<typename StreamType>(auto &&... args) -> std::unique_ptr<MergeTreeReaderStream>
    {
        return std::make_unique<StreamType>(
            std::forward<decltype(args)>(args)...,
            data_part_info_for_read->getDataPartStorage(), MergeTreeDataPartCompact::DATA_FILE_NAME,
            MergeTreeDataPartCompact::DATA_FILE_EXTENSION, marks_count,
            all_mark_ranges, settings, uncompressed_cache,
            data_part_info_for_read->getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION),
            marks_loader, profile_callback, clock_type);
    };

    /// A reader can have no columns of the part to read, e.g. the main reader of a task when `PREWHERE` has read all
    /// the columns, or a reader of columns missing in the part. It must not create a stream, which would be prefetched.
    bool has_columns_in_part = std::ranges::any_of(column_positions, [](const auto & position) { return position.has_value(); });
    bool use_shared_stream = has_columns_in_part && shouldUseSharedStream();

    streams.assign(columns_to_read.size(), nullptr);

    if (use_shared_stream)
    {
        shared_stream.emplace(Stream{.stream = create_stream.operator()<MergeTreeReaderStreamAllOfMultipleColumns>(), .first_position = 0});
        for (size_t i = 0; i < columns_to_read.size(); ++i)
            if (column_positions[i])
                streams[i] = shared_stream->stream.get();
    }
    else
    {
        for (size_t i = 0; i < columns_to_read.size(); ++i)
        {
            if (!column_positions[i])
                continue;

            size_t column_position = *column_positions[i];
            auto it = streams_by_position.find(column_position);
            if (it == streams_by_position.end())
            {
                /// The right bound of the stream is computed from the marks of the last substream of the column,
                /// so that the data of all substreams of the column is inside the bound.
                size_t first_position = has_substream_marks ? columns_substreams.getFirstSubstreamPosition(column_position) : column_position;
                size_t last_position = has_substream_marks ? columns_substreams.getLastSubstreamPosition(column_position) : column_position;

                it = streams_by_position.emplace(
                    column_position,
                    Stream{.stream = create_stream.operator()<MergeTreeReaderStreamOneOfMultipleColumns>(first_position, last_position), .first_position = first_position}).first;
            }

            streams[i] = it->second.stream.get();
        }
    }

    for (size_t i = 0; i < columns_to_read.size(); ++i)
        if (streams[i])
            streams_by_name[columns_to_read[i].getNameInStorage()] = streams[i];

    initialized = true;
    updateStreamsReadRequestMap();
}
catch (...)
{
    if (!isRetryableException(std::current_exception()) && !isQueryCancellation(std::current_exception()))
        data_part_info_for_read->reportBroken();
    throw;
}

}
