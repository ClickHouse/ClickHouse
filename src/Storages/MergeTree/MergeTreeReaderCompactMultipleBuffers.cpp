#include <Storages/MergeTree/MergeTreeReaderCompactMultipleBuffers.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

size_t MergeTreeReaderCompactMultipleBuffers::readRows(
    size_t from_mark,
    bool continue_reading, size_t max_rows_to_read,
    MutableColumns & res_columns)
try
{
    init();

    if (continue_reading)
        from_mark = next_mark;

    size_t read_rows = 0;
    size_t num_columns = columns_to_read.size();

    checkNumberOfColumns(num_columns);
    createColumnsForReading(res_columns);

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark);

        deserialize_binary_bulk_state_map.clear();
        deserialize_binary_bulk_state_map_for_subcolumns.clear();

        /// See the comments in `MergeTreeReaderCompactSingleBuffer::readRows`.
        std::unordered_map<String, ColumnPtr> columns_cache;
        std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;
        std::unordered_map<String, ColumnPtr> columns_cache_for_subcolumns;

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (!res_columns[pos])
                continue;

            /// If we have substream marks, subcolumns will be read separately.
            if (columns_to_read[pos].isSubcolumn() && has_substream_marks)
                continue;

            auto & stream = *streams[pos];
            stream.adjustRightMark(last_mark_to_read); /// Must go before seek.
            stream.seekToMarkAndColumn(from_mark, has_substream_marks ? columns_substreams.getFirstSubstreamPosition(*column_positions[pos]) : *column_positions[pos]);

            auto * cache_for_subcolumns = columns_for_offsets[pos] ? nullptr : &columns_cache_for_subcolumns;
            auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
            readPrefix(pos, from_mark, stream, &deserialize_states_cache);
            readData(pos, *res_columns[pos], rows_to_read, from_mark, res_columns[pos]->size(), stream, columns_cache, cache_for_subcolumns, nullptr);
        }

        if (has_substream_marks && has_subcolumns)
        {
            readSubcolumnsPrefixes(from_mark);
            initSubcolumnsDeserializationOrder();
            for (const auto & [column, subcolumns_order] : subcolumns_deserialization_order)
            {
                ISerialization::SubstreamsCache substreams_cache;
                size_t subcolumns_size_before_reading = res_columns[subcolumns_order[0]]->size();
                for (size_t pos : subcolumns_order)
                {
                    if (!res_columns[pos])
                        continue;

                    readData(pos, *res_columns[pos], rows_to_read, from_mark, subcolumns_size_before_reading, *streams[pos], columns_cache, &columns_cache_for_subcolumns, &substreams_cache);
                }
            }
        }

        ++from_mark;
        read_rows += rows_to_read;
    }

    next_mark = from_mark;
    return read_rows;
}
catch (...)
{
    if (!isRetryableException(std::current_exception()))
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

void MergeTreeReaderCompactMultipleBuffers::prefetchBeginOfRange(Priority priority)
{
    bool do_prefetch = data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_settings.prefetch
        : settings.read_settings.local_fs_settings.prefetch;

    if (!do_prefetch || all_mark_ranges.getNumberOfMarks() == 0)
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
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}

MergeTreeReaderStream & MergeTreeReaderCompactMultipleBuffers::getStream(const NameAndTypePair & column)
{
    init();

    auto it = streams_by_name.find(column.getNameInStorage());
    if (it == streams_by_name.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no stream for column {} in part {}", column.name, data_part_info_for_read->getDataPartStorage()->getFullPath());

    return *it->second;
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

    /// A part is written in stripes if the second granule of the first column precedes the first granule of the
    /// second column. Otherwise all columns of a granule are adjacent, and separate buffers for the columns would
    /// read the whole file each, so one buffer is used for all columns.
    /// The granules of a column are compared by the mark of its first substream: with substream marks, the marks
    /// of all substreams of one column of one granule are adjacent in both layouts.
    size_t num_columns = data_part_info_for_read->getColumns().size();
    bool is_striped = false;
    if (marks_count >= 2 && num_columns >= 2)
    {
        size_t second_column_position = has_substream_marks ? columns_substreams.getFirstSubstreamPosition(1) : 1;
        marks_getter = marks_loader->loadMarks();
        is_striped = marks_getter->getMark(1, 0) < marks_getter->getMark(0, second_column_position);
    }

    streams.assign(columns_to_read.size(), nullptr);

    if (!is_striped)
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
                    Stream{.stream = create_stream.operator()<MergeTreeReaderStreamOneOfMultipleColumns>(last_position), .first_position = first_position}).first;
            }

            streams[i] = it->second.stream.get();
        }
    }

    for (size_t i = 0; i < columns_to_read.size(); ++i)
        if (streams[i])
            streams_by_name[columns_to_read[i].getNameInStorage()] = streams[i];

    initialized = true;
}
catch (...)
{
    if (!isRetryableException(std::current_exception()))
        data_part_info_for_read->reportBroken();
    throw;
}

}
