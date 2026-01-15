#include <Storages/MergeTree/MergeTreeReaderCompactSingleBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Compression/CachedCompressedReadBuffer.h>

namespace DB
{

size_t MergeTreeReaderCompactSingleBuffer::readRows(
    size_t from_mark, size_t current_task_last_mark,
    bool continue_reading, size_t max_rows_to_read,
    size_t rows_offset, Columns & res_columns)
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

        if (rows_to_read <= rows_offset)
        {
            rows_offset -= rows_to_read;
            ++from_mark;
            continue;
        }
        rows_to_read -= rows_offset;

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
            if (!res_columns[pos])
            {
                continue;
            }

            /// If we have substream marks, subcolumns will be read separately.
            if (columns_to_read[pos].isSubcolumn() && has_substream_marks)
                continue;

            stream->adjustRightMark(current_task_last_mark); /// Must go before seek.
            stream->seekToMarkAndColumn(from_mark, has_substream_marks ? columns_substreams.getFirstSubstreamPosition(*column_positions[pos]) : *column_positions[pos]);

            auto * cache_for_subcolumns = columns_for_offsets[pos] ? nullptr : &columns_cache_for_subcolumns;
            auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
            readPrefix(pos, from_mark, *stream, &deserialize_states_cache);
            readData(pos, res_columns[pos], rows_to_read, rows_offset, from_mark, res_columns[pos]->size(), *stream, columns_cache, cache_for_subcolumns, nullptr);
        }

        /// If we have subcolumns and substreams marks, we read subcolumns separately, because we want to
        /// use deserialization prefixes cache and substreams cache during deserialization of subcolumns of the same column.
        if (has_substream_marks && has_subcolumns)
        {
            readSubcolumnsPrefixes(from_mark, current_task_last_mark);
            initSubcolumnsDeserializationOrder();
            /// Deserialize all subcolumns according to subcolumns_deserialization_order.
            for (const auto & [column, subcolumns_order] : subcolumns_deserialization_order)
            {
                ISerialization::SubstreamsCache substreams_cache;
                size_t subcolumns_size_before_reading = res_columns[subcolumns_order[0]]->size();
                for (size_t pos : subcolumns_order)
                {
                    if (!res_columns[pos])
                        continue;

                    readData(pos, res_columns[pos], rows_to_read, rows_offset, from_mark, subcolumns_size_before_reading, *stream, columns_cache, &columns_cache_for_subcolumns, &substreams_cache);
                }
            }
        }

        ++from_mark;
        read_rows += rows_to_read;
        rows_offset = 0;
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
        e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read, rows_offset));
    }

    throw;
}

void MergeTreeReaderCompactSingleBuffer::init()
try
{
    if (initialized)
        return;

    auto stream_settings = settings;
    stream_settings.allow_different_codecs = true;

    stream = std::make_unique<MergeTreeReaderStreamAllOfMultipleColumns>(
        data_part_info_for_read->getDataPartStorage(), MergeTreeDataPartCompact::DATA_FILE_NAME,
        MergeTreeDataPartCompact::DATA_FILE_EXTENSION, data_part_info_for_read->getMarksCount(),
        all_mark_ranges, stream_settings,uncompressed_cache,
        data_part_info_for_read->getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION),
        marks_loader, profile_callback, clock_type);

    initialized = true;
}
catch (...)
{
    if (!isRetryableException(std::current_exception()))
        data_part_info_for_read->reportBroken();
    throw;
}

}
