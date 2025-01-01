#include <Storages/MergeTree/MergeTreeReaderCompactSingleBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

size_t MergeTreeReaderCompactSingleBuffer::readRows(
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
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

        /// Use cache to avoid reading the column with the same name twice.
        /// It may happen if there are empty array Nested in the part.
        ISerialization::SubstreamsCache cache;
        /// If we need to read multiple subcolumns from a single column in storage,
        /// we will read it this column only once and then reuse to extract all subcolumns.
        /// We cannot use SubstreamsCache for it, because we may also read the full column itself
        /// and it might me not empty inside res_columns (and SubstreamsCache contains the whole columns).
        /// TODO: refactor the code in a way when we first read all full columns and then extract all subcolumns from them.
        std::unordered_map<String, ColumnPtr> columns_cache_for_subcolumns;

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (!res_columns[pos])
                continue;

            auto & column = res_columns[pos];

            stream->adjustRightMark(current_task_last_mark); /// Must go before seek.
            stream->seekToMarkAndColumn(from_mark, *column_positions[pos]);

            auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
            {
                if (needSkipStream(pos, substream_path))
                    return nullptr;

                return stream->getDataBuffer();
            };

            /// If we read only offsets we have to read prefix anyway
            /// to preserve correctness of serialization.
            auto buffer_getter_for_prefix = [&](const auto &) -> ReadBuffer *
            {
                return stream->getDataBuffer();
            };

            readPrefix(columns_to_read[pos], buffer_getter, buffer_getter_for_prefix, columns_for_offsets[pos]);
            readData(columns_to_read[pos], column, rows_to_read, buffer_getter, cache, columns_cache_for_subcolumns, columns_for_offsets[pos]);
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
