#include <Storages/MergeTree/MergeTreeReaderCompactSingleBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Compression/CachedCompressedReadBuffer.h>

#include <algorithm>

namespace DB
{

size_t MergeTreeReaderCompactSingleBuffer::readRows(
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

std::optional<size_t> MergeTreeReaderCompactSingleBuffer::takeCompressedBytesRead()
{
    if (!count_compressed_bytes_read)
        return std::nullopt;

    return takeBytesOfCompressedBlockRanges(ranges_read);
}

void MergeTreeReaderCompactSingleBuffer::addGranulesServedFromColumnsCache(size_t pos, size_t from_mark, size_t to_mark)
{
    init();

    /// With marks of substreams, a subcolumn is read from the substreams it needs, and a column from all of them. A subcolumn
    /// whose streams are not all substreams of the part (e.g. the null map of a sparse column, which is read from its offsets),
    /// or that is a path of a dynamic column, is counted as the whole column, an upper bound.
    const size_t column_position = *column_positions[pos];
    const auto & column = columns_to_read[pos];
    std::vector<size_t> positions;
    if (!has_substream_marks)
    {
        positions.push_back(column_position);
    }
    else if (column.isSubcolumn() && !column.getTypeInStorage()->hasDynamicSubcolumns())
    {
        /// The streams as the reader reads them, with the prefixes in streams of their own, see `initSubcolumnsDeserializationOrder`.
        ISerialization::EnumerateStreamsSettings enumerate_settings;
        enumerate_settings.data_part_type = MergeTreeDataPartType::Compact;
        enumerate_settings.use_specialized_prefixes_and_suffixes_substreams = true;
        bool all_found = true;
        serializations[pos]->enumerateStreams(enumerate_settings, [&](const ISerialization::SubstreamPath & substream_path)
        {
            if (needSkipStream(pos, substream_path))
                return;
            if (auto position = columns_substreams.tryGetSubstreamPosition(column_position, column, substream_path, storage_settings))
                positions.push_back(*position);
            else
                all_found = false;
        }, ISerialization::SubstreamData(serializations[pos]).withType(column.type));
        if (!all_found)
            positions.clear();
    }

    if (positions.empty())
    {
        for (size_t position = columns_substreams.getFirstSubstreamPosition(column_position);
             position <= columns_substreams.getLastSubstreamPosition(column_position); ++position)
            positions.push_back(position);
    }

    for (size_t mark = from_mark; mark < to_mark; ++mark)
        for (const size_t position : positions)
            stream->addRangeOfPosition(mark, position, ranges_read);
}

void MergeTreeReaderCompactSingleBuffer::readGranule(
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

        stream->adjustRightMark(last_mark_to_read); /// Must go before seek.
        stream->seekToMarkAndColumn(mark, has_substream_marks ? columns_substreams.getFirstSubstreamPosition(*column_positions[pos]) : *column_positions[pos]);

        auto * cache_for_subcolumns = columns_for_offsets[pos] ? nullptr : &columns_cache_for_subcolumns;
        auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
        readPrefix(pos, mark, *stream, &deserialize_states_cache);
        readData(pos, *res_columns[pos], rows_to_read, mark, res_columns[pos]->size(), *stream, columns_cache, cache_for_subcolumns, nullptr);
    }

    /// If we have subcolumns and substreams marks, we read subcolumns separately, because we want to
    /// use deserialization prefixes cache and substreams cache during deserialization of subcolumns of the same column.
    if (has_substream_marks && has_subcolumns)
    {
        readSubcolumnsPrefixes(mark);
        initSubcolumnsDeserializationOrder();
        /// Deserialize all subcolumns according to subcolumns_deserialization_order.
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

                readData(pos, *res_columns[pos], rows_to_read, mark, subcolumns_size_before_reading, *stream, columns_cache, &columns_cache_for_subcolumns, &substreams_cache);
            }
        }
    }
}

void MergeTreeReaderCompactSingleBuffer::init()
try
{
    if (initialized)
        return;

    stream = std::make_unique<MergeTreeReaderStreamAllOfMultipleColumns>(
        data_part_info_for_read->getDataPartStorage(), MergeTreeDataPartCompact::DATA_FILE_NAME,
        MergeTreeDataPartCompact::DATA_FILE_EXTENSION, data_part_info_for_read->getMarksCount(),
        all_mark_ranges, settings, uncompressed_cache,
        data_part_info_for_read->getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION),
        marks_loader, profile_callback, clock_type);
    stream->updateReadRequestMap(read_request_map);
    if (count_compressed_bytes_read)
        stream->setBlocksReadSink(&ranges_read);

    initialized = true;
}
catch (...)
{
    if (!isRetryableException(std::current_exception()))
        data_part_info_for_read->reportBroken();
    throw;
}

}
