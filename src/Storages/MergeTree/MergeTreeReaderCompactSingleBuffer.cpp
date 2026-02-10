#include <Storages/MergeTree/MergeTreeReaderCompactSingleBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <Compression/CachedCompressedReadBuffer.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_hybrid_storage;
    extern const MergeTreeSettingsUInt64 hybrid_storage_max_row_size;
}

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

    /// Check if we should use hybrid row-based reading
    /// Note: For compact parts, hybrid reading requires special handling
    /// since all columns share a single data file. For now, we support
    /// hybrid reading by finding the __row column position and reading it.
    if (use_hybrid_row_reading)
    {
        /// Find the position of __row column
        auto row_column_pos = data_part_info_for_read->getColumnPosition(RowDataColumn::name);
        if (row_column_pos.has_value())
        {
            return readRowsFromHybridStorage(
                from_mark, current_task_last_mark, continue_reading,
                max_rows_to_read, rows_offset, res_columns, *row_column_pos);
        }
        /// If __row column not found, fall through to regular column-based reading
    }

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

size_t MergeTreeReaderCompactSingleBuffer::readRowsFromHybridStorage(
    size_t from_mark,
    size_t current_task_last_mark,
    bool /* continue_reading */,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns,
    size_t row_column_position)
{
    size_t read_rows = 0;

    /// Get the non-key columns info for deserialization
    auto all_non_key_columns = getNonKeyColumnsInRowData();
    auto non_key_serializations = getNonKeySerializations();

    /// Build a mapping from column name to position in res_columns
    std::unordered_map<String, size_t> column_name_to_result_pos;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
        column_name_to_result_pos[columns_to_read[i].name] = i;

    /// Identify columns to extract from __row and columns to read from column storage
    NamesAndTypesList columns_to_extract;
    std::vector<size_t> result_positions;
    NameSet columns_in_row_data;

    for (const auto & col : columns_to_read)
    {
        auto it = column_name_to_result_pos.find(col.name);
        if (it != column_name_to_result_pos.end())
        {
            /// Check if this column is in the non-key columns (i.e., stored in __row)
            bool in_row_data = false;
            for (const auto & non_key_col : all_non_key_columns)
            {
                if (non_key_col.name == col.name)
                {
                    in_row_data = true;
                    break;
                }
            }

            if (in_row_data)
            {
                columns_to_extract.push_back(col);
                result_positions.push_back(it->second);
                columns_in_row_data.insert(col.name);
            }
        }
    }

    /// Read row data from __row column
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

        /// Seek to the __row column in the compact data file
        stream->adjustRightMark(current_task_last_mark);
        stream->seekToMarkAndColumn(from_mark, has_substream_marks
            ? columns_substreams.getFirstSubstreamPosition(row_column_position)
            : row_column_position);

        /// Read the __row column data for this mark
        ColumnPtr row_data_column = RowDataColumn::type->createColumn();
        auto row_serialization = RowDataColumn::type->getDefaultSerialization();

        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = [&](const auto &) -> ReadBuffer * { return stream->getDataBuffer(); };
        deserialize_settings.data_part_type = MergeTreeDataPartType::Compact;

        ISerialization::DeserializeBinaryBulkStatePtr state;
        row_serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state, nullptr);
        row_serialization->deserializeBinaryBulkWithMultipleStreams(
            row_data_column, rows_offset, rows_to_read, deserialize_settings, state, nullptr);

        size_t rows_in_granule = row_data_column->size();

        /// Create mutable columns for extraction
        MutableColumns mutable_columns;
        for (const auto & col : columns_to_extract)
        {
            auto serialization_it = non_key_serializations.find(col.name);
            if (serialization_it != non_key_serializations.end())
                mutable_columns.push_back(col.type->createColumn(*serialization_it->second));
            else
                mutable_columns.push_back(col.type->createColumn());
        }

        /// Extract requested columns from each row
        const auto & row_data_string_column = assert_cast<const ColumnString &>(*row_data_column);

        for (size_t i = 0; i < rows_in_granule; ++i)
        {
            std::string_view row_data = row_data_string_column.getDataAt(i);
            String row_data_str(row_data.data(), row_data.size());

            row_data_serializer->extractColumns(
                row_data_str,
                all_non_key_columns,
                columns_to_extract,
                non_key_serializations,
                mutable_columns);
        }

        /// Append extracted data to result columns
        for (size_t i = 0; i < result_positions.size(); ++i)
        {
            size_t pos = result_positions[i];
            if (res_columns[pos] == nullptr)
                res_columns[pos] = std::move(mutable_columns[i]);
            else
                res_columns[pos]->assumeMutable()->insertRangeFrom(*mutable_columns[i], 0, mutable_columns[i]->size());
        }

        ++from_mark;
        read_rows += rows_in_granule;
        rows_offset = 0;
    }

    /// For key columns (not in __row), we need to read them from the compact data file
    /// Reset and read the key columns
    for (size_t pos = 0; pos < columns_to_read.size(); ++pos)
    {
        const auto & column_to_read = columns_to_read[pos];

        /// Skip columns that were already extracted from __row
        if (columns_in_row_data.contains(column_to_read.name))
            continue;

        if (!column_positions[pos])
            continue;

        /// This is a key column - read it from the compact data file
        /// Re-read from the beginning for these columns
        size_t key_from_mark = from_mark - (read_rows > 0 ? 1 : 0);  /// Approximate, may need adjustment
        while (key_from_mark < from_mark)
        {
            stream->adjustRightMark(current_task_last_mark);
            stream->seekToMarkAndColumn(key_from_mark, has_substream_marks
                ? columns_substreams.getFirstSubstreamPosition(*column_positions[pos])
                : *column_positions[pos]);

            std::unordered_map<String, ColumnPtr> columns_cache;
            readPrefix(pos, key_from_mark, *stream, nullptr);

            size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(key_from_mark);
            readData(pos, res_columns[pos], rows_to_read, 0, key_from_mark, res_columns[pos] ? res_columns[pos]->size() : 0, *stream, columns_cache, nullptr, nullptr);

            ++key_from_mark;
        }
    }

    next_mark = from_mark;
    return read_rows;
}

}
