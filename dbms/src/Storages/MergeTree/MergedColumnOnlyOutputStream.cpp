#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    MergeTreeData & storage_, const Block & header_, const String & part_path_, bool sync_,
    CompressionCodecPtr default_codec_, bool skip_offsets_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    WrittenOffsetColumns & already_written_offset_columns_,
    const MergeTreeIndexGranularity & index_granularity_,
    const MergeTreeIndexGranularityInfo * index_granularity_info_)
    : IMergedBlockOutputStream(
        storage_, part_path_, storage_.global_context.getSettings().min_compress_block_size,
        storage_.global_context.getSettings().max_compress_block_size, default_codec_,
        storage_.global_context.getSettings().min_bytes_to_use_direct_io,
        false,
        indices_to_recalc_,
        index_granularity_,
        index_granularity_info_),
    header(header_), sync(sync_), skip_offsets(skip_offsets_),
    already_written_offset_columns(already_written_offset_columns_)
{
    serialization_states.reserve(header.columns());
    WrittenOffsetColumns tmp_offset_columns;
    IDataType::SerializeBinaryBulkSettings settings;

    for (const auto & column_name : header.getNames())
    {
        const auto & col = header.getByName(column_name);

        const auto columns = storage.getColumns();
        addStreams(part_path, col.name, *col.type, columns.getCodecOrDefault(col.name, codec), 0, skip_offsets);
        serialization_states.emplace_back(nullptr);
        settings.getter = createStreamGetter(col.name, tmp_offset_columns, false);
        col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
    }

    initSkipIndices();
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    std::set<String> skip_indexes_column_names_set;
    for (const auto & index : skip_indices)
        std::copy(index->columns.cbegin(), index->columns.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    Names skip_indexes_column_names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());

    std::vector<ColumnWithTypeAndName> skip_indexes_columns(skip_indexes_column_names.size());
    std::map<String, size_t> skip_indexes_column_name_to_position;
    for (size_t i = 0, size = skip_indexes_column_names.size(); i < size; ++i)
    {
        const auto & name = skip_indexes_column_names[i];
        skip_indexes_column_name_to_position.emplace(name, i);
        skip_indexes_columns[i] = block.getByName(name);
    }

    size_t rows = block.rows();
    if (!rows)
        return;

    size_t new_index_offset = 0;
    size_t new_current_mark = 0;
    WrittenOffsetColumns offset_columns = already_written_offset_columns;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.getByName(header.getByPosition(i).name);
        std::tie(new_current_mark, new_index_offset) = writeColumn(column.name, *column.type, *column.column, offset_columns, skip_offsets, serialization_states[i], current_mark);
    }

    /// Should be written before index offset update, because we calculate,
    /// indices of currently written granules
    calculateAndSerializeSkipIndices(skip_indexes_columns, rows);

    index_offset = new_index_offset;
    current_mark = new_current_mark;
}

void MergedColumnOnlyOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeData::DataPart::Checksums MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums()
{
    /// Finish columns serialization.
    auto & settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;

    WrittenOffsetColumns offset_columns;
    for (size_t i = 0, size = header.columns(); i < size; ++i)
    {
        auto & column = header.getByPosition(i);
        serialize_settings.getter = createStreamGetter(column.name, already_written_offset_columns, skip_offsets);
        column.type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[i]);

        /// We wrote at least one row
        if (with_final_mark && (index_offset != 0 || current_mark != 0))
            writeFinalMark(column.name, column.type, offset_columns, skip_offsets, serialize_settings.path);
    }

    MergeTreeData::DataPart::Checksums checksums;

    for (auto & column_stream : column_streams)
    {
        column_stream.second->finalize();
        if (sync)
            column_stream.second->sync();

        column_stream.second->addToChecksums(checksums);
    }

    finishSkipIndicesSerialization(checksums);

    column_streams.clear();
    serialization_states.clear();

    return checksums;
}

}
