#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{

namespace
{
    constexpr auto INDEX_FILE_EXTENSION = ".idx";
}

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    MergeTreeData & storage_, const Block & header_, String part_path_, bool sync_,
    CompressionCodecPtr default_codec_, bool skip_offsets_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    WrittenOffsetColumns & already_written_offset_columns,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergedBlockOutputStream(
        storage_, storage_.global_context.getSettings().min_compress_block_size,
        storage_.global_context.getSettings().max_compress_block_size, default_codec_,
        storage_.global_context.getSettings().min_bytes_to_use_direct_io,
        false,
        index_granularity_),
    header(header_), part_path(part_path_), sync(sync_), skip_offsets(skip_offsets_),
    skip_indices(indices_to_recalc), already_written_offset_columns(already_written_offset_columns)
{
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!initialized)
    {
        column_streams.clear();
        serialization_states.clear();
        serialization_states.reserve(block.columns());
        WrittenOffsetColumns tmp_offset_columns;
        IDataType::SerializeBinaryBulkSettings settings;

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.safeGetByPosition(i);

            const auto columns = storage.getColumns();
            addStreams(part_path, col.name, *col.type, columns.getCodecOrDefault(col.name, codec), 0, skip_offsets);
            serialization_states.emplace_back(nullptr);
            settings.getter = createStreamGetter(col.name, tmp_offset_columns, false);
            col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
        }

        for (const auto & index : skip_indices)
        {
            String stream_name = index->getFileName();
            skip_indices_streams.emplace_back(
                    std::make_unique<ColumnStream>(
                            stream_name,
                            part_path + stream_name, INDEX_FILE_EXTENSION,
                            part_path + stream_name, marks_file_extension,
                            codec, max_compress_block_size,
                            0, aio_threshold));
            skip_indices_aggregators.push_back(index->createIndexAggregator());
            skip_index_filling.push_back(0);
        }

        initialized = true;
    }

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

    {
        /// Creating block for update
        Block indices_update_block(skip_indexes_columns);
        size_t skip_index_current_mark = 0;

        /// Filling and writing skip indices like in IMergedBlockOutputStream::writeColumn
        for (size_t i = 0; i < storage.skip_indices.size(); ++i)
        {
            const auto index = storage.skip_indices[i];
            auto & stream = *skip_indices_streams[i];
            size_t prev_pos = 0;
            skip_index_current_mark = skip_index_mark;
            while (prev_pos < rows)
            {
                UInt64 limit = 0;
                if (prev_pos == 0 && index_offset != 0)
                {
                    limit = index_offset;
                }
                else
                {
                    limit = index_granularity.getMarkRows(skip_index_current_mark);
                    if (skip_indices_aggregators[i]->empty())
                    {
                        skip_indices_aggregators[i] = index->createIndexAggregator();
                        skip_index_filling[i] = 0;

                        if (stream.compressed.offset() >= min_compress_block_size)
                            stream.compressed.next();

                        writeIntBinary(stream.plain_hashing.count(), stream.marks);
                        writeIntBinary(stream.compressed.offset(), stream.marks);
                        /// Actually this numbers is redundant, but we have to store them
                        /// to be compatible with normal .mrk2 file format
                        if (storage.canUseAdaptiveGranularity())
                            writeIntBinary(1UL, stream.marks);

                        ++skip_index_current_mark;
                    }
                }

                size_t pos = prev_pos;
                skip_indices_aggregators[i]->update(indices_update_block, &pos, limit);

                if (pos == prev_pos + limit)
                {
                    ++skip_index_filling[i];

                    /// write index if it is filled
                    if (skip_index_filling[i] == index->granularity)
                    {
                        skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                        skip_index_filling[i] = 0;
                    }
                }
                prev_pos = pos;
            }
        }
        skip_index_mark = skip_index_current_mark;
    }

    size_t new_index_offset = 0;
    size_t new_current_mark = 0;
    WrittenOffsetColumns offset_columns = already_written_offset_columns;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        std::tie(new_current_mark, new_index_offset) = writeColumn(column.name, *column.type, *column.column, offset_columns, skip_offsets, serialization_states[i], current_mark);
    }

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


        if (with_final_mark)
            writeFinalMark(column.name, column.type, offset_columns, skip_offsets, serialize_settings.path);
    }

    /// Finish skip index serialization
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    MergeTreeData::DataPart::Checksums checksums;

    for (auto & column_stream : column_streams)
    {
        column_stream.second->finalize();
        if (sync)
            column_stream.second->sync();

        column_stream.second->addToChecksums(checksums);
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->finalize();
        stream->addToChecksums(checksums);
    }

    column_streams.clear();
    serialization_states.clear();
    initialized = false;

    skip_indices_streams.clear();
    skip_indices_aggregators.clear();
    skip_index_filling.clear();

    return checksums;
}

}
