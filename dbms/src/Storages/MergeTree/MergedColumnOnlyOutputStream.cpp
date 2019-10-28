#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeDataPartPtr & data_part_, const Block & header_, bool sync_,
    CompressionCodecPtr default_codec_, bool skip_offsets_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    WrittenOffsetColumns & already_written_offset_columns_,
    const MergeTreeIndexGranularityInfo * index_granularity_info_)
    : IMergedBlockOutputStream(
        data_part_, default_codec_,
        {
            data_part_->storage.global_context.getSettings().min_compress_block_size,
            data_part_->storage.global_context.getSettings().max_compress_block_size,
            data_part_->storage.global_context.getSettings().min_bytes_to_use_direct_io,
        },
        false,
        indices_to_recalc_,
        index_granularity_info_ ? index_granularity_info_->is_adaptive : data_part_->storage.canUseAdaptiveGranularity()),
    header(header_), sync(sync_), skip_offsets(skip_offsets_),
    already_written_offset_columns(already_written_offset_columns_)
{
    std::cerr << "(MergedColumnOnlyOutputStream) storage: " << storage.getTableName() << "\n";
    std::cerr << "(MergedColumnOnlyOutputStream) can_use_adaptive_granularity: " << can_use_adaptive_granularity << "\n";
    std::cerr << "(MergedColumnOnlyOutputStream) index_granularity_info: " << !!index_granularity_info_ << "\n";
    if (index_granularity_info_)
        std::cerr << "(MergedColumnOnlyOutputStream) index_granularity_info->isAdaptive(): " << index_granularity_info_->is_adaptive << "\n";

    writer = data_part_->getWriter(header.getNamesAndTypesList(), default_codec_, writer_settings);
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

    /// FIXME skip_offsets
    UNUSED(skip_offsets);
    UNUSED(already_written_offset_columns);

    auto [new_index_offset, new_current_mark] = writer->write(block, nullptr, current_mark, index_offset, index_granularity);

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
    MergeTreeData::DataPart::Checksums checksums;
    bool write_final_mark = with_final_mark && (index_offset != 0 || current_mark != 0);
    writer->finalize(checksums, write_final_mark, sync);

    finishSkipIndicesSerialization(checksums);

    return checksums;
}

}
