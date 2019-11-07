#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeDataPartPtr & data_part, const Block & header_, bool sync_,
    CompressionCodecPtr default_codec, bool skip_offsets_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    WrittenOffsetColumns & already_written_offset_columns_,
    const MergeTreeIndexGranularityInfo * index_granularity_info)
    : IMergedBlockOutputStream(data_part),
    header(header_), sync(sync_), skip_offsets(skip_offsets_),
    already_written_offset_columns(already_written_offset_columns_)
{
    // std::cerr << "(MergedColumnOnlyOutputStream) storage: " << storage.getTableName() << "\n";
    // std::cerr << "(MergedColumnOnlyOutputStream) can_use_adaptive_granularity: " << can_use_adaptive_granularity << "\n";
    // std::cerr << "(MergedColumnOnlyOutputStream) index_granularity_info: " << !!index_granularity_info_ << "\n";
    // if (index_granularity_info_)
    //     std::cerr << "(MergedColumnOnlyOutputStream) index_granularity_info->isAdaptive(): " << index_granularity_info_->is_adaptive << "\n";

    WriterSettings writer_settings(data_part->storage.global_context.getSettings(), false);
    if (index_granularity_info && !index_granularity_info->is_adaptive)
        writer_settings.can_use_adaptive_granularity = false;
    writer = data_part->getWriter(header.getNamesAndTypesList(), indices_to_recalc, default_codec, writer_settings);
    writer_wide = typeid_cast<MergeTreeDataPartWriterWide *>(writer.get());
    if (!writer_wide)
        throw Exception("MergedColumnOnlyOutputStream can be used only for writing Wide parts", ErrorCodes::LOGICAL_ERROR);

    /// FIXME unnessary init of primary idx
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    std::set<String> skip_indexes_column_names_set;
    for (const auto & index : storage.skip_indices) /// FIXME save local indices
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

    WrittenOffsetColumns offset_columns = already_written_offset_columns;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = block.getByName(header.getByPosition(i).name);
        writer_wide->writeColumn(column.name, *column.type, *column.column, offset_columns, skip_offsets);
    }

    writer_wide->calculateAndSerializeSkipIndices(skip_indexes_columns, rows);
    writer_wide->next();
}

void MergedColumnOnlyOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeData::DataPart::Checksums MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums()
{
    /// Finish columns serialization.
    MergeTreeData::DataPart::Checksums checksums;
    bool write_final_mark = true; /// FIXME
    writer->finishDataSerialization(checksums, write_final_mark, sync);

    return checksums;
}

}
