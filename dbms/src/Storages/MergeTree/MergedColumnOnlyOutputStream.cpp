#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeDataPartPtr & data_part, const Block & header_, bool sync_,
    CompressionCodecPtr default_codec, bool skip_offsets_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    WrittenOffsetColumns * offset_columns_,
    const MergeTreeIndexGranularity & index_granularity,
    const MergeTreeIndexGranularityInfo * index_granularity_info,
    const String & filename_suffix)
    : IMergedBlockOutputStream(data_part),
    header(header_), sync(sync_)
{
    // std::cerr << "(MergedColumnOnlyOutputStream) storage: " << storage.getTableName() << "\n";
    // std::cerr << "(MergedColumnOnlyOutputStream) can_use_adaptive_granularity: " << can_use_adaptive_granularity << "\n";
    // std::cerr << "(MergedColumnOnlyOutputStream) index_granularity_info: " << !!index_granularity_info_ << "\n";
    // if (index_granularity_info_)
    //     std::cerr << "(MergedColumnOnlyOutputStream) index_granularity_info->isAdaptive(): " << index_granularity_info_->is_adaptive << "\n";

    MergeTreeWriterSettings writer_settings(
        data_part->storage.global_context.getSettings(),
        index_granularity_info ? index_granularity_info->is_adaptive : data_part->storage.canUseAdaptiveGranularity());
    writer_settings.filename_suffix = filename_suffix;

    writer = data_part->getWriter(header.getNamesAndTypesList(), indices_to_recalc, default_codec, writer_settings, index_granularity);
    writer->setOffsetColumns(offset_columns_, skip_offsets_);
    writer->initSkipIndices();
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    std::set<String> skip_indexes_column_names_set;
    for (const auto & index : storage.skip_indices) /// FIXME save local indices
        std::copy(index->columns.cbegin(), index->columns.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    Names skip_indexes_column_names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());

    Block skip_indexes_block = getBlockAndPermute(block, skip_indexes_column_names, nullptr);

    size_t rows = block.rows();
    if (!rows)
        return;

    std::cerr << "(MergedColumnOnlyOutputStream::write) writing rows: " << rows << "\n";

    writer->write(block);
    writer->calculateAndSerializeSkipIndices(skip_indexes_block, rows);
    writer->next();
}

void MergedColumnOnlyOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeData::DataPart::Checksums MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums()
{
    /// Finish columns serialization.
    MergeTreeData::DataPart::Checksums checksums;
    writer->finishDataSerialization(checksums, sync);
    writer->finishSkipIndicesSerialization(checksums);

    return checksums;
}

}
