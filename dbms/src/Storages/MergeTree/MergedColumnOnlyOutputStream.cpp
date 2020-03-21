#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeDataPartPtr & data_part, Block header_, bool sync_,
    CompressionCodecPtr default_codec, bool skip_offsets_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    WrittenOffsetColumns * offset_columns_,
    const MergeTreeIndexGranularity & index_granularity,
    const MergeTreeIndexGranularityInfo * index_granularity_info,
    bool is_writing_temp_files)
    : IMergedBlockOutputStream(data_part),
    header(std::move(header_)), sync(sync_)
{
    const auto & global_settings = data_part->storage.global_context.getSettings();
    MergeTreeWriterSettings writer_settings(
        global_settings,
        index_granularity_info ? index_granularity_info->is_adaptive : data_part->storage.canUseAdaptiveGranularity(),
        global_settings.min_bytes_to_use_direct_io);

    writer_settings.is_writing_temp_files = is_writing_temp_files;
    writer_settings.skip_offsets = skip_offsets_;

    writer = data_part->getWriter(header.getNamesAndTypesList(), indices_to_recalc,
        default_codec,std::move(writer_settings), index_granularity);
    writer->setWrittenOffsetColumns(offset_columns_);
    writer->initSkipIndices();
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : writer->getSkipIndices())
        std::copy(index->columns.cbegin(), index->columns.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    Names skip_indexes_column_names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());

    Block skip_indexes_block = getBlockAndPermute(block, skip_indexes_column_names, nullptr);

    size_t rows = block.rows();
    if (!rows)
        return;

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
