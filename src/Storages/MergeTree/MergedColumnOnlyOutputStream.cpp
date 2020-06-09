#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const Block & header_,
    CompressionCodecPtr default_codec,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    WrittenOffsetColumns * offset_columns_,
    const MergeTreeIndexGranularity & index_granularity,
    const MergeTreeIndexGranularityInfo * index_granularity_info)
    : IMergedBlockOutputStream(data_part), header(header_)
{
    const auto & global_settings = data_part->storage.global_context.getSettings();
    MergeTreeWriterSettings writer_settings(
        global_settings,
        index_granularity_info ? index_granularity_info->is_adaptive : data_part->storage.canUseAdaptiveGranularity(),
        global_settings.min_bytes_to_use_direct_io);

    writer = data_part->getWriter(
        header.getNamesAndTypesList(),
        indices_to_recalc,
        default_codec,
        std::move(writer_settings),
        index_granularity);

    writer->setWrittenOffsetColumns(offset_columns_);
    writer->initSkipIndices();
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : writer->getSkipIndices())
        std::copy(index->index.column_names.cbegin(), index->index.column_names.cend(),
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

MergeTreeData::DataPart::Checksums
MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums(MergeTreeData::MutableDataPartPtr & new_part, MergeTreeData::DataPart::Checksums & all_checksums)
{
    /// Finish columns serialization.
    MergeTreeData::DataPart::Checksums checksums;
    writer->finishDataSerialization(checksums);
    writer->finishSkipIndicesSerialization(checksums);

    auto columns = new_part->getColumns();

    auto removed_files = removeEmptyColumnsFromPart(new_part, columns, checksums);
    for (const String & removed_file : removed_files)
        if (all_checksums.files.count(removed_file))
            all_checksums.files.erase(removed_file);

    new_part->setColumns(columns);
    return checksums;
}

}
