#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const Block & header_,
    CompressionCodecPtr default_codec,
    const MergeTreeIndices & indices_to_recalc,
    WrittenOffsetColumns * offset_columns_,
    const MergeTreeIndexGranularity & index_granularity,
    const MergeTreeIndexGranularityInfo * index_granularity_info)
    : IMergedBlockOutputStream(data_part, metadata_snapshot_)
    , header(header_)
{
    const auto & global_settings = data_part->storage.getContext()->getSettings();
    const auto & storage_settings = data_part->storage.getSettings();

    MergeTreeWriterSettings writer_settings(
        global_settings,
        storage_settings,
        index_granularity_info ? index_granularity_info->is_adaptive : data_part->storage.canUseAdaptiveGranularity(),
        /* rewrite_primary_key = */false);

    writer = data_part->getWriter(
        header.getNamesAndTypesList(),
        metadata_snapshot_,
        indices_to_recalc,
        default_codec,
        std::move(writer_settings),
        index_granularity);

    auto * writer_on_disk = dynamic_cast<MergeTreeDataPartWriterOnDisk *>(writer.get());
    if (!writer_on_disk)
        throw Exception("MergedColumnOnlyOutputStream supports only parts stored on disk", ErrorCodes::NOT_IMPLEMENTED);

    writer_on_disk->setWrittenOffsetColumns(offset_columns_);
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!block.rows())
        return;

    writer->write(block, nullptr);
}

void MergedColumnOnlyOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeData::DataPart::Checksums
MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums(
    MergeTreeData::MutableDataPartPtr & new_part,
    MergeTreeData::DataPart::Checksums & all_checksums,
    bool sync)
{
    /// Finish columns serialization.
    MergeTreeData::DataPart::Checksums checksums;
    writer->finish(checksums, sync);

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());

    auto columns = new_part->getColumns();

    auto removed_files = removeEmptyColumnsFromPart(new_part, columns, checksums);
    for (const String & removed_file : removed_files)
        if (all_checksums.files.count(removed_file))
            all_checksums.files.erase(removed_file);

    new_part->setColumns(columns);
    return checksums;
}

}
