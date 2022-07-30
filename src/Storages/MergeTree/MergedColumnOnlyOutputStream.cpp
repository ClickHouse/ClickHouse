#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Interpreters/Context.h>
#include <IO/WriteSettings.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    DataPartStorageBuilderPtr data_part_storage_builder_,
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const Block & header_,
    CompressionCodecPtr default_codec,
    const MergeTreeIndices & indices_to_recalc,
    WrittenOffsetColumns * offset_columns_,
    const MergeTreeIndexGranularity & index_granularity,
    const MergeTreeIndexGranularityInfo * index_granularity_info)
    : IMergedBlockOutputStream(std::move(data_part_storage_builder_), data_part, metadata_snapshot_, header_.getNamesAndTypesList(), /*reset_columns=*/ true)
    , header(header_)
{
    const auto & global_settings = data_part->storage.getContext()->getSettings();
    const auto & storage_settings = data_part->storage.getSettings();

    MergeTreeWriterSettings writer_settings(
        global_settings,
        data_part->storage.getContext()->getWriteSettings(),
        storage_settings,
        index_granularity_info ? index_granularity_info->is_adaptive : data_part->storage.canUseAdaptiveGranularity(),
        /* rewrite_primary_key = */false);

    writer = data_part->getWriter(
        data_part_storage_builder,
        header.getNamesAndTypesList(),
        metadata_snapshot_,
        indices_to_recalc,
        default_codec,
        writer_settings,
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
    new_serialization_infos.add(block);
}

MergeTreeData::DataPart::Checksums
MergedColumnOnlyOutputStream::fillChecksums(
    MergeTreeData::MutableDataPartPtr & new_part,
    MergeTreeData::DataPart::Checksums & all_checksums)
{
    /// Finish columns serialization.
    MergeTreeData::DataPart::Checksums checksums;
    writer->fillChecksums(checksums);

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());

    auto columns = new_part->getColumns();
    auto serialization_infos = new_part->getSerializationInfos();
    serialization_infos.replaceData(new_serialization_infos);

    auto removed_files = removeEmptyColumnsFromPart(new_part, columns, serialization_infos, checksums);

    for (const String & removed_file : removed_files)
    {
        data_part_storage_builder->removeFileIfExists(removed_file);

        if (all_checksums.files.contains(removed_file))
            all_checksums.files.erase(removed_file);
    }

    new_part->setColumns(columns);
    new_part->setSerializationInfos(serialization_infos);

    return checksums;
}

void MergedColumnOnlyOutputStream::finish(bool sync)
{
    writer->finish(sync);
}

}
