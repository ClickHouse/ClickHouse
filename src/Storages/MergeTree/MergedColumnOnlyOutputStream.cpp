#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/WriteSettings.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeMutableDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    CompressionCodecPtr default_codec,
    const MergeTreeIndices & indices_to_recalc,
    const ColumnsStatistics & stats_to_recalc_,
    WrittenOffsetColumns * offset_columns_,
    const MergeTreeIndexGranularity & index_granularity,
    const MergeTreeIndexGranularityInfo * index_granularity_info)
    : IMergedBlockOutputStream(data_part->storage.getSettings(), data_part->getDataPartStoragePtr(), metadata_snapshot_, columns_list_, /*reset_columns=*/ true)
{
    const auto & global_settings = data_part->storage.getContext()->getSettingsRef();

    MergeTreeWriterSettings writer_settings(
        global_settings,
        data_part->storage.getContext()->getWriteSettings(),
        storage_settings,
        index_granularity_info ? index_granularity_info->mark_type.adaptive : data_part->storage.canUseAdaptiveGranularity(),
        /* rewrite_primary_key = */ false);

    writer = createMergeTreeDataPartWriter(
        data_part->getType(),
        data_part->name, data_part->storage.getLogName(), data_part->getSerializations(),
        data_part_storage, data_part->index_granularity_info,
        storage_settings,
        columns_list_,
        data_part->getColumnPositions(),
        metadata_snapshot_,
        data_part->storage.getVirtualsPtr(),
        indices_to_recalc,
        stats_to_recalc_,
        data_part->getMarksFileExtension(),
        default_codec,
        writer_settings,
        index_granularity);

    auto * writer_on_disk = dynamic_cast<MergeTreeDataPartWriterOnDisk *>(writer.get());
    if (!writer_on_disk)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergedColumnOnlyOutputStream supports only parts stored on disk");

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
    NameSet checksums_to_remove;
    writer->fillChecksums(checksums, checksums_to_remove);

    for (const auto & filename : checksums_to_remove)
        all_checksums.files.erase(filename);

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
        new_part->getDataPartStorage().removeFileIfExists(removed_file);
        all_checksums.files.erase(removed_file);
    }

    new_part->setColumns(columns, serialization_infos, metadata_snapshot->getMetadataVersion());
    return checksums;
}

void MergedColumnOnlyOutputStream::finish(bool sync)
{
    writer->finish(sync);
}

}
