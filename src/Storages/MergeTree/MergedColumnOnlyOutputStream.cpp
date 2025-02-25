#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/WriteSettings.h>
#include "Common/logger_useful.h"

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
    const MergeTreeIndices & indices_to_recalc,
    const ColumnsStatistics & stats_to_recalc,
    CompressionCodecPtr default_codec,
    MergeTreeIndexGranularityPtr index_granularity_ptr,
    size_t part_uncompressed_bytes,
    WrittenOffsetColumns * offset_columns)
    : IMergedBlockOutputStream(data_part->storage.getSettings(), data_part->getDataPartStoragePtr(), metadata_snapshot_, columns_list_, /*reset_columns=*/ true)
{
    /// Save marks in memory if prewarm is enabled to avoid re-reading marks file.
    bool save_marks_in_cache = data_part->storage.getMarkCacheToPrewarm(part_uncompressed_bytes) != nullptr;
    /// Save primary index in memory if cache is disabled or is enabled with prewarm to avoid re-reading priamry index file.
    bool save_primary_index_in_memory = !data_part->storage.getPrimaryIndexCache() || data_part->storage.getPrimaryIndexCacheToPrewarm(part_uncompressed_bytes);

    /// Granularity is never recomputed while writing only columns.
    MergeTreeWriterSettings writer_settings(
        data_part->storage.getContext()->getSettingsRef(),
        data_part->storage.getContext()->getWriteSettings(),
        storage_settings,
        data_part->index_granularity_info.mark_type.adaptive,
        /*rewrite_primary_key=*/ false,
        save_marks_in_cache,
        save_primary_index_in_memory,
        /*blocks_are_granules_size=*/ false);

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
        stats_to_recalc,
        data_part->getMarksFileExtension(),
        default_codec,
        writer_settings,
        std::move(index_granularity_ptr));

    auto * writer_on_disk = dynamic_cast<MergeTreeDataPartWriterOnDisk *>(writer.get());
    if (!writer_on_disk)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergedColumnOnlyOutputStream supports only parts stored on disk");

    writer_on_disk->setWrittenOffsetColumns(offset_columns);
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!block.rows())
        return;

    writer->write(block, nullptr);
    new_serialization_infos.add(block);
}

std::pair<MergeTreeData::DataPart::Checksums, NameSet>
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

    LOG_TRACE(getLogger("MergedColumnOnlyOutputStream"), "filled checksums {}", new_part->getNameWithState());

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());

    auto columns = new_part->getColumns();
    auto serialization_infos = new_part->getSerializationInfos();
    serialization_infos.replaceData(new_serialization_infos);

    new_part->setColumns(columns, serialization_infos, metadata_snapshot->getMetadataVersion());

    auto removed_files = removeEmptyColumnsFromPart(new_part, columns, serialization_infos, checksums);
    for (const String & removed_file : removed_files)
    {
        LOG_DEBUG(getLogger("MergedColumnOnlyOutputStream"), "remove file from checksum {}, existsFile {}",
            removed_file, new_part->getDataPartStorage().existsFile(removed_file));
            all_checksums.files.erase(removed_file);
    }

    return {std::move(checksums), std::move(removed_files)};
}

void MergedColumnOnlyOutputStream::finish(bool sync)
{
    writer->finish(sync);
}

void MergedColumnOnlyOutputStream::cancel() noexcept
{
    writer->cancel();
}

}
