#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/WriteSettings.h>

namespace DB
{

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    const MergeTreeMutableDataPartPtr & data_part,
    MergeTreeSettingsPtr data_settings,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & indices_to_recalc,
    CompressionCodecPtr default_codec,
    MergeTreeIndexGranularityPtr index_granularity_ptr,
    size_t part_uncompressed_bytes,
    WrittenOffsetSubstreams * written_offset_substreams)
    : IMergedBlockOutputStream(
          std::move(data_settings),
          data_part->getDataPartStoragePtr(),
          metadata_snapshot_,
          columns_list_,
          /*reset_columns=*/true)
{
    /// Save marks in memory if prewarm is enabled to avoid re-reading marks file.
    auto prewarm_caches = data_part->storage.getCachesToPrewarm(part_uncompressed_bytes);
    bool save_marks_in_cache = prewarm_caches.mark_cache != nullptr || prewarm_caches.index_mark_cache != nullptr;
    /// Save primary index in memory if cache is disabled or is enabled with prewarm to avoid re-reading primary index file.
    bool save_primary_index_in_memory = !data_part->storage.getPrimaryIndexCache() || prewarm_caches.primary_index_cache;

    /// Granularity is never recomputed while writing only columns.
    MergeTreeWriterSettings writer_settings(
        data_part->storage.getContext()->getSettingsRef(),
        data_part->storage.getContext()->getWriteSettings(),
        storage_settings,
        data_part,
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
        data_part->getMarksFileExtension(),
        default_codec,
        writer_settings,
        std::move(index_granularity_ptr),
        written_offset_substreams);
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!block.rows())
        return;

    writer->write(block, nullptr);
    new_serialization_infos.add(block);
}

void MergedColumnOnlyOutputStream::finalizeIndexGranularity()
{
    writer->finalizeIndexGranularity();
}

MergeTreeData::DataPart::Checksums MergedColumnOnlyOutputStream::fillChecksums(MergeTreeData::MutableDataPartPtr & new_part, MergeTreeDataPartChecksums & all_checksums)
{
    /// Finish columns serialization.
    MergeTreeData::DataPart::Checksums checksums;
    NameSet checksums_to_remove;
    writer->fillChecksums(checksums, checksums_to_remove);

    for (const auto & filename : checksums_to_remove)
        all_checksums.files.erase(filename);

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
    {
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());
    }

    auto columns = new_part->getColumns();
    auto serialization_infos = new_part->getSerializationInfos();
    serialization_infos.replaceData(new_serialization_infos);

    NameSet empty_columns;
    for (const auto & column : writer->getColumnsSample())
    {
        if (new_part->expired_columns.contains(column.name))
            empty_columns.emplace(column.name);
    }
    auto removed_files = removeEmptyColumnsFromPart(new_part, columns, empty_columns, serialization_infos, checksums);

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

void MergedColumnOnlyOutputStream::cancel() noexcept
{
    writer->cancel();
}

}
