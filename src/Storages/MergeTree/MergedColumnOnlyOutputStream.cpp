#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
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
    const CachesToPrewarm & prewarm_caches,
    WrittenOffsetSubstreams * written_offset_substreams,
    bool try_adaptive_codec,
    PackedFilesWriter * external_packed_skip_indices_writer)
    : IMergedBlockOutputStream(
          std::move(data_settings),
          data_part->getDataPartStoragePtr(),
          metadata_snapshot_,
          columns_list_,
          /*reset_columns=*/true)
{
    /// Granularity is never recomputed while writing only columns.
    MergeTreeWriterSettings writer_settings(
        data_part->storage.getContext()->getSettingsRef(),
        data_part->storage.getContext()->getWriteSettings(),
        storage_settings,
        data_part,
        data_part->index_granularity_info.mark_type.adaptive,
        /*rewrite_primary_key=*/ false,
        prewarm_caches,
        /*blocks_are_granules_size=*/ false,
        try_adaptive_codec);

    writer_settings.external_packed_skip_indices_writer = external_packed_skip_indices_writer;

    writer = createMergeTreeDataPartWriter(
        data_part->getType(),
        data_part->name, data_part->storage.getLogName(), data_part->getSerializations().toSerializationByName(),
        data_part_storage, data_part->index_granularity_info,
        storage_settings,
        columns_list_,
        data_part->getColumnPositions(),
        metadata_snapshot_,
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

    writer->write(block, nullptr, nullptr);
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
