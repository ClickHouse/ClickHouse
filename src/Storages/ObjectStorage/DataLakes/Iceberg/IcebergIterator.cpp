
#include "config.h"
#if USE_AVRO

#    include <cstddef>
#    include <memory>
#    include <optional>
#    include <Formats/FormatFilterInfo.h>
#    include <Formats/FormatParserSharedResources.h>
#    include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Stringifier.h>
#    include <Common/Exception.h>


#    include <Core/NamesAndTypes.h>
#    include <Core/Settings.h>
#    include <Databases/DataLake/Common.h>
#    include <Databases/DataLake/ICatalog.h>
#    include <Disks/ObjectStorages/StoredObject.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <Interpreters/Context.h>

#    include <IO/CompressedReadBufferWrapper.h>
#    include <Interpreters/ExpressionActions.h>
#    include <Storages/ObjectStorage/DataLakes/Common.h>
#    include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#    include <Storages/ObjectStorage/StorageObjectStorageSource.h>

#    include <Storages/ColumnsDescription.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#    include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>

#    include <Common/SharedLockGuard.h>
#    include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event IcebergPartitionPrunedFiles;
extern const Event IcebergMinMaxIndexPrunedFiles;
};

namespace DB
{


using namespace Iceberg;

std::optional<ManifestFileEntry> SingleThreadIcebergKeysIterator::next()
{
    if (!data_snapshot)
    {
        return std::nullopt;
    }

    while (manifest_file_index < data_snapshot->manifest_list_entries.size())
    {
        if (!current_manifest_file_content)
        {
            if (data_snapshot->manifest_list_entries[manifest_file_index].content_type != manifest_file_content_type)
            {
                ++manifest_file_index;
                continue;
            }
            current_manifest_file_content = getManifestFile(
                object_storage,
                data_source_description,
                path_for_read,
                iceberg_metadata_cache,
                schema_processor,
                format_version,
                table_location,
                local_context,
                log,
                data_snapshot->manifest_list_entries[manifest_file_index].manifest_file_path,
                data_snapshot->manifest_list_entries[manifest_file_index].added_sequence_number,
                data_snapshot->manifest_list_entries[manifest_file_index].added_snapshot_id);
            internal_data_index = 0;
        }
        while (internal_data_index < current_manifest_file_content->getFiles(content_type).size())
        {
            const auto & manifest_file_entry = current_manifest_file_content->getFiles(content_type)[internal_data_index++];
            LOG_DEBUG(
                log,
                "Inspecting manifest file: {}, data file: {}, content type: {}, schema id: {}",
                current_manifest_file_content->getPathToManifestFile(),
                manifest_file_entry.file_path,
                FileContentTypeToString(content_type),
                manifest_file_entry.schema_id);
            if (manifest_file_entry.status == ManifestEntryStatus::DELETED)
            {
                continue;
            }

            if ((manifest_file_entry.schema_id != previous_entry_schema) && (use_partition_pruning))
            {
                previous_entry_schema = manifest_file_entry.schema_id;
                if (previous_entry_schema > manifest_file_entry.schema_id)
                {
                    LOG_WARNING(
                        log,
                        "Manifest entries in file {} are not sorted by schema id",
                        current_manifest_file_content->getPathToManifestFile());
                }
                current_pruner.emplace(
                    *schema_processor,
                    table_snapshot->schema_id,
                    manifest_file_entry.schema_id,
                    filter_dag.get(),
                    *current_manifest_file_content,
                    local_context);
            }
            LOG_DEBUG(
                log,
                "Current pruner exists: {}, Manifest file entry: {}, schema id: {}",
                current_pruner.has_value(),
                manifest_file_entry.file_path,
                manifest_file_entry.schema_id);
            auto pruning_status = current_pruner ? current_pruner->canBePruned(manifest_file_entry) : PruningReturnStatus::NOT_PRUNED;
            switch (pruning_status)
            {
                case PruningReturnStatus::NOT_PRUNED:
                    return manifest_file_entry;
                case PruningReturnStatus::MIN_MAX_INDEX_PRUNED: {
                    ++min_max_index_pruned_files;
                    break;
                }
                case PruningReturnStatus::PARTITION_PRUNED: {
                    ++partition_pruned_files;
                    break;
                }
            }
        }
        current_manifest_file_content = nullptr;
        current_pruner = std::nullopt;
        ++manifest_file_index;
        internal_data_index = 0;
        previous_entry_schema = -1;
    }

    return std::nullopt;
}

SingleThreadIcebergKeysIterator::~SingleThreadIcebergKeysIterator()
{
    LOG_DEBUG(
        &Poco::Logger::get("IcebergIterator"),
        "SingleThreadIcebergKeysIterator destroyed, min max pruned files: {}, partition pruned files: {}",
        min_max_index_pruned_files,
        partition_pruned_files);
    if (partition_pruned_files > 0)
        ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunedFiles, partition_pruned_files);
    if (min_max_index_pruned_files > 0)
        ProfileEvents::increment(ProfileEvents::IcebergMinMaxIndexPrunedFiles, min_max_index_pruned_files);
}
}

#endif
