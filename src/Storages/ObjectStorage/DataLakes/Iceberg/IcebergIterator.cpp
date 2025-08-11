
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

#    include <Common/ProfileEvents.h>
#    include <Common/SharedLockGuard.h>
#    include <Common/logger_useful.h>

namespace DB
{

using namespace Iceberg;

std::vector<Iceberg::ManifestFileEntry> SingleThreadIcebergKeysIterator::getPositionDeletes() const
{
    if (!data_snapshot)
    {
        return {};
    }
    std::vector<Iceberg::ManifestFileEntry> files;
    {
        for (const auto & manifest_list_entry : data_snapshot->manifest_list_entries)
        {
            Int64 previous_entry_schema_delete_objects = -1;
            std::optional<ManifestFilesPruner> pruner;
            auto manifest_file_ptr = getManifestFile(
                object_storage,
                configuration,
                iceberg_metadata_cache,
                schema_processor,
                format_version,
                table_location,
                local_context,
                log,
                manifest_list_entry.manifest_file_path,
                manifest_list_entry.added_sequence_number,
                manifest_list_entry.added_snapshot_id);
            const auto & data_files_in_manifest = manifest_file_ptr->getFiles(FileContentType::DATA);
            for (const auto & manifest_file_entry : data_files_in_manifest)
            {
                // Trying to reuse already initialized pruner
                if ((manifest_file_entry.schema_id != previous_entry_schema_delete_objects) && (use_partition_pruning))
                {
                    previous_entry_schema_delete_objects = manifest_file_entry.schema_id;
                    if (previous_entry_schema_delete_objects > manifest_file_entry.schema_id)
                    {
                        LOG_WARNING(log, "Manifest entries in file {} are not sorted by schema id", manifest_list_entry.manifest_file_path);
                    }
                    pruner.emplace(
                        *schema_processor,
                        table_snapshot->schema_id,
                        manifest_file_entry.schema_id,
                        filter_dag,
                        *manifest_file_ptr,
                        local_context);
                }

                if (manifest_file_entry.status != ManifestEntryStatus::DELETED)
                {
                    if (!use_partition_pruning || !pruner->canBePruned(manifest_file_entry))
                    {
                        files.push_back(manifest_file_entry);
                    }
                }
            }
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

IcebergDataObjectInfoPtr SingleThreadIcebergKeysIterator::next()
{
    if (!data_snapshot)
    {
        return nullptr;
    }

    while (manifest_file_index < data_snapshot->manifest_list_entries.size())
    {
        if (!current_manifest_file_content)
        {
            current_manifest_file_content = getManifestFile(
                object_storage,
                configuration,
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
        while (internal_data_index < current_manifest_file_content->getFiles(FileContentType::DATA).size())
        {
            const auto & manifest_file_entry = current_manifest_file_content->getFiles(FileContentType::DATA)[internal_data_index++];
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
                    filter_dag,
                    *current_manifest_file_content,
                    local_context);
            }
            if (!current_pruner || !current_pruner->canBePruned(manifest_file_entry))
            {
                return std::make_shared<IcebergDataObjectInfo>(manifest_file_entry, position_deletes_files);
            } else {
                
            }
        }
        current_manifest_file_content = nullptr;
        current_pruner = std::nullopt;
        ++manifest_file_index;
        internal_data_index = 0;
        previous_entry_schema = -1;
    }

    return nullptr;
}
}

#endif
