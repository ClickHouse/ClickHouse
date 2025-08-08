
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
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#    include <Common/ProfileEvents.h>
#    include <Common/SharedLockGuard.h>
#    include <Common/logger_useful.h>


#include <>


std::vector<ParsedDataFileInfo> IcebergMetadata::getDataFiles(
    const ActionsDAG * filter_dag, ContextPtr local_context, const std::vector<ManifestFileEntry> & position_delete_files) const
{
    return getFilesImpl<ParsedDataFileInfo>(
        filter_dag,
        FileContentType::DATA,
        local_context,
        [this, &position_delete_files](const ManifestFileEntry & entry)
        { return ParsedDataFileInfo{this->configuration.lock(), entry, position_delete_files}; });
}

std::vector<Iceberg::ManifestFileEntry>
IcebergMetadata::getPositionDeleteFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const
{
    return getFilesImpl<ManifestFileEntry>(
        filter_dag,
        FileContentType::POSITION_DELETE,
        local_context,
        // In the current design we can't avoid storing ManifestFileEntry in RAM explicitly for position deletes
        [](const ManifestFileEntry & entry) { return entry; });
}

std::span<const Iceberg::ManifestFileEntry> definePositionDeletesSpan(
    Iceberg::ManifestFileEntry data_object_, const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_)
{
    ///Object in position_deletes_objects_ are sorted by common_partition_specification, partition_key_value and added_sequence_number.
    /// It is done to have an invariant that position deletes objects which corresponds
    /// to the data object form a subsegment in a position_deletes_objects_ vector.
    /// We need to take all position deletes objects which has the same partition schema and value and has added_sequence_number
    /// greater than or equal to the data object added_sequence_number (https://iceberg.apache.org/spec/#scan-planning)
    /// ManifestFileEntry has comparator by default which helps to do that.
    auto beg_it = std::lower_bound(position_deletes_objects_.begin(), position_deletes_objects_.end(), data_object_);
    auto end_it = std::upper_bound(
        position_deletes_objects_.begin(),
        position_deletes_objects_.end(),
        data_object_,
        [](const Iceberg::ManifestFileEntry & lhs, const Iceberg::ManifestFileEntry & rhs)
        {
            return std::tie(lhs.common_partition_specification, lhs.partition_key_value)
                < std::tie(rhs.common_partition_specification, rhs.partition_key_value);
        });
    if (beg_it - position_deletes_objects_.begin() > end_it - position_deletes_objects_.begin())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Position deletes objects are not sorted by common_partition_specification and partition_key_value, "
            "beginning: {}, end: {}, position_deletes_objects size: {}",
            beg_it - position_deletes_objects_.begin(),
            end_it - position_deletes_objects_.begin(),
            position_deletes_objects_.size());
    }
    if ((beg_it != end_it) && configuration_->format != "Parquet")
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            configuration_->format);
    }
    position_deletes_objects = std::span<const Iceberg::ManifestFileEntry>{beg_it, end_it};
}


ParsedDataFileInfo::ParsedDataFileInfo(
    StorageObjectStorageConfigurationPtr configuration_,
    Iceberg::ManifestFileEntry data_object_,
    const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_)
    : data_object_file_path_key(data_object_.file_path_key)
    , data_object_file_path(data_object_.file_path)
{
}

ManifestFilePtr IcebergMetadata::getManifestFile(
    ContextPtr local_context, const String & filename, Int64 inherited_sequence_number, Int64 inherited_snapshot_id) const
{
    auto configuration_ptr = configuration.lock();

    auto create_fn = [&]()
    {
        ObjectInfo manifest_object_info(filename);

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto buffer = createReadBuffer(manifest_object_info, object_storage, local_context, log, read_settings);
        AvroForIcebergDeserializer manifest_file_deserializer(std::move(buffer), filename, getFormatSettings(local_context));

        return std::make_shared<ManifestFileContent>(
            manifest_file_deserializer,
            filename,
            format_version,
            configuration_ptr->getPathForRead().path,
            schema_processor,
            inherited_sequence_number,
            inherited_snapshot_id,
            table_location,
            local_context);
    };

    if (iceberg_metadata_cache)
    {
        auto manifest_file
            = iceberg_metadata_cache->getOrSetManifestFile(IcebergMetadataFilesCache::getKey(configuration_ptr, filename), create_fn);
        return manifest_file;
    }
    return create_fn();
}

template <typename T>
std::vector<T> IcebergMetadata::getFilesImpl(
    const ActionsDAG * filter_dag,
    FileContentType file_content_type,
    ContextPtr local_context,
    std::function<T(const ManifestFileEntry &)> transform_function) const
{
    std::vector<T> files;
    {
        SharedLockGuard lock(mutex);

        if (!relevant_snapshot)
            return {};


        for (const auto & manifest_list_entry : relevant_snapshot->manifest_list_entries)
        {
            Int64 previous_entry_schema = -1;
            std::optional<ManifestFilesPruner> pruner;
            auto manifest_file_ptr = getManifestFile(
                local_context,
                manifest_list_entry.manifest_file_path,
                manifest_list_entry.added_sequence_number,
                manifest_list_entry.added_snapshot_id);
            const auto & data_files_in_manifest = manifest_file_ptr->getFiles(file_content_type);
            for (const auto & manifest_file_entry : data_files_in_manifest)
            {
                // Trying to reuse already initialized pruner
                if ((manifest_file_entry.schema_id != previous_entry_schema) && (use_partition_pruning))
                {
                    previous_entry_schema = manifest_file_entry.schema_id;
                    if (previous_entry_schema > manifest_file_entry.schema_id)
                    {
                        LOG_WARNING(log, "Manifest entries in file {} are not sorted by schema id", manifest_list_entry.manifest_file_path);
                    }
                    pruner.emplace(
                        schema_processor,
                        relevant_snapshot_schema_id,
                        manifest_file_entry.schema_id,
                        filter_dag ? filter_dag : nullptr,
                        *manifest_file_ptr,
                        local_context);
                }

                if (manifest_file_entry.status != ManifestEntryStatus::DELETED)
                {
                    if (!use_partition_pruning || !pruner->canBePruned(manifest_file_entry))
                    {
                        files.push_back(transform_function(manifest_file_entry));
                    }
                }
            }
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}
