#pragma once
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#include <Common/SharedMutex.h>
#include <tuple>
#include <optional>
#include <base/defines.h>

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>



namespace DB {

using namespace Iceberg;

struct ParsedDataFileInfo
{
    ParsedDataFileInfo(
        StorageObjectStorageConfigurationPtr configuration_,
        Iceberg::ManifestFileEntry data_object_,
        const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_);
    String data_object_file_path_key;
    String data_object_file_path; // Full path to the data object file
    std::span<const Iceberg::ManifestFileEntry> position_deletes_objects;

    bool operator<(const ParsedDataFileInfo & other) const
    {
        return std::tie(data_object_file_path_key) < std::tie(other.data_object_file_path_key);
    }
};

struct IcebergDataObjectInfo : public RelativePathWithMetadata
{
    IcebergDataObjectInfo(std::optional<ObjectMetadata> metadata_, ParsedDataFileInfo parsed_data_file_info_);

    ParsedDataFileInfo parsed_data_file_info;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;


class IcebergKeysIterator : public IObjectIterator
{
public:
    IcebergKeysIterator(
        ObjectStoragePtr object_storage_, Iceberg::IcebergTableStateSnapshotPtr snapshot_, IcebergDataSnapshot iceberg_data_snapshot, IDataLakeMetadata::FileProgressCallback callback_);

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    ObjectInfoPtr next(size_t) override;

    void lazyInitialize() {
        if (initialized)
            return;

        initialized = true;
        position_deletes_files = getPositionDeleteFiles(filter_dag, context);
    }

private:
    bool initialized = false;

    IcebergDataSnapshot iceberg_data_snapshot;
    size_t current_manifest_list_entry_index = 0;
    Iceberg::ManifestFileContent current_manifest_content;
    size_t current_manifest_file_entry_index = 0;

    ActionsDAG * filter_dag = nullptr;

    // By Iceberg design it is difficult to avoid storing position deletes in memory.
    std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;

    std::shared_ptr<IcebergSchemaProcessor> schema_processor;

    Iceberg::ManifestFilePtr
    getManifestFile(ContextPtr local_context, const String & filename, Int64 inherited_sequence_number, Int64 inherited_snapshot_id) const
        TSA_REQUIRES_SHARED(mutex);
    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);
    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;

    std::vector<ParsedDataFileInfo> getDataFiles(
        const ActionsDAG * filter_dag,
        ContextPtr local_context,
        const std::vector<Iceberg::ManifestFileEntry> & position_delete_files) const;
    std::vector<Iceberg::ManifestFileEntry> getPositionDeleteFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const;

    ManifestFilePtr getManifestFile(
        ContextPtr local_context, const String & filename, Int64 inherited_sequence_number, Int64 inherited_snapshot_id) const;
    };

template <typename T>
std::vector<T> IcebergMetadata::getFilesImpl(
    const ActionsDAG * filter_dag,
    FileContentType file_content_type,
    ContextPtr local_context,
    std::function<T(const ManifestFileEntry &)> transform_function) const
{
    if (!local_context && filter_dag)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Context is required with non-empty filter_dag to implement partition pruning for Iceberg table");
    }

    bool use_partition_pruning = filter_dag && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning].value;

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


};


IcebergKeysIterator::IcebergKeysIterator(ObjectStoragePtr object_storage_, Iceberg::IcebergTableStateSnapshotPtr snapshot_, IDataLakeMetadata::FileProgressCallback callback_) {

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

        auto buffer = StorageObjectStorageSource::createReadBuffer(manifest_object_info, object_storage, local_context, log, read_settings);
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




} // namespace DB


#endif
