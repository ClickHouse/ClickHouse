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
    Int32 read_schema_id;
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
        ObjectStoragePtr object_storage_,
        ActionsDAG * filter_dag_,
        Iceberg::IcebergTableStateSnapshotPtr snapshot_,
        IcebergDataSnapshot iceberg_data_snapshot,
        IDataLakeMetadata::FileProgressCallback callback_)
        : use_partition_pruning(
              []()
              {
                  if (!local_context && filter_dag)
                  {
                      throw DB::Exception(
                          DB::ErrorCodes::LOGICAL_ERROR,
                          "Context is required with non-empty filter_dag to implement partition pruning for Iceberg table");
                  }
                  return filter_dag && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning].value;
              })
    {
    }

    size_t estimatedKeysCount() override { return data_files.size(); }

    ObjectInfoPtr next(size_t) override;

    void lazyInitialize()
    {
        if (initialized)
            return;

        initialized = true;
        position_deletes_files = getPositionDeleteFiles(filter_dag, context);
    }

private:
    bool initialized = false;

    IcebergDataSnapshot iceberg_data_snapshot;
    size_t current_manifest_list_entry_index = 0;
    std::optional<Iceberg::ManifestFileContent> current_manifest_content;
    size_t current_manifest_file_entry_index = 0;

    ActionsDAG * filter_dag = nullptr;

    // By Iceberg design it is difficult to avoid storing position deletes in memory.
    std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;

    bool use_partition_pruning;

    std::shared_ptr<IcebergSchemaProcessor> schema_processor;
    ContextPtr local_context;

    Iceberg::ManifestFilePtr
    getManifestFile(ContextPtr local_context, const String & filename, Int64 inherited_sequence_number, Int64 inherited_snapshot_id) const
        TSA_REQUIRES_SHARED(mutex);
    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);
    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;
    std::vector<Iceberg::ManifestFileEntry> getPositionDeleteFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const;

    ManifestFilePtr getManifestFile(
        ContextPtr local_context, const String & filename, Int64 inherited_sequence_number, Int64 inherited_snapshot_id) const;
};

} // namespace DB


#endif
