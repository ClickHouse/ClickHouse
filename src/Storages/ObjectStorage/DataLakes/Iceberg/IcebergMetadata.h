#pragma once
#include "config.h"

#if USE_AVRO

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h"

#include <unordered_map>

namespace DB
{

class IcebergMetadata : public IDataLakeMetadata, private WithContext
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;


    static constexpr auto name = "Iceberg";

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationObserverPtr configuration_,
        const DB::ContextPtr & context_,
        Int32 metadata_version_,
        Int32 format_version_,
        const Poco::JSON::Object::Ptr & object);


    /// Get data files. On first request it reads manifest_list file and iterates through manifest files to find all data files.
    /// All subsequent calls when the same data snapshot is relevant will return saved list of files (because it cannot be changed
    /// without changing metadata file). Drops on every snapshot update.
    Strings getDataFiles() const override;

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override { return *schema_processor.getClickhouseTableSchemaById(current_schema_id); }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const override { return column_name_to_physical_name; }

    const DataLakePartitionColumns & getPartitionColumns() const override { return partition_columns; }

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * iceberg_metadata = dynamic_cast<const IcebergMetadata *>(&other);
        return iceberg_metadata && getVersion() == iceberg_metadata->getVersion();
    }

    static DataLakeMetadataPtr
    create(const ObjectStoragePtr & object_storage, const ConfigurationObserverPtr & configuration, const ContextPtr & local_context);

    size_t getVersion() const { return current_metadata_version; }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value() ? schema_processor.getClickhouseTableSchemaById(version_if_outdated.value()) : nullptr;
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value()
            ? schema_processor.getSchemaTransformationDagByIds(version_if_outdated.value(), current_schema_id)
            : nullptr;
    }

    bool supportsExternalMetadataChange() const override { return true; }

    static Int32
    parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & local_context) override;

private:
    using ManifestEntryByDataFile = std::unordered_map<String, Iceberg::ManifestFileEntry>;

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    mutable Iceberg::ManifestFilesByName manifest_files_by_name;
    mutable Iceberg::ManifestListsByName manifest_lists_by_name;
    mutable ManifestEntryByDataFile manifest_entry_by_data_file;

    Int32 current_metadata_version;
    Int32 format_version;
    Int32 current_schema_id;
    std::optional<Iceberg::IcebergSnapshot> current_snapshot;

    mutable std::optional<Strings> cached_files_for_current_snapshot;

    Iceberg::ManifestList initializeManifestList(const String & manifest_list_file) const;

    Iceberg::IcebergSnapshot getSnapshot(const String & manifest_list_file) const;

    std::optional<Int32> getSchemaVersionByFileIfOutdated(String data_path) const;

    Iceberg::ManifestFileEntry getManifestFile(const String & manifest_file) const;

    Iceberg::ManifestFileEntry initializeManifestFile(const String & filename, const ConfigurationPtr & configuration_ptr) const;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);

    Poco::JSON::Object::Ptr readJSON(const String & metadata_file_path, const ContextPtr & local_context) const;

    //Fields are needed only for providing dynamic polymorphism
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
};

}

#endif
