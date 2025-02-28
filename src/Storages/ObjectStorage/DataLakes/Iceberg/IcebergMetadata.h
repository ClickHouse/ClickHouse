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

#include <tuple>


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
        const Poco::JSON::Object::Ptr & metadata_object);


    /// Get data files. On first request it reads manifest_list file and iterates through manifest files to find all data files.
    /// All subsequent calls when the same data snapshot is relevant will return saved list of files (because it cannot be changed
    /// without changing metadata file). Drops on every snapshot update.
    Strings getDataFiles() const override { return getDataFilesImpl(nullptr); }

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override { return *schema_processor.getClickhouseTableSchemaById(current_snapshot_schema_id); }

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * iceberg_metadata = dynamic_cast<const IcebergMetadata *>(&other);
        return iceberg_metadata && getVersion() == iceberg_metadata->getVersion();
    }

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const ConfigurationObserverPtr & configuration,
        const ContextPtr & local_context,
        bool allow_experimental_delta_kernel_rs);


    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value() ? schema_processor.getClickhouseTableSchemaById(version_if_outdated.value()) : nullptr;
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value()
            ? schema_processor.getSchemaTransformationDagByIds(version_if_outdated.value(), current_snapshot_schema_id)
            : nullptr;
    }

    bool supportsExternalMetadataChange() const override { return true; }

    static Int32
    parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    Strings makePartitionPruning(const ActionsDAG & filter_dag) override;

    bool supportsPartitionPruning() override { return true; }

private:
    using ManifestEntryByDataFile = std::unordered_map<String, Iceberg::ManifestFileIterator>;

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    mutable Iceberg::ManifestFilesStorage manifest_files_by_name;
    mutable Iceberg::ManifestListsStorage manifest_lists_by_name;
    mutable ManifestEntryByDataFile manifest_file_by_data_file;

    std::tuple<Int32, Int64, Int32> getVersion() const
    {
        return std::make_tuple(
            current_metadata_version, current_snapshot.has_value() ? current_snapshot->getSnapshotId() : -1, current_snapshot_schema_id);
    }

    Int32 current_metadata_version;
    Int32 format_version;
    Int32 current_snapshot_schema_id;
    Poco::JSON::Object::Ptr metadata_object;
    std::optional<Iceberg::IcebergSnapshot> current_snapshot;

    mutable std::optional<Strings> cached_unprunned_files_for_last_processed_snapshot;

    bool updateState(const ContextPtr & local_context);

    Iceberg::ManifestList initializeManifestList(const String & filename) const;
    mutable std::vector<Iceberg::ManifestFileEntry> positional_delete_files_for_current_query;

    Iceberg::ManifestListIterator getManifestList(const String & filename) const;

    Int64 getRelevantSnapshotId(const Poco::JSON::Object::Ptr & metadata, const ContextPtr & local_context) const;

    bool needUpdateSnapshot(Int64 snapshot_id, Int32 current_schema_id) const;

    void addTableSchemaById(Int32 schema_id);

    Iceberg::IcebergSnapshot getSnapshot(Int64 snapshot_id) const;

    std::optional<Int32> getSchemaVersionByFileIfOutdated(String data_path) const;

    Iceberg::ManifestFileContent initializeManifestFile(const String & filename, Int64 inherited_sequence_number) const;

    Iceberg::ManifestFileIterator getManifestFile(const String & filename) const;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);

    Poco::JSON::Object::Ptr readJSON(const String & metadata_file_path, const ContextPtr & local_context) const;

    Strings getDataFilesImpl(const ActionsDAG * filter_dag) const;

    std::optional<Iceberg::ManifestFileIterator> tryGetManifestFile(const String & filename) const;

    std::string getManifestListBySnapshotId(const Poco::JSON::Object::Ptr & metadata, Int64 snapshot_id) const;
};

}

#endif
