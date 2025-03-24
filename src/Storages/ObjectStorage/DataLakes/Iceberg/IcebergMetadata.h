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

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

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
    NamesAndTypesList getTableSchema() const override
    {
        return *schema_processor.getClickhouseTableSchemaById(relevant_snapshot_schema_id);
    }

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * iceberg_metadata = dynamic_cast<const IcebergMetadata *>(&other);
        return iceberg_metadata && getVersion() == iceberg_metadata->getVersion();
    }

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const ConfigurationObserverPtr & configuration,
        const ContextPtr & local_context);

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value() ? schema_processor.getClickhouseTableSchemaById(version_if_outdated.value()) : nullptr;
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value()
            ? schema_processor.getSchemaTransformationDagByIds(version_if_outdated.value(), relevant_snapshot_schema_id)
            : nullptr;
    }

    bool supportsExternalMetadataChange() const override { return true; }

    static Int32
    parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    Strings makePartitionPruning(const ActionsDAG & filter_dag) override;

    bool supportsPartitionPruning() override { return true; }

    std::optional<size_t> totalRows() const override;
    std::optional<size_t> totalBytes() const override;

private:
    using ManifestEntryByDataFile = std::unordered_map<String, Iceberg::ManifestFilePtr>;
    using ManifestFilesStorage = std::unordered_map<String, Iceberg::ManifestFilePtr>;
    using ManifestListsStorage = std::unordered_map<String, Iceberg::ManifestListPtr>;

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    mutable ManifestFilesStorage manifest_files_by_name;
    mutable ManifestListsStorage manifest_lists_by_name;
    mutable ManifestEntryByDataFile manifest_file_by_data_file;

    std::tuple<Int64, Int32> getVersion() const { return std::make_tuple(relevant_snapshot_id, relevant_snapshot_schema_id); }

    Int32 last_metadata_version;
    Poco::JSON::Object::Ptr last_metadata_object;
    Int32 format_version;


    Int32 relevant_snapshot_schema_id;
    std::optional<Iceberg::IcebergSnapshot> relevant_snapshot;
    Int64 relevant_snapshot_id{-1};
    String table_location;

    mutable std::optional<Strings> cached_unprunned_files_for_last_processed_snapshot;

    void updateState(const ContextPtr & local_context);

    void updateSnapshot();

    Iceberg::ManifestList initializeManifestList(const String & filename) const;
    mutable std::vector<Iceberg::ManifestFileEntry> positional_delete_files_for_current_query;

    void addTableSchemaById(Int32 schema_id);

    Iceberg::ManifestListPtr getManifestList(const String & filename) const;

    std::optional<Int32> getSchemaVersionByFileIfOutdated(String data_path) const;

    Iceberg::ManifestFilePtr initializeManifestFile(const String & filename, Int64 inherited_sequence_number) const;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);

    Poco::JSON::Object::Ptr readJSON(const String & metadata_file_path, const ContextPtr & local_context) const;

    Strings getDataFilesImpl(const ActionsDAG * filter_dag) const;

    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;
};
}

#endif
