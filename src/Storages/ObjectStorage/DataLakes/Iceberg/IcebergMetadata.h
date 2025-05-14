#pragma once
#include "config.h"

#if USE_AVRO

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
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

struct IcebergDataObjectInfo : public RelativePathWithMetadata
{
    explicit IcebergDataObjectInfo(
        Iceberg::ManifestFileEntry data_object_,
        std::optional<ObjectMetadata> metadata_ = std::nullopt,
        const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_ = {})
        : RelativePathWithMetadata(data_object_.file.file_name, std::move(metadata_))
        , data_object(data_object_)
    {
        for (auto & position_deletes_object : position_deletes_objects_)
        {
            if (position_deletes_object.partition_key_value == data_object.partition_key_value)
                position_deletes_objects.push_back(position_deletes_object);
        }
    }

    const Iceberg::ManifestFileEntry data_object;
    std::vector<Iceberg::ManifestFileEntry> position_deletes_objects;

    // Return the path in the Iceberg metadata
    std::string getIcebergDataPath() const { return data_object.file.file_path_key; }
};
using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

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
        const Poco::JSON::Object::Ptr & metadata_object,
        IcebergMetadataFilesCachePtr cache_ptr);

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

    bool hasDataTransformer() const override { return !getPositionDeletesFiles().empty(); }

    std::shared_ptr<ISimpleTransform> getDataTransformer(
        const ObjectInfoPtr & /* object_info */,
        const Block & /* header */,
        const ConfigurationPtr /* configuration_ */,
        const std::optional<FormatSettings> & /* format_settings */,
        ContextPtr /* context */) const override;

    bool supportsSchemaEvolution() const override { return true; }

    static Int32
    parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    std::optional<size_t> totalRows() const override;
    std::optional<size_t> totalBytes() const override;

protected:
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size) const override;

private:
    using ManifestEntryByDataFile = std::unordered_map<String, Iceberg::ManifestFilePtr>;

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    IcebergMetadataFilesCachePtr manifest_cache;
    mutable ManifestEntryByDataFile manifest_file_by_data_file;

    std::tuple<Int64, Int32> getVersion() const { return std::make_tuple(relevant_snapshot_id, relevant_snapshot_schema_id); }

    Int32 last_metadata_version;
    Poco::JSON::Object::Ptr last_metadata_object;
    Int32 format_version;


    Int32 relevant_snapshot_schema_id;
    std::optional<Iceberg::IcebergSnapshot> relevant_snapshot;
    Int64 relevant_snapshot_id{-1};
    String table_location;

    mutable std::optional<std::vector<Iceberg::ManifestFileEntry>> cached_unprunned_files_for_last_processed_snapshot;
    mutable std::optional<std::vector<Iceberg::ManifestFileEntry>> cached_unprunned_position_deletes_files_for_last_processed_snapshot;

    void updateState(const ContextPtr & local_context, bool metadata_file_changed);

    std::vector<Iceberg::ManifestFileEntry> getDataFiles(const ActionsDAG * filter_dag) const;

    std::vector<Iceberg::ManifestFileEntry> getPositionDeletesFiles() const;

    void updateSnapshot();

    Iceberg::ManifestListPtr getManifestList(const String & filename) const;
    mutable std::vector<Iceberg::ManifestFileEntry> positional_delete_files_for_current_query;

    void addTableSchemaById(Int32 schema_id);

    std::optional<Int32> getSchemaVersionByFileIfOutdated(String data_path) const;

    void initializeDataFiles(Iceberg::ManifestListPtr manifest_list_ptr) const;

    Iceberg::ManifestFilePtr getManifestFile(const String & filename, Int64 inherited_sequence_number) const;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);

    Strings getDataFilesImpl(const ActionsDAG * filter_dag) const;

    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;
};
}

#endif
