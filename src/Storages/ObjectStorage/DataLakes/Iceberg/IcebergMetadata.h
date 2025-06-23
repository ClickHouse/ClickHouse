#pragma once
#include <algorithm>
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

class IcebergMetadata : public IDataLakeMetadata
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;
    using IcebergHistory = std::vector<Iceberg::IcebergHistoryRecord>;

    static constexpr auto name = "Iceberg";

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationObserverPtr configuration_,
        const ContextPtr & context_,
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

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(local_context, data_path);
        return version_if_outdated.has_value() ? schema_processor.getClickhouseTableSchemaById(version_if_outdated.value()) : nullptr;
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(local_context, data_path);
        return version_if_outdated.has_value()
            ? schema_processor.getSchemaTransformationDagByIds(version_if_outdated.value(), relevant_snapshot_schema_id)
            : nullptr;
    }

    bool hasDataTransformer(const ObjectInfoPtr & object_info) const override;

    std::shared_ptr<ISimpleTransform> getDataTransformer(
        const ObjectInfoPtr & /* object_info */,
        const Block & /* header */,
        const std::optional<FormatSettings> & /* format_settings */,
        ContextPtr /* context */) const override;

    bool supportsSchemaEvolution() const override { return true; }

    static Int32
    parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    IcebergHistory getHistory(ContextPtr local_context) const;

    std::optional<size_t> totalRows(ContextPtr Local_context) const override;
    std::optional<size_t> totalBytes(ContextPtr Local_context) const override;


    friend class IcebergKeysIterator;
    friend struct IcebergDataObjectInfo;

protected:
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        ContextPtr local_context) const override;

private:
    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    IcebergMetadataFilesCachePtr manifest_cache;

    std::tuple<Int64, Int32> getVersion() const { return std::make_tuple(relevant_snapshot_id, relevant_snapshot_schema_id); }

    Int32 last_metadata_version;
    Int32 format_version;

    mutable std::atomic<bool> schema_id_by_data_file_initialized{false};
    mutable std::unordered_map<String, Int32> schema_id_by_data_file;
    mutable std::mutex schema_id_by_data_file_mutex;


    Int32 relevant_snapshot_schema_id;
    std::optional<Iceberg::IcebergSnapshot> relevant_snapshot;
    Int64 relevant_snapshot_id{-1};
    String table_location;

    mutable std::optional<std::vector<Iceberg::ManifestFileEntry>> cached_unprunned_files_for_last_processed_snapshot;
    mutable std::optional<std::vector<Iceberg::ManifestFileEntry>> cached_unprunned_position_deletes_files_for_last_processed_snapshot;

    void updateState(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object, bool metadata_file_changed);

    std::vector<Iceberg::ManifestFileEntry> getDataFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const;

    std::vector<Iceberg::ManifestFileEntry> getPositionalDeleteFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const;

    void updateSnapshot(ContextPtr local_context, Poco::JSON::Object::Ptr metadata_object);

    ManifestFileCacheKeys getManifestList(ContextPtr local_context, const String & filename) const;
    mutable std::vector<Iceberg::ManifestFileEntry> positional_delete_files_for_current_query;

    void addTableSchemaById(Int32 schema_id, Poco::JSON::Object::Ptr metadata_object);

    std::optional<Int32> getSchemaVersionByFileIfOutdated(ContextPtr local_context, String data_path) const;

    void initializeSchemasFromManifestList(ContextPtr local_context, ManifestFileCacheKeys manifest_list_ptr) const;

    void initializeSchemasFromManifestFile(Iceberg::ManifestFilePtr manifest_file_ptr) const;

    Iceberg::ManifestFilePtr getManifestFile(ContextPtr local_context, const String & filename, Int64 inherited_sequence_number) const;

    std::optional<String> getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata);

    Iceberg::ManifestFilePtr tryGetManifestFile(const String & filename) const;

    std::vector<Iceberg::ManifestFileEntry> getFilesImpl(const ActionsDAG * filter_dag, Iceberg::FileContentType file_content_type, ContextPtr local_context) const;
};

struct IcebergDataObjectInfo : public RelativePathWithMetadata
{
    explicit IcebergDataObjectInfo(
        const IcebergMetadata & iceberg_metadata,
        Iceberg::ManifestFileEntry data_object_,
        std::optional<ObjectMetadata> metadata_ = std::nullopt,
        const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_ = {});

    const Iceberg::ManifestFileEntry data_object;
    std::span<const Iceberg::ManifestFileEntry> position_deletes_objects;

    // Return the path in the Iceberg metadata
    std::string getIcebergDataPath() const { return data_object.file_path_key; }
};
using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

class IcebergKeysIterator : public IObjectIterator
{
public:
    IcebergKeysIterator(
        const IcebergMetadata & iceberg_metadata_,
        std::vector<Iceberg::ManifestFileEntry>&& data_files_,
        std::vector<Iceberg::ManifestFileEntry>&& position_deletes_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_);

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    ObjectInfoPtr next(size_t) override;

private:
    const IcebergMetadata & iceberg_metadata;
    std::vector<Iceberg::ManifestFileEntry> data_files;
    std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;
};

}

#endif
