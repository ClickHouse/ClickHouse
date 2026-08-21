#pragma once
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#include <optional>
#include <base/defines.h>

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace DB
{

class IcebergMetadata : public IDataLakeMetadata
{
public:
    using IcebergHistory = std::vector<Iceberg::IcebergHistoryRecord>;

    static constexpr auto name = "Iceberg";

    const char * getName() const override { return name; }

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const ContextPtr & context_,
        IcebergMetadataFilesCachePtr cache_ptr);

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema(ContextPtr local_context) const override;

    std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr local_context) const override;
    std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(const DataLakeTableStateSnapshot &, ContextPtr local_context) const override;
    bool shouldReloadSchemaForConsistency(ContextPtr local_context) const override;

    bool operator==(const IDataLakeMetadata & /*other*/) const override { return false; }

    static void createInitial(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        ASTPtr order_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_);

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const override;
    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, ObjectInfoPtr object_info) const override;

    static Int32 parseTableSchema(
        const Poco::JSON::Object::Ptr & metadata_object, Iceberg::IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }
    bool supportsWrites() const override { return true; }
    bool supportsParallelInsert() const override { return true; }

    IcebergHistory getHistory(ContextPtr local_context) const;

    static bool supportsTotalRows(ContextPtr, ObjectStorageType) { return true; }
    std::optional<size_t> totalRows(ContextPtr Local_context) const override;
    static bool supportsTotalBytes(ContextPtr, ObjectStorageType) { return true; }
    std::optional<size_t> totalBytes(ContextPtr Local_context) const override;

    bool isDataSortedBySortingKey(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const override;

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const override;

    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const override;

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr /*configuration*/,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override;

    CompressionMethod getCompressionMethod() const { return persistent_components.metadata_compression_method; }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings) override;
    bool supportsDelete() const override { return true; }
    void mutate(
        const MutationCommands & commands,
        StorageObjectStorageConfigurationPtr configuration,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings) override;

    void checkMutationIsPossible(const MutationCommands & commands) override;

    void modifyFormatSettings(FormatSettings & format_settings, const Context & local_context) const override;
    void addDeleteTransformers(ObjectInfoPtr object_info, QueryPipelineBuilder & builder, const std::optional<FormatSettings> & format_settings, FormatParserSharedResourcesPtr parser_shared_resources, ContextPtr local_context) const override;
    void checkAlterIsPossible(const AlterCommands & commands) override;
    void alter(const AlterCommands & params, ContextPtr context) override;

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr local_context) const override;

    void drop(ContextPtr context) override;

private:
    Iceberg::PersistentTableComponents initializePersistentTableComponents(
        StorageObjectStorageConfigurationPtr configuration, IcebergMetadataFilesCachePtr cache_ptr, ContextPtr context_);

    Iceberg::IcebergDataSnapshotPtr
    getIcebergDataSnapshot(Poco::JSON::Object::Ptr metadata_object, Int64 snapshot_id, ContextPtr local_context) const;

    Iceberg::IcebergDataSnapshotPtr createIcebergDataSnapshotFromSnapshotJSON(Poco::JSON::Object::Ptr snapshot_object, Int64 snapshot_id, ContextPtr local_context) const;
    std::pair<Iceberg::IcebergDataSnapshotPtr, Int32>
    getStateImpl(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object) const;
    std::pair<Iceberg::IcebergDataSnapshotPtr, Iceberg::TableStateSnapshot>
    getState(const ContextPtr & local_context, const String & metadata_path, Int32 metadata_version) const;
    Iceberg::IcebergDataSnapshotPtr
    getRelevantDataSnapshotFromTableStateSnapshot(Iceberg::TableStateSnapshot table_state_snapshot, ContextPtr local_context) const;
    std::pair<Iceberg::IcebergDataSnapshotPtr, Iceberg::TableStateSnapshot> getRelevantState(const ContextPtr & context) const;

    LoggerPtr log;
    const ObjectStoragePtr object_storage;
    DB::Iceberg::PersistentTableComponents persistent_components;
    const DataLakeStorageSettings & data_lake_settings;
    const String write_format;
    KeyDescription getSortingKey(ContextPtr local_context, Iceberg::TableStateSnapshot actual_table_state_snapshot) const;
};
}

#endif
