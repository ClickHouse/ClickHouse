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

class IcebergMetadata;
using IcebergMetadataPtr = std::shared_ptr<IcebergMetadata>;

class IcebergMetadata
{
public:
    using IcebergHistory = std::vector<Iceberg::IcebergHistoryRecord>;

    static constexpr auto name = "Iceberg";

    const char * getName() const { return name; }

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const ContextPtr & context_,
        IcebergMetadataFilesCachePtr cache_ptr);

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema(ContextPtr local_context) const;

    StorageInMemoryMetadata getStorageSnapshotMetadata(ContextPtr local_context) const;

    bool operator==(const IDataLakeMetadata & /*other*/) const { return false; }

    static void createInitial(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_);

    static IcebergMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const;
    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, ObjectInfoPtr object_info) const;

    static Int32 parseTableSchema(
        const Poco::JSON::Object::Ptr & metadata_object, Iceberg::IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const { return true; }
    bool supportsWrites() const { return true; }
    bool supportsParallelInsert() const { return true; }

    IcebergHistory getHistory(ContextPtr local_context) const;

    std::optional<size_t> totalRows(ContextPtr Local_context) const;
    std::optional<size_t> totalBytes(ContextPtr Local_context) const;

    bool isDataSortedBySortingKey(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const;

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const;

    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const;

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog);

    CompressionMethod getCompressionMethod() const { return persistent_components.metadata_compression_method; }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings);
    bool supportsDelete() const { return true; }
    void mutate(
        const MutationCommands & commands,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings);

    void checkMutationIsPossible(const MutationCommands & commands) const;

    void modifyFormatSettings(FormatSettings & format_settings, const Context & local_context) const;
    void addDeleteTransformers(
        ObjectInfoPtr object_info,
        QueryPipelineBuilder & builder,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr local_context) const;
    void checkAlterIsPossible(const AlterCommands & commands) const;
    void alter(const AlterCommands & params, ContextPtr context);

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr local_context) const;

    void drop(ContextPtr context);

private:
    Iceberg::PersistentTableComponents initializePersistentTableComponents(
        IcebergMetadataFilesCachePtr cache_ptr,
        ContextPtr context_);

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
    StorageObjectStorageConfigurationPtr getConfiguration() const;


    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationWeakPtr configuration;
    LoggerPtr log;
    DB::Iceberg::PersistentTableComponents persistent_components;
    KeyDescription getSortingKey(ContextPtr local_context, Iceberg::TableStateSnapshot actual_table_state_snapshot) const;
};
}

#endif
