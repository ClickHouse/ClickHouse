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

#include <tuple>
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

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const ContextPtr & context_,
        IcebergMetadataFilesCachePtr cache_ptr);

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override;

    bool operator==(const IDataLakeMetadata & /*other*/) const override { return false; }

    static void createInitial(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_);

    static DataLakeMetadataPtr create(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationWeakPtr & configuration,
        const ContextPtr & local_context);

    bool supportsSchemaEvolution() const override { return true; }

    static Int32 parseTableSchema(
        const Poco::JSON::Object::Ptr & metadata_object, Iceberg::IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }
    bool supportsWrites() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    IcebergHistory getHistory(ContextPtr local_context) const;

    std::optional<size_t> updateConfigurationAndGetTotalRows(ContextPtr Local_context) const override;
    std::optional<size_t> updateConfigurationAndGetTotalBytes(ContextPtr Local_context) const override;

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const override;
    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, ObjectInfoPtr object_info) const override;

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const override;

    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageSnapshotPtr storage_snapshot, ContextPtr context) const override;
    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override;

    CompressionMethod getCompressionMethod() const { return persistent_components.metadata_compression_method; }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings) override;
    bool supportsDelete() const override { return true; }
    void mutate(const MutationCommands & commands,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings) override;

    void checkMutationIsPossible(const MutationCommands & commands) override;

    bool needUpdateOnReadWrite() const override { return false; }

    void addDeleteTransformers(ObjectInfoPtr object_info, QueryPipelineBuilder & builder, const std::optional<FormatSettings> & format_settings, ContextPtr local_context) const override;
    void checkAlterIsPossible(const AlterCommands & commands) override;
    void alter(const AlterCommands & params, ContextPtr context) override;

    void sendTemporaryStateToStorageSnapshot(StorageSnapshotPtr storage_snapshot) override
    {
        LOG_DEBUG(log, "Releasing to snapshot, stacktrace: {}", StackTrace().toString());
        std::optional<Iceberg::IcebergTableStateSnapshot> snapshot_data = last_table_state_snapshot.get();
        if (storage_snapshot)
        {
            if (!snapshot_data)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg state should be initialized when creating a storage snapshot");
            }
            storage_snapshot->data = std::make_unique<Iceberg::IcebergSpecificSnapshotData>(std::move(snapshot_data.value()));
        }
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr local_context) const override;

private:
    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationWeakPtr configuration;
    LoggerPtr log;

    DB::Iceberg::PersistentTableComponents persistent_components;


    Iceberg::OneThreadProtecting<Iceberg::IcebergTableStateSnapshot> last_table_state_snapshot;

    Iceberg::PersistentTableComponents initializePersistentTableComponents(Poco::JSON::Object::Ptr metadata_object);

    Iceberg::IcebergDataSnapshotPtr
    getIcebergDataSnapshot(Poco::JSON::Object::Ptr metadata_object, Int64 snapshot_id, ContextPtr local_context) const;
    Iceberg::IcebergDataSnapshotPtr
    createIcebergDataSnapshotFromSnapshotJSON(Poco::JSON::Object::Ptr snapshot_object, Int64 snapshot_id, ContextPtr local_context) const;
    std::pair<Iceberg::IcebergDataSnapshotPtr, Int32>
    getStateImpl(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object) const;
    std::pair<Iceberg::IcebergDataSnapshotPtr, Iceberg::IcebergTableStateSnapshot>
    getState(const ContextPtr & local_context, String metadata_path, Int32 metadata_version) const;
    Iceberg::IcebergDataSnapshotPtr
    getRelevantDataSnapshotFromTableStateSnapshot(Iceberg::IcebergTableStateSnapshot table_state_snapshot, ContextPtr local_context) const;
};
}

#endif
