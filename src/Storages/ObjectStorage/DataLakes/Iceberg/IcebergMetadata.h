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

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>

namespace DB
{

class IcebergMetadata;
class IcebergMetadata : public IDataLakeMetadata
{
public:
    using IcebergHistory = std::vector<Iceberg::IcebergHistoryRecord>;

    static constexpr auto name = "Iceberg";

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        const ContextPtr & context_,
        Int32 metadata_version_,
        Int32 format_version_,
        const Poco::JSON::Object::Ptr & metadata_object,
        IcebergMetadataFilesCachePtr cache_ptr,
        CompressionMethod metadata_compression_method_);

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override;

    bool operator==(const IDataLakeMetadata & other) const override
    {
        const auto * iceberg_metadata = dynamic_cast<const IcebergMetadata *>(&other);
        return iceberg_metadata && getVersion() == iceberg_metadata->getVersion();
    }

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

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const override;
    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, ObjectInfoPtr object_info) const override;

    bool supportsSchemaEvolution() const override { return true; }

    static Int32 parseTableSchema(
        const Poco::JSON::Object::Ptr & metadata_object, Iceberg::IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }
    bool supportsWrites() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    IcebergHistory getHistory(ContextPtr local_context) const;

    std::optional<size_t> totalRows(ContextPtr Local_context) const override;
    std::optional<size_t> totalBytes(ContextPtr Local_context) const override;

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const override;

    ColumnMapperPtr getColumnMapperForCurrentSchema() const override;
    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override;

    CompressionMethod getCompressionMethod() const { return metadata_compression_method; }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings) override;
    bool supportsDelete() const override { return true; }
    void mutate(const MutationCommands & commands,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings) override;

    void checkMutationIsPossible(const MutationCommands & commands) override;

    void addDeleteTransformers(ObjectInfoPtr object_info, QueryPipelineBuilder & builder, const std::optional<FormatSettings> & format_settings, ContextPtr local_context) const override;
    void checkAlterIsPossible(const AlterCommands & commands) override;
    void alter(const AlterCommands & params, ContextPtr context) override;
    void drop(ContextPtr context) override;
protected:
    ObjectIterator
    iterate(const ActionsDAG * filter_dag, FileProgressCallback callback, size_t list_batch_size, ContextPtr local_context) const override;

private:
    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationWeakPtr configuration;

    DB::Iceberg::PersistentTableComponents persistent_components;

    LoggerPtr log;


    std::tuple<Int64, Int32> getVersion() const;

    mutable SharedMutex mutex;

    Int32 last_metadata_version TSA_GUARDED_BY(mutex);

    Int32 relevant_snapshot_schema_id TSA_GUARDED_BY(mutex);
    Iceberg::IcebergDataSnapshotPtr relevant_snapshot TSA_GUARDED_BY(mutex);
    Int64 relevant_snapshot_id TSA_GUARDED_BY(mutex) {-1};
    CompressionMethod metadata_compression_method;

    void updateState(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object) TSA_REQUIRES(mutex);
    void updateSnapshot(ContextPtr local_context, Poco::JSON::Object::Ptr metadata_object) TSA_REQUIRES(mutex);
    void addTableSchemaById(Int32 schema_id, Poco::JSON::Object::Ptr metadata_object) const TSA_REQUIRES(mutex);
    std::optional<Int32> getSchemaVersionByFileIfOutdated(String data_path) const TSA_REQUIRES_SHARED(mutex);
    void initializeSchemasFromManifestList(ContextPtr local_context, ManifestFileCacheKeys manifest_list_ptr) const TSA_REQUIRES(mutex);
};
}

#endif
