#pragma once
#include "Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h"
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
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataFileEntry.h>
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

    ~IcebergMetadata() override;

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
        const Poco::JSON::Object::Ptr & metadata_object,
        Iceberg::IcebergSchemaProcessor & schema_processor,
        ContextPtr context_,
        LoggerPtr metadata_logger);

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

    bool supportsImport(ContextPtr) const override { return true; }

    SinkToStoragePtr import(
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::function<void(const std::string &)> & new_file_path_callback,
        SharedHeader sample_block,
        const std::string & iceberg_metadata_json_string,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context) override;

    /// Commit an export-partition transaction. All parameters that are saved in ZooKeeper at the
    /// start of the export operation (schema_id, partition_spec_id, partition_values,
    /// partition_columns, partition_types) must be provided by the caller.
    /// The partition spec object is derived from the metadata using partition_spec_id.
    /// If the live metadata has diverged (schema or partition spec changed) the call throws
    /// immediately — the caller must restart from scratch.
    ///
    /// data_file_paths contains the metadata-path for each exported data file (as recorded in
    /// ZooKeeper).  For every path a co-located sidecar Avro file (same path, ".avro" extension)
    /// must exist in the object storage; it supplies record_count and file_size_in_bytes.
    void commitExportPartitionTransaction(
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id,
        const String & transaction_id,
        Int64 original_schema_id,
        Int64 partition_spec_id,
        const std::vector<Field> & partition_values,
        SharedHeader sample_block,
        const std::vector<String> & data_file_paths,
        StorageObjectStorageConfigurationPtr configuration,
        ContextPtr context) override;

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

    Pipe executeCommand(
        const String & command_name,
        const ASTPtr & args,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        std::shared_ptr<DataLake::ICatalog> catalog,
        ContextPtr context,
        const StorageID & storage_id) override;

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr local_context) const override;

    void drop(ContextPtr context) override;

    std::optional<String> partitionKey(ContextPtr) const override;
    std::optional<String> sortingKey(ContextPtr) const override;

    Poco::JSON::Object::Ptr getMetadataJSON(ContextPtr local_context) const;

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
    std::pair<Iceberg::IcebergDataSnapshotPtr, Iceberg::TableStateSnapshot> getRelevantState(const ContextPtr & context, bool force_fetch_latest_metadata = false) const;

    std::optional<String> getPartitionKey(ContextPtr local_context, Iceberg::TableStateSnapshot actual_table_state_snapshot) const;
    KeyDescription getSortingKey(ContextPtr local_context, Iceberg::TableStateSnapshot actual_table_state_snapshot) const;

    bool commitImportPartitionTransactionImpl(
        FileNamesGenerator & filename_generator,
        Poco::JSON::Object::Ptr & metadata,
        Poco::JSON::Object::Ptr & partition_spec,
        const String & transaction_id,
        Int64 original_schema_id,
        Int64 partition_spec_id,
        const std::vector<Field> & partition_values,
        const std::vector<String> & partition_columns,
        const std::vector<DataTypePtr> & partition_types,
        SharedHeader sample_block,
        const std::vector<String> & data_file_paths,
        const std::vector<IcebergSerializedFileStats> & per_file_stats,
        Int64 total_data_files,
        Int64 total_rows,
        Int64 total_chunks_size,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id,
        const String & blob_storage_type_name,
        const String & blob_storage_namespace_name,
        ContextPtr context);

    LoggerPtr log;
    const ObjectStoragePtr object_storage;
    DB::Iceberg::PersistentTableComponents persistent_components;
    const DataLakeStorageSettings & data_lake_settings;
    const String write_format;
    BackgroundSchedulePoolTaskHolder background_metadata_prefetch_task;

    void backgroundMetadataPrefetcherThread();
};
}

#endif
