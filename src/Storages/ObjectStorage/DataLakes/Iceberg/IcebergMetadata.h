#pragma once
#include "config.h"

#if USE_AVRO

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#    include <optional>
#    include <tuple>
#    include <base/defines.h>
#    include <Common/SharedMutex.h>

#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

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
        Int32 format_version_,
        IcebergMetadataFilesCachePtr cache_ptr,
        String table_location_);

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override;

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

    bool hasPositionDeleteTransformer(const ObjectInfoPtr & object_info) const override;

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const ObjectInfoPtr & /* object_info */,
        const SharedHeader & /* header */,
        const std::optional<FormatSettings> & /* format_settings */,
        ContextPtr /* context */) const override;

    bool supportsSchemaEvolution() const override { return true; }

    static Int32 parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

    bool supportsUpdate() const override { return true; }
    bool supportsWrites() const override { return true; }

    bool update(const ContextPtr & local_context) override;

    IcebergHistory getHistory(ContextPtr local_context) const;

    std::optional<size_t> totalRows(ContextPtr Local_context) const override;
    std::optional<size_t> totalBytes(ContextPtr Local_context) const override;

protected:
    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        ContextPtr local_context) const override;

private:
    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationWeakPtr configuration;
    mutable std::shared_ptr<IcebergSchemaProcessor> schema_processor;
    IcebergMetadataFilesCachePtr iceberg_metadata_cache;
    const Int32 format_version;
    const String table_location;
    LoggerPtr log;

    Iceberg::IcebergTableStateSnapshot & getRelevantSnapshot() const { return *relevant_snapshot; }

    mutable SharedMutex mutex;

    Iceberg::IcebergTableStateSnapshotPtr relevant_snapshot{};

    ColumnMapperPtr initializeColumnMapper(
        Poco::JSON::Array::Ptr schemas, Poco::JSON::Object::Ptr snapshot, StorageObjectStorageConfigurationPtr configuration_ptr) const;


    Iceberg::IcebergDataSnapshotPtr getDataSnapshot(ContextPtr local_context, Poco::JSON::Object::Ptr metadata_object);
    void updateState(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object) TSA_REQUIRES(mutex);
    void getSnapshot(ContextPtr local_context, Poco::JSON::Object::Ptr metadata_object) TSA_REQUIRES(mutex);
    ManifestFileCacheKeys getManifestList(ContextPtr local_context, const String & filename) const;
    void addTableSchemaById(Int32 schema_id, Poco::JSON::Object::Ptr metadata_object) TSA_REQUIRES(mutex);

    template <typename T>
    std::vector<T> getFilesImpl(
        const ActionsDAG * filter_dag,
        Iceberg::FileContentType file_content_type,
        ContextPtr local_context,
        std::function<T(const Iceberg::ManifestFileEntry &)> transform_function) const;
};
}

#endif
