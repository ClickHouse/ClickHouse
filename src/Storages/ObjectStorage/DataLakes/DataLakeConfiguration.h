#pragma once
#include "config.h"

#include <Storages/IStorage.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <memory>
#include <string>

#include <Common/ErrorCodes.h>
#include <Common/filesystemHelpers.h>
#include <Disks/DiskType.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/DataLake/GlueCatalog.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
}

namespace DataLakeStorageSetting
{
    extern DataLakeStorageSettingsDatabaseDataLakeCatalogType storage_catalog_type;
    extern DataLakeStorageSettingsString object_storage_endpoint;
    extern DataLakeStorageSettingsString storage_aws_access_key_id;
    extern DataLakeStorageSettingsString storage_aws_secret_access_key;
    extern DataLakeStorageSettingsString storage_region;
    extern DataLakeStorageSettingsString storage_catalog_url;
    extern DataLakeStorageSettingsString storage_warehouse;
    extern DataLakeStorageSettingsString storage_catalog_credential;

    extern DataLakeStorageSettingsString storage_auth_scope;
    extern DataLakeStorageSettingsString storage_auth_header;
    extern DataLakeStorageSettingsString storage_oauth_server_uri;
    extern DataLakeStorageSettingsBool storage_oauth_server_use_request_body;
}

template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorageConfiguration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorageConfiguration>
{
public:
    explicit DataLakeConfiguration(DataLakeStorageSettingsPtr settings_) : settings(settings_) {}

    bool isDataLakeConfiguration() const override { return true; }

    const DataLakeStorageSettings & getDataLakeSettings() const override { return *settings; }

    std::string getEngineName() const override { return DataLakeMetadata::name + BaseStorageConfiguration::getEngineName(); }

    StorageObjectStorageConfiguration::Path getRawPath() const override
    {
        auto result = BaseStorageConfiguration::getRawPath().path;
        return StorageObjectStorageConfiguration::Path(result.ends_with('/') ? result : result + "/");
    }

    /// Returns true, if metadata is of the latest version, false if unknown.
    void update(ObjectStoragePtr object_storage, ContextPtr local_context, bool if_not_updated_before) override
    {
        const bool updated_before = current_metadata != nullptr;
        if (updated_before && if_not_updated_before)
            return;

        BaseStorageConfiguration::update(object_storage, local_context, if_not_updated_before);
        if (current_metadata && current_metadata->supportsUpdate())
        {
            current_metadata->update(local_context);
            return;
        }
        current_metadata = DataLakeMetadata::create(object_storage, weak_from_this(), local_context);
    }

    void create(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_) override
    {
        if (object_storage->getType() == ObjectStorageType::Local)
        {
            auto user_files_path = local_context->getUserFilesPath();
            if (!fileOrSymlinkPathStartsWith(this->getPathForRead().path, user_files_path))
                throw Exception(
                    ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", this->getPathForRead().path, user_files_path);
        }
        BaseStorageConfiguration::update(object_storage, local_context, true);

        DataLakeMetadata::createInitial(
            object_storage, weak_from_this(), local_context, columns, partition_by, if_not_exists, catalog, table_id_);
    }

    bool supportsDelete() const override
    {
        assertInitialized();
        return current_metadata->supportsDelete();
    }

    bool supportsParallelInsert() const override
    {
        assertInitialized();
        return current_metadata->supportsParallelInsert();
    }

    void mutate(const MutationCommands & commands,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings) override
    {
        assertInitialized();
        current_metadata->mutate(commands, context, storage_id, metadata_snapshot, catalog, format_settings);
    }

    void checkMutationIsPossible(const MutationCommands & commands) override
    {
        assertInitialized();
        current_metadata->checkMutationIsPossible(commands);
    }

    void checkAlterIsPossible(const AlterCommands & commands) override
    {
        assertInitialized();
        current_metadata->checkAlterIsPossible(commands);
    }

    void alter(const AlterCommands & params, ContextPtr context) override
    {
        assertInitialized();
        current_metadata->alter(params, context);

    }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override
    {
        if (ready_object_storage)
            return ready_object_storage;
        return BaseStorageConfiguration::createObjectStorage(context, is_readonly);
    }

    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata(ContextPtr local_context) const override
    {
        assertInitialized();
        if (auto schema = current_metadata->getTableSchema(local_context); !schema.empty())
            return ColumnsDescription(std::move(schema));
        return std::nullopt;
    }

    std::optional<size_t> totalRows(ContextPtr local_context) override
    {
        assertInitialized();
        return current_metadata->totalRows(local_context);
    }

    std::optional<size_t> totalBytes(ContextPtr local_context) override
    {
        assertInitialized();
        return current_metadata->totalBytes(local_context);
    }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const override
    {
        assertInitialized();
        return current_metadata->getInitialSchemaByPath(local_context, object_info);
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, ObjectInfoPtr object_info) const override
    {
        assertInitialized();
        return current_metadata->getSchemaTransformer(local_context, object_info);
    }

    StorageInMemoryMetadata getStorageSnapshotMetadata(ContextPtr context) const override
    {
        assertInitialized();
        return current_metadata->getStorageSnapshotMetadata(context);
    }

    /// This method should work even if metadata is not initialized
    bool needsUpdateForSchemaConsistency() const override
    {
#if USE_AVRO
        return std::is_same_v<IcebergMetadata, DataLakeMetadata>;
#endif
        return false;
    }

    IDataLakeMetadata * getExternalMetadata() override
    {
        assertInitialized();
        return current_metadata.get();
    }

    bool supportsFileIterator() const override { return true; }

    bool supportsWrites() const override
    {
        assertInitialized();
        return current_metadata->supportsWrites();
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr context) override
    {
        assertInitialized();
        return current_metadata->iterate(filter_dag, callback, list_batch_size, storage_metadata, context);
    }

#if USE_PARQUET
    /// This is an awful temporary crutch,
    /// which will be removed once DeltaKernel is used by default for DeltaLake.
    /// By release 25.3.
    /// (Because it does not make sense to support it in a nice way
    /// because the code will be removed ASAP anyway)
    DeltaLakePartitionColumns getDeltaLakePartitionColumns() const
    {
        assertInitialized();
        const auto * delta_lake_metadata = dynamic_cast<const DeltaLakeMetadata *>(current_metadata.get());
        if (delta_lake_metadata)
            return delta_lake_metadata->getPartitionColumns();
        return {};
    }
#endif

    void modifyFormatSettings(FormatSettings & settings_, const Context & local_context) const override
    {
        assertInitialized();
        current_metadata->modifyFormatSettings(settings_, local_context);
    }

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const override
    {
        assertInitialized();
        return current_metadata->getColumnMapperForObject(object_info);
    }
    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const override
    {
        assertInitialized();
        return current_metadata->getColumnMapperForCurrentSchema(storage_metadata_snapshot, context);
    }

    void drop(ContextPtr local_context) override
    {
        if (current_metadata)
            current_metadata->drop(local_context);
    }

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override
    {
        return current_metadata->write(
            sample_block,
            table_id,
            object_storage,
            shared_from_this(),
            format_settings.has_value() ? *format_settings : FormatSettings{},
            context,
            catalog);
    }

    std::shared_ptr<DataLake::ICatalog> getCatalog([[maybe_unused]] ContextPtr context, [[maybe_unused]] bool is_attach) const override
    {
#if USE_AWS_S3 && USE_AVRO
        if ((*settings)[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::GLUE)
        {
            auto catalog_parameters = DataLake::CatalogSettings{
                .storage_endpoint = (*settings)[DataLakeStorageSetting::object_storage_endpoint].value,
                .aws_access_key_id = (*settings)[DataLakeStorageSetting::storage_aws_access_key_id].value,
                .aws_secret_access_key = (*settings)[DataLakeStorageSetting::storage_aws_secret_access_key].value,
                .region = (*settings)[DataLakeStorageSetting::storage_region].value,
            };

            return std::make_shared<DataLake::GlueCatalog>(
                (*settings)[DataLakeStorageSetting::storage_catalog_url].value,
                context,
                catalog_parameters,
                /* table_engine_definition */nullptr
            );
        }
        /// Attach condition is provided for compatibility.
        if ((*settings)[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::ICEBERG_REST ||
            (is_attach && (*settings)[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::NONE && !(*settings)[DataLakeStorageSetting::storage_catalog_url].value.empty()))
        {
            return std::make_shared<DataLake::RestCatalog>(
                (*settings)[DataLakeStorageSetting::storage_warehouse].value,
                (*settings)[DataLakeStorageSetting::storage_catalog_url].value,
                (*settings)[DataLakeStorageSetting::storage_catalog_credential].value,
                (*settings)[DataLakeStorageSetting::storage_auth_scope].value,
                (*settings)[DataLakeStorageSetting::storage_auth_header],
                (*settings)[DataLakeStorageSetting::storage_oauth_server_uri].value,
                (*settings)[DataLakeStorageSetting::storage_oauth_server_use_request_body].value,
                context);
        }

#endif
        return nullptr;
    }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings) override
    {
        assertInitialized();
        return current_metadata->optimize(metadata_snapshot, context, format_settings);
    }

    void addDeleteTransformers(ObjectInfoPtr object_info, QueryPipelineBuilder & builder, const std::optional<FormatSettings> & format_settings, ContextPtr local_context) const override
    {
        current_metadata->addDeleteTransformers(object_info, builder, format_settings, local_context);
    }

    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override
    {
        if (!Context::getGlobalContextInstance()->getAllowedDisksForTableEngines().contains(disk_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} is not allowed for usage in storage engines. The list of allowed disks is defined by `allowed_disks_for_table_engines", disk_name);

        BaseStorageConfiguration::fromDisk(disk_name, args, context, with_structure);
        auto disk = context->getDisk(disk_name);
        ready_object_storage = disk->getObjectStorage();
    }

private:
    DataLakeMetadataPtr current_metadata;
    LoggerPtr log = getLogger("DataLakeConfiguration");
    const DataLakeStorageSettingsPtr settings;
    ObjectStoragePtr ready_object_storage;

    void assertInitialized() const
    {
        if (!current_metadata)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata is not initialized");
    }

    ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        bool supports_tuple_elements,
        ContextPtr local_context,
        const PrepareReadingFromFormatHiveParams &) override
    {
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(
                object_storage,
                weak_from_this(),
                local_context);
        }
        return current_metadata->prepareReadingFromFormat(
            requested_columns, storage_snapshot, local_context, supports_subset_of_columns, supports_tuple_elements);
    }
};


#if USE_AVRO
#    if USE_AWS_S3
using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
using StorageS3PaimonConfiguration = DataLakeConfiguration<StorageS3Configuration, PaimonMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
using StorageAzurePaimonConfiguration = DataLakeConfiguration<StorageAzureConfiguration, PaimonMetadata>;
#endif

#if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
using StorageHDFSPaimonConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, PaimonMetadata>;
#endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;
using StorageLocalPaimonConfiguration = DataLakeConfiguration<StorageLocalConfiguration, PaimonMetadata>;
#endif

#if USE_PARQUET
#if USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureDeltaLakeConfiguration = DataLakeConfiguration<StorageAzureConfiguration, DeltaLakeMetadata>;
#endif

using StorageLocalDeltaLakeConfiguration = DataLakeConfiguration<StorageLocalConfiguration, DeltaLakeMetadata>;

#endif

#if USE_AWS_S3
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;
#endif
}
