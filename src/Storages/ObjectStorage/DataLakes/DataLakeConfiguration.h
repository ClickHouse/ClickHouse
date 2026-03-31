#pragma once

#include "config.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>

#include <Storages/IStorage.h>
#include <Storages/NamedCollectionsHelpers.h>
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
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Disks/DiskType.h>

#include <memory>
#include <string>
#include <type_traits>

#include <Common/ErrorCodes.h>
#include <Common/filesystemHelpers.h>
#include <Disks/DiskType.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/DataLake/GlueCatalog.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
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
    extern const DataLakeStorageSettingsDatabaseDataLakeCatalogType storage_catalog_type;
    extern const DataLakeStorageSettingsString object_storage_endpoint;
    extern const DataLakeStorageSettingsString storage_aws_access_key_id;
    extern const DataLakeStorageSettingsString storage_aws_secret_access_key;
    extern const DataLakeStorageSettingsString storage_region;
    extern const DataLakeStorageSettingsString storage_aws_role_arn;
    extern const DataLakeStorageSettingsString storage_aws_role_session_name;
    extern const DataLakeStorageSettingsString storage_catalog_url;
    extern const DataLakeStorageSettingsString storage_warehouse;
    extern const DataLakeStorageSettingsString storage_catalog_credential;
    extern const DataLakeStorageSettingsString storage_auth_scope;
    extern const DataLakeStorageSettingsString storage_auth_header;
    extern const DataLakeStorageSettingsString storage_oauth_server_uri;
    extern const DataLakeStorageSettingsBool storage_oauth_server_use_request_body;
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
}

template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorageConfiguration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata, bool is_cluster_supported = true>
class DataLakeConfiguration : public BaseStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorageConfiguration>
{
public:
    DataLakeConfiguration() {}

    explicit DataLakeConfiguration(
        DataLakeStorageSettingsPtr settings_,
        std::optional<std::string> catalog_namespaces_ = std::nullopt)
        : settings(settings_)
        , catalog_namespaces(catalog_namespaces_.value_or("*")) {}

    bool isDataLakeConfiguration() const override { return true; }

    const DataLakeStorageSettings & getDataLakeSettings() const override { return *settings; }

    std::string getEngineName() const override { return DataLakeMetadata::name + BaseStorageConfiguration::getEngineName(); }

    StorageObjectStorageConfiguration::Path getRawPath() const override
    {
        auto result = BaseStorageConfiguration::getRawPath().path;
        return StorageObjectStorageConfiguration::Path(result.ends_with('/') ? result : result + "/");
    }
    void setRawPath(const StorageObjectStorageConfiguration::Path & path) override { BaseStorageConfiguration::setRawPath(path); }

    void update(ObjectStoragePtr object_storage, ContextPtr local_context) override
    {
        BaseStorageConfiguration::update(object_storage, local_context);
        assertLocalPathCorrect(object_storage, local_context);
        if (current_metadata && current_metadata->supportsUpdate())
        {
            current_metadata->update(local_context);
            return;
        }
        current_metadata = DataLakeMetadata::create(object_storage, weak_from_this(), local_context);
    }

    void lazyInitializeIfNeeded(ObjectStoragePtr object_storage, ContextPtr local_context) override
    {
        if (current_metadata != nullptr)
            return;
        BaseStorageConfiguration::update(object_storage, local_context);
        assertLocalPathCorrect(object_storage, local_context);
        current_metadata = DataLakeMetadata::create(object_storage, weak_from_this(), local_context);
    }

    void create(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        ASTPtr order_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_) override
    {
        BaseStorageConfiguration::update(object_storage, local_context);

        assertLocalPathCorrect(object_storage, local_context);
        DataLakeMetadata::createInitial(
            object_storage, weak_from_this(), local_context, columns, partition_by, order_by, if_not_exists, catalog, table_id_);
    }

    bool supportsDelete() const override
    {
        assertInitializedDL();
        return current_metadata->supportsDelete();
    }

    bool supportsParallelInsert() const override
    {
        assertInitializedDL();
        return current_metadata->supportsParallelInsert();
    }

    void mutate(const MutationCommands & commands,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings) override
    {
        assertInitializedDL();
        current_metadata->mutate(commands, shared_from_this(), context, storage_id, metadata_snapshot, catalog, format_settings);
    }

    void checkMutationIsPossible(const MutationCommands & commands) override
    {
        assertInitializedDL();
        current_metadata->checkMutationIsPossible(commands);
    }

    void checkAlterIsPossible(const AlterCommands & commands) override
    {
        assertInitializedDL();
        current_metadata->checkAlterIsPossible(commands);
    }

    void alter(const AlterCommands & params, ContextPtr context) override
    {
        assertInitializedDL();
        current_metadata->alter(params, context);

    }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, StorageObjectStorageConfiguration::CredentialsConfigurationCallback refresh_credentials_callback) override
    {
        if (ready_object_storage)
            return ready_object_storage;
        return BaseStorageConfiguration::createObjectStorage(context, is_readonly, refresh_credentials_callback);
    }

    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata(ContextPtr local_context) const override
    {
        assertInitializedDL();
        if (auto schema = current_metadata->getTableSchema(local_context); !schema.empty())
            return ColumnsDescription(std::move(schema));
        return std::nullopt;
    }

    bool supportsTotalRows(ContextPtr context, ObjectStorageType storage_type) const override
    {
        return DataLakeMetadata::supportsTotalRows(context, storage_type);
    }

    std::optional<size_t> totalRows(ContextPtr local_context) override
    {
        assertInitializedDL();
        return current_metadata->totalRows(local_context);
    }

    bool supportsTotalBytes(ContextPtr context, ObjectStorageType storage_type) const override
    {
        return DataLakeMetadata::supportsTotalBytes(context, storage_type);
    }

    std::optional<size_t> totalBytes(ContextPtr local_context) override
    {
        assertInitializedDL();
        return current_metadata->totalBytes(local_context);
    }

    bool isDataSortedBySortingKey(StorageMetadataPtr metadata_snapshot, ContextPtr local_context) const override
    {
        assertInitializedDL();
        return current_metadata->isDataSortedBySortingKey(metadata_snapshot, local_context);
    }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr local_context, ObjectInfoPtr object_info) const override
    {
        assertInitializedDL();
        return current_metadata->getInitialSchemaByPath(local_context, object_info);
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr local_context, ObjectInfoPtr object_info) const override
    {
        assertInitializedDL();
        return current_metadata->getSchemaTransformer(local_context, object_info);
    }

    std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr context) const override
    {
        assertInitializedDL();
        return current_metadata->getTableStateSnapshot(context);
    }

    std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(
        const DataLakeTableStateSnapshot & state, ContextPtr context) const override
    {
        assertInitializedDL();
        auto metadata = current_metadata->buildStorageMetadataFromState(state, context);
        if (metadata)
            LOG_TEST(log, "Built storage metadata from state with columns: {}",
                metadata->getColumns().toString(/* include_comments */false));
        return metadata;
    }

    bool shouldReloadSchemaForConsistency(ContextPtr context) const override
    {
        assertInitializedDL();
        return current_metadata->shouldReloadSchemaForConsistency(context);
    }

    IDataLakeMetadata * getExternalMetadata() override
    {
        assertInitializedDL();
        return current_metadata.get();
    }

    bool supportsFileIterator() const override { return true; }

    bool supportsWrites() const override
    {
        assertInitializedDL();
        return current_metadata->supportsWrites();
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr context) override
    {
        assertInitializedDL();
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
        assertInitializedDL();
        const auto * delta_lake_metadata = dynamic_cast<const DeltaLakeMetadata *>(current_metadata.get());
        if (delta_lake_metadata)
            return delta_lake_metadata->getPartitionColumns();
        return {};
    }
#endif

    void modifyFormatSettings(FormatSettings & settings_, const Context & local_context) const override
    {
        assertInitializedDL();
        current_metadata->modifyFormatSettings(settings_, local_context);
    }

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr object_info) const override
    {
        assertInitializedDL();
        return current_metadata->getColumnMapperForObject(object_info);
    }
    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const override
    {
        assertInitializedDL();
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
        lazyInitializeIfNeeded(object_storage, context);
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
                .namespaces = catalog_namespaces,
                .aws_role_arn = (*settings)[DataLakeStorageSetting::storage_aws_role_arn].value,
                .aws_role_session_name = (*settings)[DataLakeStorageSetting::storage_aws_role_session_name].value
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
                catalog_namespaces,
                context);
        }

#endif
        return nullptr;
    }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings) override
    {
        assertInitializedDL();
        return current_metadata->optimize(metadata_snapshot, context, format_settings);
    }

    void addDeleteTransformers(ObjectInfoPtr object_info, QueryPipelineBuilder & builder, const std::optional<FormatSettings> & format_settings, FormatParserSharedResourcesPtr parser_shared_resources, ContextPtr local_context) const override
    {
        current_metadata->addDeleteTransformers(object_info, builder, format_settings, parser_shared_resources, local_context);
    }

    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override
    {
        if (!Context::getGlobalContextInstance()->getAllowedDisksForTableEngines().contains(disk_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} is not allowed for usage in storage engines. The list of allowed disks is defined by `allowed_disks_for_table_engines", disk_name);

        BaseStorageConfiguration::fromDisk(disk_name, args, context, with_structure);
        auto disk = context->getDisk(disk_name);
        ready_object_storage = disk->getObjectStorage();
    }

    bool supportsPrewhere() const override
    {
#if USE_AVRO
        return std::is_same_v<DataLakeMetadata, IcebergMetadata>;
#else
        return false;
#endif
    }

    bool isClusterSupported() const override { return is_cluster_supported; }

    ASTPtr createArgsWithAccessData() const override
    {
        auto res = BaseStorageConfiguration::createArgsWithAccessData();

        auto iceberg_metadata_file_path = (*settings)[DataLakeStorageSetting::iceberg_metadata_file_path];

        if (iceberg_metadata_file_path.changed)
        {
            auto * arguments = res->template as<ASTExpressionList>();
            if (!arguments)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments are not an expression list");

            bool has_settings = false;

            for (auto & arg : arguments->children)
            {
                if (auto * settings_ast = arg->template as<ASTSetQuery>())
                {
                    has_settings = true;
                    settings_ast->changes.setSetting("iceberg_metadata_file_path", iceberg_metadata_file_path.value);
                    break;
                }
            }

            if (!has_settings)
            {
                boost::intrusive_ptr<ASTSetQuery> settings_ast = make_intrusive<ASTSetQuery>();
                settings_ast->is_standalone = false;
                settings_ast->changes.setSetting("iceberg_metadata_file_path", iceberg_metadata_file_path.value);
                arguments->children.push_back(settings_ast);
            }
        }

        return res;
    }

private:
    const DataLakeStorageSettingsPtr settings;
    ObjectStoragePtr ready_object_storage;
    std::string catalog_namespaces;
    DataLakeMetadataPtr current_metadata;
    LoggerPtr log = getLogger("DataLakeConfiguration");

    void assertLocalPathCorrect(ObjectStoragePtr object_storage, ContextPtr local_context)
    {
        if (object_storage->getType() == ObjectStorageType::Local)
        {
            auto user_files_path = local_context->getUserFilesPath();
            if (!fileOrSymlinkPathStartsWith(this->getPathForRead().path, user_files_path))
                throw Exception(
                    ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", this->getPathForRead().path, user_files_path);
        }
    }

    void assertInitializedDL() const
    {
        BaseStorageConfiguration::assertInitialized();
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
        assertLocalPathCorrect(object_storage, local_context);
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

#    if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
using StorageAzurePaimonConfiguration = DataLakeConfiguration<StorageAzureConfiguration, PaimonMetadata>;
#endif

#    if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
using StorageHDFSPaimonConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, PaimonMetadata>;
#endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;
using StorageLocalPaimonConfiguration = DataLakeConfiguration<StorageLocalConfiguration, PaimonMetadata, /* is_cluster_supported */ false>;

/// Class detects storage type by `storage_type` parameter if exists
/// and uses appropriate implementation - S3, Azure, HDFS or Local
class StorageIcebergConfiguration : public StorageObjectStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorageConfiguration>
{
    friend class StorageObjectStorageConfiguration;

public:
    StorageIcebergConfiguration() {}

    explicit StorageIcebergConfiguration(DataLakeStorageSettingsPtr settings_) : settings(settings_) {}

    void initialize(
        ASTs & engine_args,
        ContextPtr local_context,
        bool with_table_structure,
        const StorageID * table_id = nullptr) override
    {
        createDynamicConfiguration(engine_args, local_context);
        getImpl().initialize(engine_args, local_context, with_table_structure, table_id);
    }

    ObjectStorageType getType() const override { return getImpl().getType(); }

    std::string getTypeName() const override { return getImpl().getTypeName(); }
    std::string getEngineName() const override { return getImpl().getEngineName(); }
    std::string getNamespaceType() const override { return getImpl().getNamespaceType(); }

    Path getRawPath() const override { return getImpl().getRawPath(); }
    void setRawPath(const Path & path) override { getImpl().setRawPath(path); }
    const String & getRawURI() const override { return getImpl().getRawURI(); }
    const Path & getPathForRead() const override { return getImpl().getPathForRead(); }
    Path getPathForWrite(const std::string & partition_id) const override { return getImpl().getPathForWrite(partition_id); }

    void setPathForRead(const Path & path) override { getImpl().setPathForRead(path); }

    const Paths & getPaths() const override { return getImpl().getPaths(); }
    void setPaths(const Paths & paths) override { getImpl().setPaths(paths); }

    String getDataSourceDescription() const override { return getImpl().getDataSourceDescription(); }
    String getNamespace() const override { return getImpl().getNamespace(); }

    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr & context) const override
        { return getImpl().getQuerySettings(context); }

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure) override
        { getImpl().addStructureAndFormatToArgsIfNeeded(args, structure_, format_, context, with_structure); }

    bool isNamespaceWithGlobs() const override { return getImpl().isNamespaceWithGlobs(); }

    bool isArchive() const override { return getImpl().isArchive(); }
    bool isPathInArchiveWithGlobs() const override { return getImpl().isPathInArchiveWithGlobs(); }
    std::string getPathInArchive() const override { return getImpl().getPathInArchive(); }

    void check(ContextPtr context) override { getImpl().check(context); }
    void validateNamespace(const String & name) const override { getImpl().validateNamespace(name); }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) override
        { return getImpl().createObjectStorage(context, is_readonly, refresh_credentials_callback); }
    bool isStaticConfiguration() const override { return getImpl().isStaticConfiguration(); }

    bool isDataLakeConfiguration() const override { return getImpl().isDataLakeConfiguration(); }

    bool supportsTotalRows(ContextPtr context, ObjectStorageType storage_type) const override { return getImpl().supportsTotalRows(context, storage_type); }
    std::optional<size_t> totalRows(ContextPtr context) override { return getImpl().totalRows(context); }
    bool supportsTotalBytes(ContextPtr context, ObjectStorageType storage_type) const override { return getImpl().supportsTotalBytes(context, storage_type); }
    std::optional<size_t> totalBytes(ContextPtr context) override { return getImpl().totalBytes(context); }
    bool isDataSortedBySortingKey(StorageMetadataPtr storage_metadata, ContextPtr context) const override
        { return getImpl().isDataSortedBySortingKey(storage_metadata, context); }

    IDataLakeMetadata * getExternalMetadata() override { return getImpl().getExternalMetadata(); }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(ContextPtr context, ObjectInfoPtr object_info) const override
        { return getImpl().getInitialSchemaByPath(context, object_info); }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(ContextPtr context, ObjectInfoPtr object_info) const override
        { return getImpl().getSchemaTransformer(context, object_info); }

    void modifyFormatSettings(FormatSettings & settings_, const Context & context) const override
        { getImpl().modifyFormatSettings(settings_, context); }

    void addDeleteTransformers(
        ObjectInfoPtr object_info,
        QueryPipelineBuilder & builder,
        const std::optional<FormatSettings> & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources,
        ContextPtr local_context) const override
        { getImpl().addDeleteTransformers(object_info, builder, format_settings, parser_shared_resources, local_context); }

    ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        bool supports_tuple_elements,
        ContextPtr local_context,
        const PrepareReadingFromFormatHiveParams & hive_parameters) override
    {
        return getImpl().prepareReadingFromFormat(
            object_storage,
            requested_columns,
            storage_snapshot,
            supports_subset_of_columns,
            supports_tuple_elements,
            local_context,
            hive_parameters);
    }

    void setSchemaHash(const String & hash) override { getImpl().setSchemaHash(hash); }

    void initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context) override
        { getImpl().initPartitionStrategy(partition_by, columns, context); }

    std::optional<DataLakeTableStateSnapshot> getTableStateSnapshot(ContextPtr local_context) const override { return getImpl().getTableStateSnapshot(local_context); }
    std::unique_ptr<StorageInMemoryMetadata> buildStorageMetadataFromState(const DataLakeTableStateSnapshot & state, ContextPtr local_context) const override
        { return getImpl().buildStorageMetadataFromState(state, local_context); }
    bool shouldReloadSchemaForConsistency(ContextPtr local_context) const override { return getImpl().shouldReloadSchemaForConsistency(local_context); }
    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata(ContextPtr local_context) const override
        { return getImpl().tryGetTableStructureFromMetadata(local_context); }

    bool supportsFileIterator() const override { return getImpl().supportsFileIterator(); }
    bool supportsParallelInsert() const override { return getImpl().supportsParallelInsert(); }
    bool supportsWrites() const override { return getImpl().supportsWrites(); }

    bool supportsPartialPathPrefix() const override { return getImpl().supportsPartialPathPrefix(); }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        IDataLakeMetadata::FileProgressCallback callback,
        size_t list_batch_size,
        StorageMetadataPtr storage_metadata,
        ContextPtr context) override
    {
        return getImpl().iterate(filter_dag, callback, list_batch_size, storage_metadata, context);
    }

    void update(
        ObjectStoragePtr object_storage_ptr,
        ContextPtr context) override
    {
        getImpl().update(object_storage_ptr, context);
    }
    void lazyInitializeIfNeeded(ObjectStoragePtr object_storage, ContextPtr local_context) override
        { return getImpl().lazyInitializeIfNeeded(object_storage, local_context); }

    void create(
        ObjectStoragePtr object_storage,
        ContextPtr local_context,
        const std::optional<ColumnsDescription> & columns,
        ASTPtr partition_by,
        ASTPtr order_by,
        bool if_not_exists,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const StorageID & table_id_) override
    {
        getImpl().create(object_storage, local_context, columns, partition_by, order_by, if_not_exists, catalog, table_id_);
    }

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & table_id,
        ObjectStoragePtr object_storage,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> catalog) override
    {
        return getImpl().write(sample_block, table_id, object_storage, format_settings, context, catalog);
    }

    bool supportsDelete() const override { return getImpl().supportsDelete(); }
    void mutate(const MutationCommands & commands,
        ContextPtr context,
        const StorageID & storage_id,
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<DataLake::ICatalog> catalog,
        const std::optional<FormatSettings> & format_settings) override
    {
        getImpl().mutate(commands, context, storage_id, metadata_snapshot, catalog, format_settings);
    }
    void checkMutationIsPossible(const MutationCommands & commands) override { getImpl().checkMutationIsPossible(commands); }

    void checkAlterIsPossible(const AlterCommands & commands) override { getImpl().checkAlterIsPossible(commands); }

    void alter(const AlterCommands & params, ContextPtr context) override { getImpl().alter(params, context); }

    const DataLakeStorageSettings & getDataLakeSettings() const override { return getImpl().getDataLakeSettings(); }

    ASTPtr createArgsWithAccessData() const override
    {
        return getImpl().createArgsWithAccessData();
    }

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override
        { getImpl().fromNamedCollection(collection, context); }
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override
        { getImpl().fromAST(args, context, with_structure); }
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override
        { getImpl().fromDisk(disk_name, args, context, with_structure); }

    /// Find storage_type argument and remove it from args if exists.
    /// Return storage type.
    ObjectStorageType extractDynamicStorageType(ASTs & args, ContextPtr context, ASTPtr * type_arg, bool cluster_name_first) const override
    {
        static const auto * const storage_type_name = "storage_type";

        {
            auto args_copy = args;
            if (cluster_name_first)
            {
                // Remove cluster name from args to avoid confusing cluster name and named collection name
                args_copy.erase(args_copy.begin());
            }

            if (auto named_collection = tryGetNamedCollectionWithOverrides(args_copy, context))
            {
                if (named_collection->has(storage_type_name))
                {
                    return objectStorageTypeFromString(named_collection->get<String>(storage_type_name));
                }
            }
        }

        auto type_it = args.end();

        /// S3 by default for backward compatibility
        /// Iceberg without storage_type == IcebergS3
        ObjectStorageType type = ObjectStorageType::S3;

        for (auto arg_it = args.begin(); arg_it != args.end(); ++arg_it)
        {
            const auto * type_ast_function = (*arg_it)->as<ASTFunction>();

            if (type_ast_function && type_ast_function->name == "equals"
                && type_ast_function->arguments && type_ast_function->arguments->children.size() == 2)
            {
                auto * name = type_ast_function->arguments->children[0]->as<ASTIdentifier>();

                if (name && name->name() == storage_type_name)
                {
                    if (type_it != args.end())
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DataLake can have only one key-value argument: storage_type='type'.");
                    }

                    auto * value = type_ast_function->arguments->children[1]->as<ASTLiteral>();

                    if (!value)
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DataLake parameter 'storage_type' has wrong type, string literal expected.");
                    }

                    if (value->value.getType() != Field::Types::String)
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DataLake parameter 'storage_type' has wrong value type, string expected.");
                    }

                    type = objectStorageTypeFromString(value->value.safeGet<String>());

                    type_it = arg_it;
                }
            }
        }

        if (type_it != args.end())
        {
            if (type_arg)
                *type_arg = *type_it;
            args.erase(type_it);
        }

        return type;
    }

    const String & getFormat() const override { return getImpl().getFormat(); }
    const String & getCompressionMethod() const override { return getImpl().getCompressionMethod(); }
    const String & getStructure() const override { return getImpl().getStructure(); }

    PartitionStrategyFactory::StrategyType getPartitionStrategyType() const override { return getImpl().getPartitionStrategyType(); }
    bool getPartitionColumnsInDataFile() const override { return getImpl().getPartitionColumnsInDataFile(); }
    std::shared_ptr<IPartitionStrategy> getPartitionStrategy() const override { return getImpl().getPartitionStrategy(); }

    void setFormat(const String & format_) override { getImpl().setFormat(format_); }
    void setCompressionMethod(const String & compression_method_) override { getImpl().setCompressionMethod(compression_method_); }
    void setStructure(const String & structure_) override { getImpl().setStructure(structure_); }

    void setPartitionStrategyType(PartitionStrategyFactory::StrategyType partition_strategy_type_) override
        { getImpl().setPartitionStrategyType(partition_strategy_type_); }
    void setPartitionColumnsInDataFile(bool partition_columns_in_data_file_) override
        { getImpl().setPartitionColumnsInDataFile(partition_columns_in_data_file_); }
    void setPartitionStrategy(const std::shared_ptr<IPartitionStrategy> & partition_strategy_) override
        { getImpl().setPartitionStrategy(partition_strategy_); }

    void assertInitialized() const override { getImpl().assertInitialized(); }

    ColumnMapperPtr getColumnMapperForObject(ObjectInfoPtr obj) const override { return getImpl().getColumnMapperForObject(obj); }

    ColumnMapperPtr getColumnMapperForCurrentSchema(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const override
        { return getImpl().getColumnMapperForCurrentSchema(storage_metadata_snapshot, context); }

    std::shared_ptr<DataLake::ICatalog> getCatalog(ContextPtr context, bool is_attach) const override
        { return getImpl().getCatalog(context, is_attach); }

    bool optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings) override
        { return getImpl().optimize(metadata_snapshot, context, format_settings); }

    bool supportsPrewhere() const override { return getImpl().supportsPrewhere(); }

    void drop(ContextPtr context) override { getImpl().drop(context); }

protected:
    void createDynamicConfiguration(ASTs & args, ContextPtr context)
    {
        ObjectStorageType type = extractDynamicStorageType(args, context, nullptr, false);
        createDynamicStorage(type);
    }

private:
    inline StorageObjectStorageConfiguration & getImpl() const
    {
        if (!impl)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dynamic DataLake storage not initialized");

        return *impl;
    }

    void createDynamicStorage(ObjectStorageType type)
    {
        if (impl)
        {
            if (impl->getType() == type)
                return;

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't change datalake engine storage");
        }

        switch (type)
        {
#    if USE_AWS_S3
            case ObjectStorageType::S3:
                impl = std::make_unique<StorageS3IcebergConfiguration>(settings);
                break;
#    endif
#    if USE_AZURE_BLOB_STORAGE
            case ObjectStorageType::Azure:
                impl = std::make_unique<StorageAzureIcebergConfiguration>(settings);
                break;
#    endif
#    if USE_HDFS
            case ObjectStorageType::HDFS:
                impl = std::make_unique<StorageHDFSIcebergConfiguration>(settings);
                break;
#    endif
            case ObjectStorageType::Local:
                impl = std::make_unique<StorageLocalIcebergConfiguration>(settings);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsuported DataLake storage {}", type);
        }
    }

    StorageObjectStorageConfigurationPtr impl;
    DataLakeStorageSettingsPtr settings;
};
#endif

#if USE_PARQUET
#if USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#endif

#if USE_AZURE_BLOB_STORAGE
using StorageAzureDeltaLakeConfiguration = DataLakeConfiguration<StorageAzureConfiguration, DeltaLakeMetadata>;
#endif

using StorageLocalDeltaLakeConfiguration = DataLakeConfiguration<StorageLocalConfiguration, DeltaLakeMetadata, /* is_cluster_supported */ false>;

#endif

#if USE_AWS_S3
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;
#endif
}
