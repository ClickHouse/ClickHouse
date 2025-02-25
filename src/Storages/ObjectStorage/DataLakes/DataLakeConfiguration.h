#pragma once

#include <Storages/IStorage.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Disks/DiskType.h>

#include <memory>
#include <string>
#include <unordered_map>

#include <Common/ErrorCodes.h>


namespace DB
{

namespace ErrorCodes
{
extern const int FORMAT_VERSION_TOO_OLD;
extern const int LOGICAL_ERROR;
}

template <typename T>
concept StorageConfiguration = std::derived_from<T, StorageObjectStorage::Configuration>;

template <StorageConfiguration BaseStorageConfiguration, typename DataLakeMetadata>
class DataLakeConfiguration : public BaseStorageConfiguration, public std::enable_shared_from_this<StorageObjectStorage::Configuration>
{
public:
    using Configuration = StorageObjectStorage::Configuration;

    bool isDataLakeConfiguration() const override { return true; }

    std::string getEngineName() const override { return DataLakeMetadata::name + BaseStorageConfiguration::getEngineName(); }

    void update(ObjectStoragePtr object_storage, ContextPtr local_context) override
    {
        BaseStorageConfiguration::update(object_storage, local_context);

        bool existed = current_metadata != nullptr;

        if (updateMetadataObjectIfNeeded(object_storage, local_context))
        {
            if (hasExternalDynamicMetadata() && existed)
            {
                throw Exception(
                    ErrorCodes::FORMAT_VERSION_TOO_OLD,
                    "Metadata is not consinsent with the one which was used to infer table schema. Please, retry the query.");
            }
            if (!supportsFileIterator())
                BaseStorageConfiguration::setPaths(current_metadata->getDataFiles());
        }
    }

    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata() const override
    {
        if (!current_metadata)
            return std::nullopt;
        auto schema_from_metadata = current_metadata->getTableSchema();
        if (!schema_from_metadata.empty())
        {
            return ColumnsDescription(std::move(schema_from_metadata));
        }
        return std::nullopt;
    }

    void implementPartitionPruning(const ActionsDAG & filter_dag) override
    {
        if (!current_metadata || !current_metadata->supportsPartitionPruning())
            return;
        BaseStorageConfiguration::setPaths(current_metadata->makePartitionPruning(filter_dag));
    }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & data_path) const override
    {
        if (!current_metadata)
            return {};
        return current_metadata->getInitialSchemaByPath(data_path);
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
    {
        if (!current_metadata)
            return {};
        return current_metadata->getSchemaTransformer(data_path);
    }

    bool hasExternalDynamicMetadata() override
    {
        return StorageObjectStorage::Configuration::allow_dynamic_metadata_for_data_lakes && current_metadata
            && current_metadata->supportsExternalMetadataChange();
    }

    ColumnsDescription updateAndGetCurrentSchema(
        ObjectStoragePtr object_storage,
        ContextPtr context) override
    {
        BaseStorageConfiguration::update(object_storage, context);
        if (updateMetadataObjectIfNeeded(object_storage, context))
        {
            if (!supportsFileIterator())
                BaseStorageConfiguration::setPaths(current_metadata->getDataFiles());
        }

        return ColumnsDescription{current_metadata->getTableSchema()};
    }

    bool supportsFileIterator() const override
    {
        chassert(current_metadata);
        return current_metadata->supportsFileIterator();
    }

    ObjectIterator iterate() override
    {
        chassert(current_metadata);
        return current_metadata->iterate();
    }

    /// This is an awful temporary crutch,
    /// which will be removed once DeltaKernel is used by default for DeltaLake.
    /// By release 25.3.
    /// (Because it does not make sense to support it in a nice way
    /// because the code will be removed ASAP anyway)
#if USE_PARQUET && USE_AWS_S3
    DeltaLakePartitionColumns getDeltaLakePartitionColumns() const
    {
        const auto * delta_lake_metadata = dynamic_cast<const DeltaLakeMetadata *>(current_metadata.get());
        if (delta_lake_metadata)
            return delta_lake_metadata->getPartitionColumns();
        return {};
    }
#endif

private:
    DataLakeMetadataPtr current_metadata;
    LoggerPtr log = getLogger("DataLakeConfiguration");

    ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context) override
    {
        auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns);
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(
                object_storage,
                weak_from_this(),
                local_context, BaseStorageConfiguration::allow_experimental_delta_kernel_rs);
        }
        auto read_schema = current_metadata->getReadSchema();
        if (!read_schema.empty())
        {
            /// There is a difference between "table schema" and "read schema".
            /// "table schema" is a schema from data lake table metadata,
            /// while "read schema" is a schema from data files.
            /// In most cases they would be the same.
            /// TODO: Try to hide this logic inside IDataLakeMetadata.

            const auto read_schema_names = read_schema.getNames();
            const auto table_schema_names = current_metadata->getTableSchema().getNames();
            chassert(read_schema_names.size() == table_schema_names.size());

            if (read_schema_names != table_schema_names)
            {
                LOG_TEST(log, "Read schema: {}, table schema: {}, requested columns: {}",
                         fmt::join(read_schema_names, ", "),
                         fmt::join(table_schema_names, ", "),
                         fmt::join(info.requested_columns.getNames(), ", "));

                auto column_name_mapping = [&]()
                {
                    std::map<std::string, std::string> result;
                    for (size_t i = 0; i < read_schema_names.size(); ++i)
                        result[table_schema_names[i]] = read_schema_names[i];
                    return result;
                }();

                /// Go through requested columns and change column name
                /// from table schema to column name from read schema.

                std::vector<NameAndTypePair> read_columns;
                for (const auto & column_name : info.requested_columns)
                {
                    const auto pos = info.format_header.getPositionByName(column_name.name);
                    auto column = info.format_header.getByPosition(pos);
                    column.name = column_name_mapping.at(column_name.name);
                    info.format_header.setColumn(pos, column);

                    read_columns.emplace_back(column.name, column.type);
                }
                info.requested_columns = NamesAndTypesList(read_columns.begin(), read_columns.end());
            }
        }

        return info;
    }

    bool updateMetadataObjectIfNeeded(
        ObjectStoragePtr object_storage,
        ContextPtr context)
    {
        if (!current_metadata)
        {
            current_metadata = DataLakeMetadata::create(
                object_storage,
                weak_from_this(),
                context,
                BaseStorageConfiguration::allow_experimental_delta_kernel_rs);
            return true;
        }

        if (current_metadata->supportsUpdate())
        {
            return current_metadata->update(context);
        }

        auto new_metadata = DataLakeMetadata::create(
            object_storage,
            weak_from_this(),
            context,
            BaseStorageConfiguration::allow_experimental_delta_kernel_rs);

        if (*current_metadata != *new_metadata)
        {
            current_metadata = std::move(new_metadata);
            return true;
        }
        else
        {
            return false;
        }
    }
};


#if USE_AVRO
#    if USE_AWS_S3
using StorageS3IcebergConfiguration = DataLakeConfiguration<StorageS3Configuration, IcebergMetadata>;
#    endif

#    if USE_AZURE_BLOB_STORAGE
using StorageAzureIcebergConfiguration = DataLakeConfiguration<StorageAzureConfiguration, IcebergMetadata>;
#    endif

#    if USE_HDFS
using StorageHDFSIcebergConfiguration = DataLakeConfiguration<StorageHDFSConfiguration, IcebergMetadata>;
#    endif

using StorageLocalIcebergConfiguration = DataLakeConfiguration<StorageLocalConfiguration, IcebergMetadata>;


class StorageIcebergConfiguration : public StorageObjectStorage::Configuration, public std::enable_shared_from_this<StorageObjectStorage::Configuration>
{
    friend class StorageObjectStorage::Configuration;

public:
    ObjectStorageType getType() const override { return getImpl().getType(); }

    std::string getTypeName() const override { return getImpl().getTypeName(); }
    std::string getEngineName() const override { return getImpl().getEngineName(); }
    std::string getNamespaceType() const override { return getImpl().getNamespaceType(); }

    Path getPath() const override { return getImpl().getPath(); }
    void setPath(const Path & path) override { getImpl().setPath(path); }

    const Paths & getPaths() const override { return getImpl().getPaths(); }
    void setPaths(const Paths & paths) override { getImpl().setPaths(paths); }

    String getDataSourceDescription() const override { return getImpl().getDataSourceDescription(); }
    String getNamespace() const override { return getImpl().getNamespace(); }

    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr & context) const override
        { return getImpl().getQuerySettings(context); }

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure) override
        { getImpl().addStructureAndFormatToArgsIfNeeded(args, structure_, format_, context, with_structure); }

    std::string getPathWithoutGlobs() const override { return getImpl().getPathWithoutGlobs(); }

    bool isArchive() const override { return getImpl().isArchive(); }
    std::string getPathInArchive() const override { return getImpl().getPathInArchive(); }

    void check(ContextPtr context) const override { getImpl().check(context); }
    void validateNamespace(const String & name) const override { getImpl().validateNamespace(name); }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override
        { return getImpl().createObjectStorage(context, is_readonly); }
    StorageObjectStorage::ConfigurationPtr clone() override { return getImpl().clone(); }
    bool isStaticConfiguration() const override { return getImpl().isStaticConfiguration(); }

    bool isDataLakeConfiguration() const override { return getImpl().isDataLakeConfiguration(); }

    void implementPartitionPruning(const ActionsDAG & filter_dag) override
        { getImpl().implementPartitionPruning(filter_dag); }

    bool hasExternalDynamicMetadata() override { return getImpl().hasExternalDynamicMetadata(); }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & path) const override
        { return getImpl().getInitialSchemaByPath(path); }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
        { return getImpl().getSchemaTransformer(data_path); }

    ColumnsDescription updateAndGetCurrentSchema(ObjectStoragePtr object_storage, ContextPtr context) override
        { return getImpl().updateAndGetCurrentSchema(object_storage, context); }

    ReadFromFormatInfo prepareReadingFromFormat(
        ObjectStoragePtr object_storage,
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        bool supports_subset_of_columns,
        ContextPtr local_context) override
    {
        return getImpl().prepareReadingFromFormat(
            object_storage,
            requested_columns,
            storage_snapshot,
            supports_subset_of_columns,
            local_context);
    }

    std::optional<ColumnsDescription> tryGetTableStructureFromMetadata() const override
        { return getImpl().tryGetTableStructureFromMetadata(); }

    void update(ObjectStoragePtr object_storage, ContextPtr local_context) override
        { return getImpl().update(object_storage, local_context); }

    void initialize(
        ASTs & engine_args,
        ContextPtr local_context,
        bool with_table_structure,
        StorageObjectStorageSettings * settings) override
    {
        createDynamicConfiguration(engine_args);
        getImpl().initialize(engine_args, local_context, with_table_structure, settings);
    }

protected:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override
        { return getImpl().fromNamedCollection(collection, context); }
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override
        { return getImpl().fromAST(args, context, with_structure); }

    /// Find storage_type argument and remove it from args if exists.
    /// Return storage type.
    ObjectStorageType extractDynamicStorageType(ASTs & args, ASTPtr * type_arg = nullptr) const override
    {
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
                auto name = type_ast_function->arguments->children[0]->as<ASTIdentifier>();

                if (name && name->name() == "storage_type")
                {
                    if (type_it != args.end())
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DataLake can have only one key-value argument: storage_type=().");
                    }

                    auto value = type_ast_function->arguments->children[1]->as<ASTLiteral>();

                    if (!value)
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DataLake parameter 'storage_type' has wrong type.");
                    }

                    if (value->value.getType() != Field::Types::String)
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DataLake parameter 'storage_type' has wrong value type.");
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

    void createDynamicConfiguration(ASTs & args)
    {
        ObjectStorageType type = extractDynamicStorageType(args);
        createDynamicStorage(type);
    }

private:
    inline StorageObjectStorage::Configuration & getImpl() const
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
                impl = std::make_unique<StorageS3IcebergConfiguration>();
                break;
#    endif
#    if USE_AZURE_BLOB_STORAGE
            case ObjectStorageType::Azure:
                impl = std::make_unique<StorageAzureIcebergConfiguration>();
                break;
#    endif
#    if USE_HDFS
            case ObjectStorageType::HDFS:
                impl = std::make_unique<StorageHDFSIcebergConfiguration>();
                break;
#    endif
            case ObjectStorageType::Local:
                impl = std::make_unique<StorageLocalIcebergConfiguration>();
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsuported DataLake storage {}", type);
        }
    }

    std::shared_ptr<StorageObjectStorage::Configuration> impl;
};
#endif

#if USE_PARQUET && USE_AWS_S3
using StorageS3DeltaLakeConfiguration = DataLakeConfiguration<StorageS3Configuration, DeltaLakeMetadata>;
#endif

#if USE_AWS_S3
using StorageS3HudiConfiguration = DataLakeConfiguration<StorageS3Configuration, HudiMetadata>;
#endif
}
