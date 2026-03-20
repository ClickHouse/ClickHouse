#include <string_view>
#include "config.h"

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <Access/Common/AccessFlags.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSetQuery.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionIceberg.h>

#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Storages/ObjectStorage/Utils.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/DataLakes/StorageIceberg.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString disk;
}

template <typename Definition, typename Configuration>
ObjectStoragePtr TableFunctionIcebergImpl<Definition, Configuration>::getObjectStorage(const ContextPtr & context, bool create_readonly) const
{
    if (!object_storage)
        object_storage = configuration->createObjectStorage(context, create_readonly, std::nullopt);
    return object_storage;
}

template <typename Definition, typename Configuration>
StorageObjectStorageConfigurationPtr TableFunctionIcebergImpl<Definition, Configuration>::getConfiguration(ContextPtr context) const
{
    if (!configuration)
    {
        const auto disk_name = settings && (*settings)[DataLakeStorageSetting::disk].changed
            ? (*settings)[DataLakeStorageSetting::disk].value
            : "";
        if (!disk_name.empty())
        {
            auto disk = context->getDisk(disk_name);
            switch (disk->getObjectStorage()->getType())
            {
#if USE_AWS_S3 && USE_AVRO
            case ObjectStorageType::S3:
                if (Definition::object_storage_type != "s3")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type doesn't match with table engine type storage");

                if (std::string_view(Definition::name).starts_with("iceberg"))
                    configuration = std::make_shared<StorageS3IcebergConfiguration>(settings);
#if USE_PARQUET
                else
                    configuration = std::make_shared<StorageS3DeltaLakeConfiguration>(settings);
#endif
                break;
#endif
#if USE_AZURE_BLOB_STORAGE && USE_AVRO
            case ObjectStorageType::Azure:
                if (Definition::name != "iceberg" &&
                    Definition::name != "icebergCluster" &&
                    Definition::name != "deltaLake" &&
                    Definition::name != "deltaLakeCluster" &&
                    Definition::object_storage_type != "azure")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type doesn't match with table engine type storage");

                if (std::string_view(Definition::name).starts_with("iceberg"))
                    configuration = std::make_shared<StorageAzureIcebergConfiguration>(settings);
#if USE_PARQUET
                else
                    configuration = std::make_shared<StorageAzureDeltaLakeConfiguration>(settings);
#endif
                break;
#endif
#if USE_AVRO
            case ObjectStorageType::Local:
                if (Definition::name != "iceberg" &&
                    Definition::name != "icebergCluster" &&
                    Definition::name != "deltaLake" &&
                    Definition::name != "deltaLakeCluster" &&
                    Definition::object_storage_type != "local")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type doesn't match with table engine type storage");
                if (std::string_view(Definition::name).starts_with("iceberg"))
                    configuration = std::make_shared<StorageLocalIcebergConfiguration>(settings);
#if USE_PARQUET
                else
                    configuration = std::make_shared<StorageLocalDeltaLakeConfiguration>(settings);
#endif
                break;
#endif
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for iceberg {}", disk->getObjectStorage()->getType());
            }
        }
        else
            configuration = std::make_shared<Configuration>(settings);
    }
    return configuration;
}

template <typename Definition, typename Configuration>
std::vector<size_t> TableFunctionIcebergImpl<Definition, Configuration>::skipAnalysisForArguments(
    const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments_nodes.size();

    std::vector<size_t> result;
    for (size_t i = 0; i < table_function_arguments_size; ++i)
    {
        auto * function_node = table_function_arguments_nodes[i]->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "headers")
            result.push_back(i);
    }
    return result;
}

template <typename Definition, typename Configuration>
std::shared_ptr<typename TableFunctionIcebergImpl<Definition, Configuration>::Settings>
TableFunctionIcebergImpl<Definition, Configuration>::createEmptySettings()
{
    return std::make_shared<DataLakeStorageSettings>();
}

template <typename Definition, typename Configuration>
void TableFunctionIcebergImpl<Definition, Configuration>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();
    ASTs & args_func = ast_copy->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    settings = createEmptySettings();

    auto & args = args_func.at(0)->children;
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            settings->loadFromQuery(*settings_ast);
            args.erase(it);
            break;
        }
    }
    parseArgumentsImpl(args, context);
}

template <typename Definition, typename Configuration>
ColumnsDescription TableFunctionIcebergImpl<
    Definition, Configuration>::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (configuration->structure == "auto")
    {
        if (const auto access_object = getSourceAccessObject())
            context->checkAccess(AccessType::READ, toStringSource(*access_object));

        auto storage = getObjectStorage(context, !is_insert_query);
        configuration->lazyInitializeIfNeeded(object_storage, context);

        std::string sample_path;
        ColumnsDescription columns;
        resolveSchemaAndFormat(
            columns,
            configuration->format,
            std::move(storage),
            configuration,
            /* format_settings */std::nullopt,
            sample_path,
            context);

        HivePartitioningUtils::setupHivePartitioningForObjectStorage(
            columns,
            configuration,
            sample_path,
            /* inferred_schema */ true,
            /* format_settings */ std::nullopt,
            context);

        return columns;
    }
    return parseColumnsListFromString(configuration->structure, context);
}

template <typename Definition, typename Configuration>
StoragePtr TableFunctionIcebergImpl<Definition, Configuration>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    chassert(configuration);
    ColumnsDescription columns;

    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    StoragePtr storage;
    const auto & query_settings = context->getSettingsRef();
    const auto & client_info = context->getClientInfo();

    const auto parallel_replicas_cluster_name = query_settings[Setting::cluster_for_parallel_replicas].toString();
    const auto can_use_parallel_replicas = !parallel_replicas_cluster_name.empty()
        && query_settings[Setting::parallel_replicas_for_cluster_engines]
        && context->canUseTaskBasedParallelReplicas()
        && !context->isDistributed();

    const auto is_secondary_query = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;

    if (can_use_parallel_replicas && !is_secondary_query && !is_insert_query)
    {
        storage = std::make_shared<StorageObjectStorageCluster>(
            parallel_replicas_cluster_name,
            configuration,
            getObjectStorage(context, !is_insert_query),
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            partition_by,
            context,
            /* is_table_function */true);

        storage->startup();
        return storage;
    }

    bool can_use_distributed_iterator =
        client_info.collaborate_with_initiator &&
        context->hasClusterFunctionReadTaskCallback();

    std::string disk_name;
    disk_name = settings && (*settings)[DataLakeStorageSetting::disk].changed
        ? (*settings)[DataLakeStorageSetting::disk].value
        : "";

    ObjectStoragePtr current_object_storage;
    if (configuration->isDataLakeConfiguration() && !disk_name.empty())
        current_object_storage = context->getDisk(disk_name)->getObjectStorage();
    else
        current_object_storage = getObjectStorage(context, !is_insert_query);

    storage = std::make_shared<StorageIceberg>(
        configuration,
        current_object_storage,
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        /* comment */ String{},
        /* format_settings */ std::nullopt,
        /* mode */ LoadingStrictnessLevel::CREATE,
        /* catalog*/ nullptr,
        /* if_not_exists*/ false,
        /* is_datalake_query*/ false,
        /* distributed_processing */ can_use_distributed_iterator,
        /* partition_by */ partition_by,
        /* order_by */ nullptr,
        /* is_table_function */true);

    storage->startup();
    return storage;
}

#if USE_AVRO && USE_AWS_S3
template class TableFunctionIcebergImpl<IcebergDefinition, StorageS3IcebergConfiguration>;
#endif
#if USE_AVRO && USE_AWS_S3
template class TableFunctionIcebergImpl<IcebergS3Definition, StorageS3IcebergConfiguration>;
#endif
#if USE_AVRO && USE_AZURE_BLOB_STORAGE
template class TableFunctionIcebergImpl<IcebergAzureDefinition, StorageAzureIcebergConfiguration>;
#endif
#if USE_AVRO && USE_HDFS
template class TableFunctionIcebergImpl<IcebergHDFSDefinition, StorageHDFSIcebergConfiguration>;
#endif
#if USE_AVRO
template class TableFunctionIcebergImpl<IcebergLocalDefinition, StorageLocalIcebergConfiguration>;
#endif

}
