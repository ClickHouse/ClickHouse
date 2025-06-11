#include "config.h"

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <Access/Common/AccessFlags.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSetQuery.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>
#include <TableFunctions/registerTableFunctions.h>

#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/Utils.h>


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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Definition, typename Configuration, bool is_data_lake>
ObjectStoragePtr TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::getObjectStorage(const ContextPtr & context, bool create_readonly) const
{
    if (!object_storage)
        object_storage = configuration->createObjectStorage(context, create_readonly);
    return object_storage;
}

template <typename Definition, typename Configuration, bool is_data_lake>
StorageObjectStorage::ConfigurationPtr TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::getConfiguration() const
{
    if (!configuration)
    {
        if constexpr (is_data_lake)
            configuration = std::make_shared<Configuration>(settings);
        else
            configuration = std::make_shared<Configuration>();
    }
    return configuration;
}

template <typename Definition, typename Configuration, bool is_data_lake>
std::vector<size_t> TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::skipAnalysisForArguments(
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

template <typename Definition, typename Configuration, bool is_data_lake>
std::shared_ptr<typename TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::Settings>
TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::createEmptySettings()
{
    if constexpr (is_data_lake)
        return std::make_shared<DataLakeStorageSettings>();
    else
        return std::make_shared<StorageObjectStorageSettings>();
}

template <typename Definition, typename Configuration, bool is_data_lake>
void TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();
    ASTs & args_func = ast_copy->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    settings = createEmptySettings();

    auto & args = args_func.at(0)->children;
    /// Support storage settings in table function,
    /// e.g. `s3(endpoint, ..., SETTINGS setting=value, ..., setting=value)`
    /// We do similarly for some other table functions
    /// whose storage implementation supports storage settings (for example, MySQL).
    for (auto * it = args.begin(); it != args.end(); ++it)
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

template <typename Definition, typename Configuration, bool is_data_lake>
ColumnsDescription TableFunctionObjectStorage<
    Definition, Configuration, is_data_lake>::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (configuration->structure == "auto")
    {
        context->checkAccess(getSourceAccessType());

        auto storage = getObjectStorage(context, !is_insert_query);
        configuration->update(
            object_storage,
            context,
            /* if_not_updated_before */true,
            /* check_consistent_with_previous_metadata */true);

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

        return columns;
    }
    return parseColumnsListFromString(configuration->structure, context);
}

template <typename Definition, typename Configuration, bool is_data_lake>
StoragePtr TableFunctionObjectStorage<Definition, Configuration, is_data_lake>::executeImpl(
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
            context);

        storage->startup();
        return storage;
    }

    storage = std::make_shared<StorageObjectStorage>(
        configuration,
        getObjectStorage(context, !is_insert_query),
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        /* comment */ String{},
        /* format_settings */ std::nullopt,
        /* mode */ LoadingStrictnessLevel::CREATE,
        /* distributed_processing */ is_secondary_query,
        /* partition_by */ nullptr,
        /* is_table_function */ true);

    storage->startup();
    return storage;
}

void registerTableFunctionObjectStorage(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AWS_S3
    factory.registerFunction<TableFunctionObjectStorage<S3Definition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on AWS S3.)",
            .examples{{"s3", "SELECT * FROM s3(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<GCSDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on GCS.)",
            .examples{{"gcs", "SELECT * FROM gcs(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<COSNDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on COSN.)",
            .examples{{"cosn", "SELECT * FROM cosn(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<OSSDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on OSS.)",
            .examples{{"oss", "SELECT * FROM oss(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionObjectStorage<AzureDefinition, StorageAzureConfiguration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on Azure Blob Storage.)",
            .examples{
            {
                "azureBlobStorage",
                "SELECT * FROM  azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, "
                "[account_name, account_key, format, compression, structure])", ""
            }},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionObjectStorage<HDFSDefinition, StorageHDFSConfiguration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on HDFS virtual filesystem.)",
            .examples{
            {
                "hdfs",
                "SELECT * FROM  hdfs(url, format, compression, structure])", ""
            }},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });
#endif
}

#if USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<AzureDefinition, StorageAzureConfiguration>;
template class TableFunctionObjectStorage<AzureClusterDefinition, StorageAzureConfiguration>;
#endif

#if USE_AWS_S3
template class TableFunctionObjectStorage<S3Definition, StorageS3Configuration>;
template class TableFunctionObjectStorage<S3ClusterDefinition, StorageS3Configuration>;
template class TableFunctionObjectStorage<GCSDefinition, StorageS3Configuration>;
template class TableFunctionObjectStorage<COSNDefinition, StorageS3Configuration>;
template class TableFunctionObjectStorage<OSSDefinition, StorageS3Configuration>;
#endif

#if USE_HDFS
template class TableFunctionObjectStorage<HDFSDefinition, StorageHDFSConfiguration>;
template class TableFunctionObjectStorage<HDFSClusterDefinition, StorageHDFSConfiguration>;
#endif
template class TableFunctionObjectStorage<LocalDefinition, StorageLocalConfiguration>;

#if USE_AVRO && USE_AWS_S3
template class TableFunctionObjectStorage<IcebergS3ClusterDefinition, StorageS3IcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<IcebergAzureClusterDefinition, StorageAzureIcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_HDFS
template class TableFunctionObjectStorage<IcebergHDFSClusterDefinition, StorageHDFSIcebergConfiguration, true>;
#endif

#if USE_PARQUET && USE_AWS_S3 && USE_DELTA_KERNEL_RS
template class TableFunctionObjectStorage<DeltaLakeClusterDefinition, StorageS3DeltaLakeConfiguration, true>;
#endif

#if USE_AWS_S3
template class TableFunctionObjectStorage<HudiClusterDefinition, StorageS3HudiConfiguration, true>;
#endif

#if USE_AVRO
void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionIceberg>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store. Alias to icebergS3)",
            .examples{{"iceberg", "SELECT * FROM iceberg(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
    factory.registerFunction<TableFunctionIcebergS3>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store.)",
            .examples{{"icebergS3", "SELECT * FROM icebergS3(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});

#endif
#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzure>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on Azure object store.)",
            .examples{{"icebergAzure", "SELECT * FROM icebergAzure(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFS>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem.)",
            .examples{{"icebergHDFS", "SELECT * FROM icebergHDFS(url)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
    factory.registerFunction<TableFunctionIcebergLocal>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored locally.)",
            .examples{{"icebergLocal", "SELECT * FROM icebergLocal(filename)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
}
#endif


#if USE_PARQUET && USE_DELTA_KERNEL_RS
void registerTableFunctionDeltaLake(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionDeltaLake>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on S3, alias of deltaLakeS3.)",
            .examples{{"deltaLake", "SELECT * FROM deltaLake(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});

    factory.registerFunction<TableFunctionDeltaLakeS3>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on S3.)",
            .examples{{"deltaLakeS3", "SELECT * FROM deltaLakeS3(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionDeltaLakeAzure>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on Azure object store.)",
            .examples{{"deltaLakeAzure", "SELECT * FROM deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, \"\n"
 "                \"[account_name, account_key, format, compression, structure])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
    // Register the new local Delta Lake table function
    factory.registerFunction<TableFunctionDeltaLakeLocal>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored locally.)",
            .examples{{"deltaLakeLocal", "SELECT * FROM deltaLakeLocal(path)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
}
#endif

#if USE_AWS_S3
void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
        {.documentation
         = {.description = R"(The table function can be used to read the Hudi table stored on object store.)",
            .examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
}
#endif

void registerDataLakeTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AVRO
    registerTableFunctionIceberg(factory);
#endif

#if USE_PARQUET && USE_DELTA_KERNEL_RS
    registerTableFunctionDeltaLake(factory);
#endif
#if USE_AWS_S3
    registerTableFunctionHudi(factory);
#endif
}
}
