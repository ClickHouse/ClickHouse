#include "config.h"

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <Access/Common/AccessFlags.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/Context.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>

#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Storages/ObjectStorage/Utils.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>


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

template <typename Definition, typename Configuration>
ObjectStoragePtr TableFunctionObjectStorage<Definition, Configuration>::getObjectStorage(const ContextPtr & context, bool create_readonly) const
{
    if (!object_storage)
        object_storage = configuration->createObjectStorage(context, create_readonly);
    return object_storage;
}

template <typename Definition, typename Configuration>
StorageObjectStorage::ConfigurationPtr TableFunctionObjectStorage<Definition, Configuration>::getConfiguration() const
{
    if (!configuration)
        configuration = std::make_shared<Configuration>();
    return configuration;
}

template <typename Definition, typename Configuration>
std::vector<size_t> TableFunctionObjectStorage<Definition, Configuration>::skipAnalysisForArguments(
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
void TableFunctionObjectStorage<Definition, Configuration>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();
    ASTs & args_func = ast_copy->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    auto & args = args_func.at(0)->children;
    parseArgumentsImpl(args, context);
}

template <typename Definition, typename Configuration>
ColumnsDescription TableFunctionObjectStorage<
    Definition, Configuration>::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (configuration->structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        ColumnsDescription columns;
        auto storage = getObjectStorage(context, !is_insert_query);
        std::string sample_path;
        resolveSchemaAndFormat(columns, configuration->format, storage, configuration, std::nullopt, sample_path, context);
        return columns;
    }
    return parseColumnsListFromString(configuration->structure, context);
}

template <typename Definition, typename Configuration>
StoragePtr TableFunctionObjectStorage<Definition, Configuration>::executeImpl(
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

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
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
            /* distributed_processing */ true,
            /* partition_by */ nullptr);

        storage->startup();
        return storage;
    }

    const auto & settings = context->getSettingsRef();
    auto parallel_replicas_cluster_name = settings[Setting::cluster_for_parallel_replicas].toString();
    auto can_use_parallel_replicas = settings[Setting::allow_experimental_parallel_reading_from_replicas] > 0
        && settings[Setting::parallel_replicas_for_cluster_engines]
        && settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::READ_TASKS
        && !parallel_replicas_cluster_name.empty()
        && !context->isDistributed();

    if (can_use_parallel_replicas)
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
        /* distributed_processing */ false,
        /* partition_by */ nullptr);

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
            .examples{{"s3", "SELECT * FROM s3(url, access_key_id, secret_access_key)", ""}
        },
        .categories{"DataLake"}},
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<GCSDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on GCS.)",
            .examples{{"gcs", "SELECT * FROM gcs(url, access_key_id, secret_access_key)", ""}
        },
        .categories{"DataLake"}},
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<COSNDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on COSN.)",
            .examples{{"cosn", "SELECT * FROM cosn(url, access_key_id, secret_access_key)", ""}
        },
        .categories{"DataLake"}},
        .allow_readonly = false
    });
    factory.registerFunction<TableFunctionObjectStorage<OSSDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on OSS.)",
            .examples{{"oss", "SELECT * FROM oss(url, access_key_id, secret_access_key)", ""}
        },
        .categories{"DataLake"}},
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
            }}
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
            }}
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
template class TableFunctionObjectStorage<IcebergS3ClusterDefinition, StorageS3IcebergConfiguration>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<IcebergAzureClusterDefinition, StorageAzureIcebergConfiguration>;
#endif

#if USE_AVRO && USE_HDFS
template class TableFunctionObjectStorage<IcebergHDFSClusterDefinition, StorageHDFSIcebergConfiguration>;
#endif

#if USE_PARQUET && USE_AWS_S3
template class TableFunctionObjectStorage<DeltaLakeClusterDefinition, StorageS3DeltaLakeConfiguration>;
#endif

#if USE_AWS_S3
template class TableFunctionObjectStorage<HudiClusterDefinition, StorageS3HudiConfiguration>;
#endif

#if USE_AVRO
void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionIceberg>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store. Alias to icebergS3)",
            .examples{{"iceberg", "SELECT * FROM iceberg(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
    factory.registerFunction<TableFunctionIcebergS3>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store.)",
            .examples{{"icebergS3", "SELECT * FROM icebergS3(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});

#endif
#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzure>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on Azure object store.)",
            .examples{{"icebergAzure", "SELECT * FROM icebergAzure(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFS>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem.)",
            .examples{{"icebergHDFS", "SELECT * FROM icebergHDFS(url)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
#endif
    factory.registerFunction<TableFunctionIcebergLocal>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored locally.)",
            .examples{{"icebergLocal", "SELECT * FROM icebergLocal(filename)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}
#endif


#if USE_AWS_S3
#if USE_PARQUET
void registerTableFunctionDeltaLake(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaLake>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on object store.)",
            .examples{{"deltaLake", "SELECT * FROM deltaLake(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}
#endif

void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
        {.documentation
         = {.description = R"(The table function can be used to read the Hudi table stored on object store.)",
            .examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}

#endif

void registerDataLakeTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AVRO
    registerTableFunctionIceberg(factory);
#endif
#if USE_AWS_S3
#if USE_PARQUET
    registerTableFunctionDeltaLake(factory);
#endif
    registerTableFunctionHudi(factory);
#endif
}
}
