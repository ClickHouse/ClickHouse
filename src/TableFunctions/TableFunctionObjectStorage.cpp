#include "config.h"

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <Access/Common/AccessFlags.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Parsers/ASTSetQuery.h>
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

    settings = std::make_shared<StorageObjectStorageSettings>();

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
        /* is_table_function */true);

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
            .description=R"(
Provides a table-like interface to select/insert files in [Amazon S3](https://aws.amazon.com/s3/) and [Google Cloud Storage](https://cloud.google.com/storage/). This table function is similar to the [hdfs function](../../sql-reference/table-functions/hdfs.md), but provides S3-specific features.

If you have multiple replicas in your cluster, you can use the [s3Cluster function](../../sql-reference/table-functions/s3Cluster.md) instead to parallelize inserts.

When using the `s3 table function` with [`INSERT INTO...SELECT`](../../sql-reference/statements/insert-into#inserting-the-results-of-select), data is read and inserted in a streaming fashion. Only a few blocks of data reside in memory while the blocks are continuously read from S3 and pushed into the destination table.
)

:::tip GCS
The S3 Table Function integrates with Google Cloud Storage by using the GCS XML API and HMAC keys.  See the [Google interoperability docs]( https://cloud.google.com/storage/docs/interoperability) for more details about the endpoint and HMAC.

For GCS, substitute your HMAC key and HMAC secret where you see `access_key_id` and `secret_access_key`.
:::
)",
            .syntax=R"(
s3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,structure] [,compression_method],[,headers])
s3(named_collection[, option=value [,..]])
)",
            .arguments{
                {"url", R"(
Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. For more information see [here](../../engines/table-engines/integrations/s3.md#wildcards-in-path).                    
:::note GCS
The GCS url is in this format as the endpoint for the Google XML API is different than the JSON API:
```text
https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```
and not ~~https://storage.cloud.google.com~~.
:::
)"},
                {"NOSIGN", "If this keyword is provided in place of credentials, all the requests will not be signed."},
                {"access_key_id", "Keys that specify credentials to use with given endpoint. Optional."},
                {"secret_access_key", "Keys that specify credentials to use with given endpoint. Optional."},
                {"session_token", "Session token to use with the given keys. Optional when passing keys."},
                {"format", "The [format](/sql-reference/formats) of the file."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."},
                {"compression_method", "Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension."},
                {"headers", "Parameter is optional. Allows headers to be passed in the S3 request. Pass in the format `headers(key=value)` e.g. `headers('x-amz-request-payer' = 'requester')`."}
            },
            .returned_value="A table with the specified structure for reading or writing data in the specified file.",
            .examples{{"s3", "SELECT * FROM s3(url, access_key_id, secret_access_key)", ""}
        },
        .category=FunctionDocumentation::Category::TableFunction},
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<GCSDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on GCS.)",
            .syntax="gcs(url, access_key_id, secret_access_key)",
            .arguments={
                {"url", ""},
                {"access_key_id", ""},
                {"secret_access_key", ""}
            },
            .returned_value="",
            .examples{{"gcs", "SELECT * FROM gcs(url, access_key_id, secret_access_key)", ""}
        },
        .category=FunctionDocumentation::Category::TableFunction},
        .allow_readonly = false
    });

    factory.registerFunction<TableFunctionObjectStorage<COSNDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on COSN.)",
            .syntax="cosn(url, access_key_id, secret_access_key)",
            .arguments={
                {"url", ""},
                {"access_key_id", ""},
                {"secret_access_key", ""}
            },
            .returned_value="",
            .examples{{"cosn", "SELECT * FROM cosn(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    });
    factory.registerFunction<TableFunctionObjectStorage<OSSDefinition, StorageS3Configuration>>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the data stored on OSS.)",
            .syntax="oss(url, access_key_id, secret_access_key)",
            .arguments={
                {"url",""},
                {"access_key_id", ""},
                {"secret_access_key", ""}
            },
            .returned_value="",
            .examples{{"oss", "SELECT * FROM oss(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction
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
            .syntax="azureBlobStorage(- connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])",
            .arguments={
                {"connection_string|storage_account_url", "connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key)"},
                {"container_name", "Container name"},
                {"blobpath", "file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings."},
                {"account_name", "if storage_account_url is used, then account name can be specified here"},
                {"account_key", "if storage_account_url is used, then account key can be specified here"},
                {"format", "The [format](/sql-reference/formats) of the file."},
                {"compression", "Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`)."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."}
            },
            .returned_value="A table with the specified structure for reading or writing data in the specified file.",
            .examples{
            {
                "azureBlobStorage",
                "SELECT * FROM  azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, "
                "[account_name, account_key, format, compression, structure])", ""
            }},
            .category=FunctionDocumentation::Category::TableFunction
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
            .syntax="hdfs(URI, format, structure)",
            .arguments={
                {"URI", "The relative URI to the file in HDFS. Path to file support following globs in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc', 'def'` — strings."},
                {"format", "The [format](/sql-reference/formats) of the file."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."}
            },
            .returned_value="A table with the specified structure for reading or writing data in the specified file.",
            .examples{
            {
                "hdfs",
                "SELECT * FROM  hdfs(url, format, compression, structure])", ""
            }},
            .category=FunctionDocumentation::Category::TableFunction
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

#if USE_PARQUET && USE_AWS_S3 && USE_DELTA_KERNEL_RS
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
            .syntax = "iceberg(url, access_key_id, secret_access_key)",
            .arguments = {
                {"url", ""},
                {"access_key_id", ""},
                {"secret_access_key", ""}
            },
            .returned_value = "A table with the specified structure for reading data in the specified Iceberg table.",
            .examples{{"iceberg", "SELECT * FROM iceberg(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
    factory.registerFunction<TableFunctionIcebergS3>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store.)",
            .syntax = R"(
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
icebergS3(named_collection[, option=value [,..]])            
            )",
            .arguments={
                {"url",""},
                {"NOSIGN",""},
                {"access_key_id", ""},
                {"secret_access_key", ""},
                {"session_token", ""},
                {"format", ""},
                {"compression_method", ""},
                {"named_collection", ""},
                {"option=value", ""}
            },
            .returned_value = "A table with the specified structure for reading data in the specified Iceberg table.",
            .examples{{"icebergS3", "SELECT * FROM icebergS3(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});

#endif
#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzure>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on Azure object store.)",
            .syntax=R"(
icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])            
            )",
            .arguments = {
                {"connection_string|storage_account_url", ""},
                {"container_name", ""},
                {"blobpath", ""},
                {"account_name", ""},
                {"account_key", ""},
                {"format", ""},
                {"compression_method", ""},
                {"named_collection", ""},
                {"option=value", ""}
            },
            .returned_value = "A table with the specified structure for reading data in the specified Iceberg table.",
            .examples{{"icebergAzure", "SELECT * FROM icebergAzure(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFS>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem.)",
            .syntax=R"(
icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])
)",
            .arguments = {
                {"path_to_table", ""},
                {"format", ""},
                {"compression_method", ""},
                {"named_collection", ""},
                {"option=value", ""},
            },
            .returned_value = "A table with the specified structure for reading data in the specified Iceberg table.",
            .examples{{"icebergHDFS", "SELECT * FROM icebergHDFS(url)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
    factory.registerFunction<TableFunctionIcebergLocal>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored locally.)",
            .syntax = R"(
icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])            
            )",
            .arguments = {
                {"path_to_table", ""},
                {"format", ""},
                {"compression_method", ""},
                {"named_collection", ""},
                {"option=value", ""},
            },
            .returned_value="A table with the specified structure for reading data in the specified Iceberg table.",
            .examples{{"icebergLocal", "SELECT * FROM icebergLocal(filename)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
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
            .syntax=R"(
deltaLake(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

deltaLakeS3(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])

deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
         )",
            .arguments{
                {"cluster_name","Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"path", "The relative path to the file from user_files_path. Path to file also supports globs."},
                {"format","[Format](/sql-reference/formats) of the files. Type: String."},
                {"structure","Table structure in `UserID UInt64, Name String` format. Determines column names and types. Type: String."},
                {"compression_method","Compression method. Supported compression types are `gz`, `br`, `xz`, `zst`, `lz4`, and `bz2`."}
            },
            .returned_value="A table with the specified structure for reading data in the specified Delta Lake table."
            .examples{{"deltaLake", "SELECT * FROM deltaLake(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});

    factory.registerFunction<TableFunctionDeltaLakeS3>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on S3.)",
            .examples{{"deltaLakeS3", "SELECT * FROM deltaLakeS3(url, access_key_id, secret_access_key)", ""}},
            .category{""}},
         .allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionDeltaLakeAzure>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on Azure object store.)",
            .examples{{"deltaLakeAzure", "SELECT * FROM deltaLakeAzure(connection_string|storage_account_url, container_name, blobpath, \"\n"
 "                \"[account_name, account_key, format, compression, structure])", ""}},
            .category{""}},
         .allow_readonly = false});
#endif
}
#endif

#if USE_AWS_S3
void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
        {.documentation
         = {.description = R"(The table function can be used to read the Hudi table stored on object store.)",
            .syntax="hudi(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])",
            .arguments={
                {"url", "Bucket url with the path to an existing Hudi table in S3."},
                {"aws_access_key_id, aws_secret_access_key", "Long-term credentials for the AWS account user. You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see Using S3 for Data Storage."},
                {"format", "The format of the file."},
                {"structure", "Structure of the table. Format `column1_name column1_type, column2_name column2_type, ...`."},
                {"compression", "Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension."}
            },
            .returned_value="A table with the specified structure for reading data in the specified Hudi table in S3.",
            .examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction,
            .related{
                "[Hudi engine](/engines/table-engines/integrations/hudi)",
                "[Hudi cluster table function](/sql-reference/table-functions/hudiCluster)"
            }
            },
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
