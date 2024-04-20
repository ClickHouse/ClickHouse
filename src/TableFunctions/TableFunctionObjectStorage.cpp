#include "config.h"

#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/AzureBlob/Configuration.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/VirtualColumnUtils.h>
#include <Analyzer/TableFunctionNode.h>
#include <Formats/FormatFactory.h>
#include <Analyzer/FunctionNode.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Definition, typename Configuration>
ObjectStoragePtr TableFunctionObjectStorage<
    Definition, Configuration>::getObjectStorage(const ContextPtr & context, bool create_readonly) const
{
    if (!object_storage)
        object_storage = configuration->createObjectStorage(context, create_readonly);
    return object_storage;
}

template <typename Definition, typename Configuration>
StorageObjectStorageConfigurationPtr TableFunctionObjectStorage<
    Definition, Configuration>::getConfiguration() const
{
    if (!configuration)
        configuration = std::make_shared<Configuration>();
    return configuration;
}

template <typename Definition, typename Configuration>
std::vector<size_t> TableFunctionObjectStorage<
    Definition, Configuration>::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
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
void TableFunctionObjectStorage<Definition, Configuration>::updateStructureAndFormatArgumentsIfNeeded(
        ASTs & args, const String & structure, const String & format, const ContextPtr & context)
{
    Configuration().addStructureAndFormatToArgs(args, structure, format, context);
}

template <typename Definition, typename Configuration>
void TableFunctionObjectStorage<Definition, Configuration>::parseArgumentsImpl(ASTs & engine_args, const ContextPtr & local_context)
{
    StorageObjectStorageConfiguration::initialize(*getConfiguration(), engine_args, local_context, true);
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
    chassert(configuration);
    if (configuration->structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        auto storage = getObjectStorage(context, !is_insert_query);
        return StorageObjectStorage::getTableStructureFromData(storage, configuration, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration->structure, context);
}

template <typename Definition, typename Configuration>
bool TableFunctionObjectStorage<
    Definition, Configuration>::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    chassert(configuration);
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context);
}

template <typename Definition, typename Configuration>
std::unordered_set<String> TableFunctionObjectStorage<
    Definition, Configuration>::getVirtualsToCheckBeforeUsingStructureHint() const
{
    return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
}

template <typename Definition, typename Configuration>
StoragePtr TableFunctionObjectStorage<Definition, Configuration>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    ColumnsDescription columns;
    chassert(configuration);
    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    StoragePtr storage = std::make_shared<StorageObjectStorage>(
        configuration,
        getObjectStorage(context, !is_insert_query),
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        /// No format_settings for table function Azure
        std::nullopt,
        /* distributed_processing */ false,
        nullptr);

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
    factory.registerFunction<TableFunctionObjectStorage<AzureDefinition, StorageAzureBlobConfiguration>>(
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
        .allow_readonly = false
    });
#endif
}

#if USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorage<AzureDefinition, StorageAzureBlobConfiguration>;
template class TableFunctionObjectStorage<AzureClusterDefinition, StorageAzureBlobConfiguration>;
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

}
