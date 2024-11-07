#include "config.h"

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>


namespace DB
{

template <typename Definition, typename Configuration>
StoragePtr TableFunctionObjectStorageCluster<Definition, Configuration>::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    auto configuration = Base::getConfiguration();

    ColumnsDescription columns;
    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!Base::structure_hint.empty())
        columns = Base::structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    auto object_storage = Base::getObjectStorage(context, !is_insert_query);
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageObjectStorage>(
            configuration,
            object_storage,
            context,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */ String{},
            /* format_settings */ std::nullopt, /// No format_settings
            /* mode */ LoadingStrictnessLevel::CREATE,
            /* distributed_processing */ true,
            /*partition_by_=*/nullptr);
    }
    else
    {
        storage = std::make_shared<StorageObjectStorageCluster>(
            ITableFunctionCluster<Base>::cluster_name,
            configuration,
            object_storage,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            context);
    }

    storage->startup();
    return storage;
}


void registerTableFunctionObjectStorageCluster(TableFunctionFactory & factory)
{
#if USE_AWS_S3
    factory.registerFunction<TableFunctionS3Cluster>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on S3 in parallel for many nodes in a specified cluster.)",
            .examples{{"s3Cluster", "SELECT * FROM  s3Cluster(cluster, url, format, structure)", ""}}},
            .allow_readonly = false
        }
    );
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionAzureBlobCluster>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on Azure Blob Storage in parallel for many nodes in a specified cluster.)",
            .examples{{
                "azureBlobStorageCluster",
                "SELECT * FROM  azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, "
                "[account_name, account_key, format, compression, structure])", ""}}},
            .allow_readonly = false
        }
    );
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionHDFSCluster>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on HDFS in parallel for many nodes in a specified cluster.)",
            .examples{{"HDFSCluster", "SELECT * FROM HDFSCluster(cluster_name, uri, format)", ""}}},
            .allow_readonly = false
        }
    );
#endif

    UNUSED(factory);
}

#if USE_AWS_S3
template class TableFunctionObjectStorageCluster<S3ClusterDefinition, StorageS3Configuration>;
#endif

#if USE_AZURE_BLOB_STORAGE
template class TableFunctionObjectStorageCluster<AzureClusterDefinition, StorageAzureConfiguration>;
#endif

#if USE_HDFS
template class TableFunctionObjectStorageCluster<HDFSClusterDefinition, StorageHDFSConfiguration>;
#endif
}
