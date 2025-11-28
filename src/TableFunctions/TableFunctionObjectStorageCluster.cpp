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

template <typename Definition, typename Configuration, bool is_data_lake>
StoragePtr TableFunctionObjectStorageCluster<Definition, Configuration, is_data_lake>::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    auto configuration = Base::getConfiguration(context);

    ColumnsDescription columns;
    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!Base::structure_hint.empty())
        columns = Base::structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    auto object_storage = Base::getObjectStorage(context, !is_insert_query);
    StoragePtr storage;

    const auto & client_info = context->getClientInfo();

    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        bool can_use_distributed_iterator =
            client_info.collaborate_with_initiator &&
            context->hasClusterFunctionReadTaskCallback();

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
            /* catalog*/nullptr,
            /* if_not_exists*/false,
            /* is_datalake_query*/ false,
            /* distributed_processing */ can_use_distributed_iterator,
            /* partition_by_ */Base::partition_by,
            /* is_table_function */true,
            /* lazy_init */ true);
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
            Base::partition_by,
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
            .examples{{"s3Cluster", "SELECT * FROM  s3Cluster(cluster, url, format, structure)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
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
                AzureClusterDefinition::name,
                "SELECT * FROM  azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, "
                "[account_name, account_key, format, compression, structure])", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionHDFSCluster>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on HDFS in parallel for many nodes in a specified cluster.)",
            .examples{{HDFSClusterDefinition::name, "SELECT * FROM HDFSCluster(cluster, uri, format)", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        .allow_readonly = false
    }
    );
#endif

    UNUSED(factory);
}


#if USE_AVRO
void registerTableFunctionIcebergCluster(TableFunctionFactory & factory)
{
    UNUSED(factory);

#if USE_AWS_S3
    factory.registerFunction<TableFunctionIcebergCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on store from disk in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergClusterDefinition::name, "SELECT * FROM icebergCluster(cluster) SETTINGS disk = 'disk'", ""},{IcebergClusterDefinition::name, "SELECT * FROM icebergCluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});

    factory.registerFunction<TableFunctionIcebergS3Cluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergS3ClusterDefinition::name, "SELECT * FROM icebergS3Cluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzureCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergAzureClusterDefinition::name, "SELECT * FROM icebergAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFSCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster.)",
            .examples{{IcebergHDFSClusterDefinition::name, "SELECT * FROM icebergHDFSCluster(cluster, uri, [format], [structure], [compression_method])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
}

void registerTableFunctionPaimonCluster(TableFunctionFactory & factory)
{
    UNUSED(factory);

#if USE_AWS_S3
    factory.registerFunction<TableFunctionPaimonCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Paimon table stored on store from disk in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonClusterDefinition::name, "SELECT * FROM paimonCluster(cluster) SETTINGS datalake_disk_name = 'disk'", ""},{PaimonClusterDefinition::name, "SELECT * FROM paimonCluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});

    factory.registerFunction<TableFunctionPaimonS3Cluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Paimon table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonS3ClusterDefinition::name, "SELECT * FROM paimonS3Cluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionPaimonAzureCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Paimon table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonAzureClusterDefinition::name, "SELECT * FROM paimonAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionPaimonHDFSCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Paimon table stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster.)",
            .examples{{PaimonHDFSClusterDefinition::name, "SELECT * FROM paimonHDFSCluster(cluster, uri, [format], [structure], [compression_method])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
}
#endif


#if USE_PARQUET
void registerTableFunctionDeltaLakeCluster(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AWS_S3 && USE_DELTA_KERNEL_RS
    factory.registerFunction<TableFunctionDeltaLakeCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on object store in parallel for many nodes in a specified cluster.)",
            .examples{{DeltaLakeClusterDefinition::name, "SELECT * FROM deltaLakeCluster(cluster, url, access_key_id, secret_access_key)", ""},{DeltaLakeClusterDefinition::name, "SELECT * FROM deltaLakeCluster(cluster) SETTINGS disk = 'disk'", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
    factory.registerFunction<TableFunctionDeltaLakeS3Cluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .examples{{DeltaLakeS3ClusterDefinition::name, "SELECT * FROM deltaLakeS3Cluster(cluster, url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionDeltaLakeAzureCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .examples{{DeltaLakeAzureClusterDefinition::name, "SELECT * FROM deltaLakeAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
}
#endif

#if USE_AWS_S3
void registerTableFunctionHudiCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudiCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Hudi table stored on object store in parallel for many nodes in a specified cluster.)",
            .examples{{HudiClusterDefinition::name, "SELECT * FROM hudiCluster(cluster, url, access_key_id, secret_access_key)", ""}},
            .category = FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
}
#endif

void registerDataLakeClusterTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AVRO
    registerTableFunctionIcebergCluster(factory);
    registerTableFunctionPaimonCluster(factory);
#endif
#if USE_PARQUET
    registerTableFunctionDeltaLakeCluster(factory);
#endif
#if USE_AWS_S3
    registerTableFunctionHudiCluster(factory);
#endif
}

}
