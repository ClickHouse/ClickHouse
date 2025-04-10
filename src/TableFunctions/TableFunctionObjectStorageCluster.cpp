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
            .syntax=R"(
s3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,structure] [,compression_method],[,headers])
s3Cluster(cluster_name, named_collection[, option=value [,..]])            
            )",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"url", "path to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path)."},
                {"NOSIGN", "If this keyword is provided in place of credentials, all the requests will not be signed."},
                {"access_key_id", "Keys that specify credentials to use with given endpoint. Optional."},
                {"secret_access_key", "Keys that specify credentials to use with given endpoint. Optional."},
                {"session_token", "Session token to use with the given keys. Optional when passing keys."},
                {"format", "The [format](/sql-reference/formats) of the file."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."},
                {"compression_method", "Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension."},
                {"headers", "Parameter is optional. Allows headers to be passed in the S3 request. Pass in the format `headers(key=value)` e.g. `headers('x-amz-request-payer' = 'requester')`. See [here](/sql-reference/table-functions/s3#accessing-requester-pays-buckets) for example of use."}
            },
            .returned_value="A table with the specified structure for reading or writing data in the specified file.",
            .examples{{"s3Cluster", "SELECT * FROM  s3Cluster(cluster, url, format, structure)", ""}},
            .category=FunctionDocumentation::Category::TableFunction
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
            .syntax="azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"connection_string|storage_account_url", "connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key)"},
                {"container_name", "Container name"},
                {"blobpath", "file path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings."},
                {"account_name", "if storage_account_url is used, then account name can be specified here"},
                {"account_key", "if storage_account_url is used, then account key can be specified here"},
                {"format", "The [format](/sql-reference/formats) of the file."},
                {"compression", "Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension. (same as setting to `auto`)."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."},
            },
            .returned_value="A table with the specified structure for reading or writing data in the specified file.",
            .examples{{
                "azureBlobStorageCluster",
                "SELECT * FROM  azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, "
                "[account_name, account_key, format, compression, structure])", ""}
            },
            .category=FunctionDocumentation::Category::TableFunction
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
            .syntax="hdfsCluster(cluster_name, URI, format, structure)",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"URI", "URI to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path)."},
                {"format", "The [format](/sql-reference/formats) of the file."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."}
            },
            .returned_value="A table with the specified structure for reading data in the specified file.",
            .examples{{"HDFSCluster", "SELECT * FROM HDFSCluster(cluster, uri, format)", ""}},
            .category=FunctionDocumentation::Category::TableFunction    
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
    factory.registerFunction<TableFunctionIcebergS3Cluster>(
        {.documentation = {
            .description = R"(The table function can be used to read the Iceberg table stored on S3 object store in parallel for many nodes in a specified cluster.)",
            .syntax=R"(
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])            
            )",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"url", ""},
                {"NOSIGN | access_key_id", ""},
                {"secret_access_key", ""},
                {"session_token", ""},
                {"format", ""},
                {"compression_method", ""}
            },
            .returned_value="A table with the specified structure for reading data from cluster in the specified Iceberg table.",
            .examples{{"icebergS3Cluster", "SELECT * FROM icebergS3Cluster(cluster, url, [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])", ""}},
            .category=FunctionDocumentation::Category::TableFunction
        },
         .allow_readonly = false});
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzureCluster>(
        {.documentation = {
            .description = R"(The table function can be used to read the Iceberg table stored on Azure object store in parallel for many nodes in a specified cluster.)",
            .syntax=R"(
icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])            
            )",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
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
            .returned_value="A table with the specified structure for reading data from cluster in the specified Iceberg table.",
            .examples{{"icebergAzureCluster", "SELECT * FROM icebergAzureCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])", ""}},
            .category=FunctionDocumentation::Category::TableFunction
        },
         .allow_readonly = false});
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFSCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster.)",
            .syntax=R"(
icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])            
            )",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"path_to_table", ""},
                {"format", ""}, 
                {"compression_method", ""},
                {"named_collection", ""},
                {"option=value", ""}
            },
            .returned_value="A table with the specified structure for reading data from cluster in the specified Iceberg table.",
            .examples{{"icebergHDFSCluster", "SELECT * FROM icebergHDFSCluster(cluster, uri, [format], [structure], [compression_method])", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
#endif
}
#endif

#if USE_AWS_S3
#if USE_PARQUET && USE_DELTA_KERNEL_RS
void registerTableFunctionDeltaLakeCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaLakeCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the DeltaLake table stored on object store in parallel for many nodes in a specified cluster.)",
            .syntax="deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])",
            .arguments={
                {"cluster_name", "Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"url", ""},
                {"aws_access_key_id", ""},
                {"aws_secret_access_key", ""},
                {"format", ""},
                {"structure", ""},
                {"compression", ""}
            },
            .returned_value="A table with the specified structure for reading data from cluster in the specified Delta Lake table in S3.",
            .examples{{"deltaLakeCluster", "SELECT * FROM deltaLakeCluster(cluster, url, access_key_id, secret_access_key)", ""}},
            .category{""}},
         .allow_readonly = false});
}
#endif

void registerTableFunctionHudiCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudiCluster>(
        {.documentation
         = {.description = R"(The table function can be used to read the Hudi table stored on object store in parallel for many nodes in a specified cluster.)",
            .syntax="hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression])",
            .arguments={
                {"cluster_name","Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers."},
                {"url", "Bucket url with the path to an existing Hudi table in S3."},
                {"aws_access_key_id", "Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)."},
                {"aws_secret_access_key", "Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3)."},
                {"format", "The [format](/interfaces/formats) of the file."},
                {"structure", "Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`."},
                {"compression", "Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, compression will be autodetected by the file extension."}
            },
            .returned_value="A table with the specified structure for reading data from cluster in the specified Hudi table in S3.",
            .examples{{"hudiCluster", "SELECT * FROM hudiCluster(cluster, url, access_key_id, secret_access_key)", ""}},
            .category=FunctionDocumentation::Category::TableFunction},
         .allow_readonly = false});
}
#endif

void registerDataLakeClusterTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AVRO
    registerTableFunctionIcebergCluster(factory);
#endif
#if USE_AWS_S3
#if USE_PARQUET && USE_DELTA_KERNEL_RS
    registerTableFunctionDeltaLakeCluster(factory);
#endif
    registerTableFunctionHudiCluster(factory);
#endif
}

}
