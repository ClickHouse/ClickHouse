#include <Core/Settings.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorageClusterFallback.h>
#include <Common/typeid_cast.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

namespace DB
{

namespace Setting
{
    extern const SettingsString object_storage_cluster;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct S3ClusterFallbackDefinition
{
    static constexpr auto name = "s3";
    static constexpr auto storage_type_name = "S3";
    static constexpr auto storage_type_cluster_name = "S3Cluster";
};

struct AzureClusterFallbackDefinition
{
    static constexpr auto name = "azureBlobStorage";
    static constexpr auto storage_type_name = "Azure";
    static constexpr auto storage_type_cluster_name = "AzureBlobStorageCluster";
};

struct HDFSClusterFallbackDefinition
{
    static constexpr auto name = "hdfs";
    static constexpr auto storage_type_name = "HDFS";
    static constexpr auto storage_type_cluster_name = "HDFSCluster";
};

struct IcebergS3ClusterFallbackDefinition
{
    static constexpr auto name = "icebergS3";
    static constexpr auto storage_type_name = "S3";
    static constexpr auto storage_type_cluster_name = "IcebergS3Cluster";
};

struct IcebergAzureClusterFallbackDefinition
{
    static constexpr auto name = "icebergAzure";
    static constexpr auto storage_type_name = "Azure";
    static constexpr auto storage_type_cluster_name = "IcebergAzureCluster";
};

struct IcebergHDFSClusterFallbackDefinition
{
    static constexpr auto name = "icebergHDFS";
    static constexpr auto storage_type_name = "HDFS";
    static constexpr auto storage_type_cluster_name = "IcebergHDFSCluster";
};

struct DeltaLakeClusterFallbackDefinition
{
    static constexpr auto name = "deltaLake";
    static constexpr auto storage_type_name = "S3";
    static constexpr auto storage_type_cluster_name = "DeltaLakeS3Cluster";
};

struct HudiClusterFallbackDefinition
{
    static constexpr auto name = "hudi";
    static constexpr auto storage_type_name = "S3";
    static constexpr auto storage_type_cluster_name = "HudiS3Cluster";
};

template <typename Definition, typename Base>
void TableFunctionObjectStorageClusterFallback<Definition, Base>::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (args.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The function {} should have arguments. The first argument must be the cluster name and the rest are the arguments of "
            "corresponding table function",
            getName());

    const auto & settings = context->getSettingsRef();

    is_cluster_function = !settings[Setting::object_storage_cluster].value.empty();

    if (is_cluster_function)
    {
        ASTPtr cluster_name_arg = std::make_shared<ASTLiteral>(settings[Setting::object_storage_cluster].value);
        args.insert(args.begin(), cluster_name_arg);
        BaseCluster::parseArgumentsImpl(args, context);
        args.erase(args.begin());
    }
    else
        BaseSimple::parseArgumentsImpl(args, context);
}

template <typename Definition, typename Base>
StoragePtr TableFunctionObjectStorageClusterFallback<Definition, Base>::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    if (is_cluster_function)
    {
        auto result = BaseCluster::executeImpl(ast_function, context, table_name, cached_columns, is_insert_query);
        if (auto storage = typeid_cast<std::shared_ptr<StorageObjectStorageCluster>>(result))
            storage->setClusterNameInSettings(true);
        return result;
    }
    else
        return BaseSimple::executeImpl(ast_function, context, table_name, cached_columns, is_insert_query);
}

#if USE_AWS_S3
using TableFunctionS3ClusterFallback = TableFunctionObjectStorageClusterFallback<S3ClusterFallbackDefinition, TableFunctionS3Cluster>;
#endif

#if USE_AZURE_BLOB_STORAGE
using TableFunctionAzureClusterFallback = TableFunctionObjectStorageClusterFallback<AzureClusterFallbackDefinition, TableFunctionAzureBlobCluster>;
#endif

#if USE_HDFS
using TableFunctionHDFSClusterFallback = TableFunctionObjectStorageClusterFallback<HDFSClusterFallbackDefinition, TableFunctionHDFSCluster>;
#endif

#if USE_AVRO && USE_AWS_S3
using TableFunctionIcebergS3ClusterFallback = TableFunctionObjectStorageClusterFallback<IcebergS3ClusterFallbackDefinition, TableFunctionIcebergS3Cluster>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
using TableFunctionIcebergAzureClusterFallback = TableFunctionObjectStorageClusterFallback<IcebergAzureClusterFallbackDefinition, TableFunctionIcebergAzureCluster>;
#endif

#if USE_AVRO && USE_HDFS
using TableFunctionIcebergHDFSClusterFallback = TableFunctionObjectStorageClusterFallback<IcebergHDFSClusterFallbackDefinition, TableFunctionIcebergHDFSCluster>;
#endif

#if USE_AWS_S3 && USE_PARQUET
using TableFunctionDeltaLakeClusterFallback = TableFunctionObjectStorageClusterFallback<DeltaLakeClusterFallbackDefinition, TableFunctionDeltaLakeCluster>;
#endif

#if USE_AWS_S3
using TableFunctionHudiClusterFallback = TableFunctionObjectStorageClusterFallback<HudiClusterFallbackDefinition, TableFunctionHudiCluster>;
#endif

void registerTableFunctionObjectStorageClusterFallback(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AWS_S3
    factory.registerFunction<TableFunctionS3ClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on S3 in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {"s3", "SELECT * FROM s3(url, format, structure)", ""},
                {"s3", "SELECT * FROM s3(url, format, structure) SETTINGS object_storage_cluster='cluster'", ""}
            },
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionAzureClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on Azure Blob Storage in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "azureBlobStorage",
                    "SELECT * FROM azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, "
                    "[account_name, account_key, format, compression, structure])", ""
                },
                {
                    "azureBlobStorage",
                    "SELECT * FROM azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, "
                    "[account_name, account_key, format, compression, structure]) "
                    "SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_HDFS
    factory.registerFunction<TableFunctionHDFSClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the data stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "hdfs",
                    "SELECT * FROM hdfs(url, format, compression, structure])", ""
                },
                {
                    "hdfs",
                    "SELECT * FROM hdfs(url, format, compression, structure]) "
                    "SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_AVRO && USE_AWS_S3
    factory.registerFunction<TableFunctionIcebergS3ClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the Iceberg table stored on S3 object store in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "icebergS3",
                    "SELECT * FROM icebergS3(url, access_key_id, secret_access_key)", ""
                },
                {
                    "icebergS3",
                    "SELECT * FROM icebergS3(url, access_key_id, secret_access_key) "
                    "SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
    factory.registerFunction<TableFunctionIcebergAzureClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the Iceberg table stored on Azure object store in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "icebergAzure",
                    "SELECT * FROM icebergAzure(url, access_key_id, secret_access_key)", ""
                },
                {
                    "icebergAzure",
                    "SELECT * FROM icebergAzure(url, access_key_id, secret_access_key) "
                    "SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_AVRO && USE_HDFS
    factory.registerFunction<TableFunctionIcebergHDFSClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the Iceberg table stored on HDFS virtual filesystem in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "icebergHDFS",
                    "SELECT * FROM icebergHDFS(url)", ""
                },
                {
                    "icebergHDFS",
                    "SELECT * FROM icebergHDFS(url) SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_AWS_S3 && USE_PARQUET
    factory.registerFunction<TableFunctionDeltaLakeClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the DeltaLake table stored on object store in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "deltaLake",
                    "SELECT * FROM deltaLake(url, access_key_id, secret_access_key)", ""
                },
                {
                    "deltaLake",
                    "SELECT * FROM deltaLake(url, access_key_id, secret_access_key) "
                    "SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif

#if USE_AWS_S3
    factory.registerFunction<TableFunctionHudiClusterFallback>(
    {
        .documentation = {
            .description=R"(The table function can be used to read the Hudi table stored on object store in parallel for many nodes in a specified cluster or from single node.)",
            .examples{
                {
                    "hudi",
                    "SELECT * FROM hudi(url, access_key_id, secret_access_key)", ""
                },
                {
                    "hudi",
                    "SELECT * FROM hudi(url, access_key_id, secret_access_key) SETTINGS object_storage_cluster='cluster'", ""
                },
            }
        },
        .allow_readonly = false
    }
    );
#endif
}

}
