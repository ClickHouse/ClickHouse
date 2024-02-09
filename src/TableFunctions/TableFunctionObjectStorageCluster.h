#pragma once
#include "config.h"
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <TableFunctions/TableFunctionObjectStorage.h>


namespace DB
{

class Context;

class StorageS3Settings;
class StorageAzureBlobSettings;
class StorageS3Configuration;
class StorageAzureBlobConfiguration;

struct AzureClusterDefinition
{
    /**
    * azureBlobStorageCluster(cluster_name, source, [access_key_id, secret_access_key,] format, compression_method, structure)
    * A table function, which allows to process many files from Azure Blob Storage on a specific cluster
    * On initiator it creates a connection to _all_ nodes in cluster, discloses asterisks
    * in Azure Blob Storage file path and dispatch each file dynamically.
    * On worker node it asks initiator about next task to process, processes it.
    * This is repeated until the tasks are finished.
    */
    static constexpr auto name = "azureBlobStorageCluster";
    static constexpr auto storage_type_name = "AzureBlobStorageCluster";
    static constexpr auto signature = " - cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure]";
};

struct S3ClusterDefinition
{
    static constexpr auto name = "s3Cluster";
    static constexpr auto storage_type_name = "S3Cluster";
    static constexpr auto signature = " - cluster, url\n"
                                      " - cluster, url, format\n"
                                      " - cluster, url, format, structure\n"
                                      " - cluster, url, access_key_id, secret_access_key\n"
                                      " - cluster, url, format, structure, compression_method\n"
                                      " - cluster, url, access_key_id, secret_access_key, format\n"
                                      " - cluster, url, access_key_id, secret_access_key, format, structure\n"
                                      " - cluster, url, access_key_id, secret_access_key, format, structure, compression_method\n"
                                      " - cluster, url, access_key_id, secret_access_key, session_token, format, structure, compression_method\n"
                                      "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";
};

struct HDFSClusterDefinition
{
    static constexpr auto name = "hdfsCluster";
    static constexpr auto storage_type_name = "HDFSCluster";
    static constexpr auto signature = " - cluster_name, uri\n"
                                      " - cluster_name, uri, format\n"
                                      " - cluster_name, uri, format, structure\n"
                                      " - cluster_name, uri, format, structure, compression_method\n";
};

template <typename Definition, typename StorageSettings, typename Configuration>
class TableFunctionObjectStorageCluster : public ITableFunctionCluster<TableFunctionObjectStorage<Definition, StorageSettings, Configuration>>
{
public:
    static constexpr auto name = Definition::name;
    static constexpr auto signature = Definition::signature;

    String getName() const override { return name; }
    String getSignature() const override { return signature; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return Definition::storage_type_name; }
};

#if USE_AWS_S3
using TableFunctionS3Cluster = TableFunctionObjectStorageCluster<S3ClusterDefinition, S3StorageSettings, StorageS3Configuration>;
#endif

#if USE_AZURE_BLOB_STORAGE
using TableFunctionAzureBlobCluster = TableFunctionObjectStorageCluster<AzureClusterDefinition, AzureStorageSettings, StorageAzureBlobConfiguration>;
#endif

#if USE_HDFS
using TableFunctionHDFSCluster = TableFunctionObjectStorageCluster<HDFSClusterDefinition, HDFSStorageSettings, StorageHDFSConfiguration>;
#endif
}
