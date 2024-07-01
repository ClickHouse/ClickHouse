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
class StorageAzureConfiguration;

struct AzureClusterDefinition
{
    static constexpr auto name = "azureBlobStorageCluster";
    static constexpr auto storage_type_name = "AzureBlobStorageCluster";
    static constexpr auto signature = " - cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure]";
    static constexpr auto max_number_of_arguments = AzureDefinition::max_number_of_arguments + 1;
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
    static constexpr auto max_number_of_arguments = S3Definition::max_number_of_arguments + 1;
};

struct HDFSClusterDefinition
{
    static constexpr auto name = "hdfsCluster";
    static constexpr auto storage_type_name = "HDFSCluster";
    static constexpr auto signature = " - cluster_name, uri\n"
                                      " - cluster_name, uri, format\n"
                                      " - cluster_name, uri, format, structure\n"
                                      " - cluster_name, uri, format, structure, compression_method\n";
    static constexpr auto max_number_of_arguments = HDFSDefinition::max_number_of_arguments + 1;
};

/**
* Class implementing s3/hdfs/azureBlobStorage)Cluster(...) table functions,
* which allow to process many files from S3/HDFS/Azure blob storage on a specific cluster.
* On initiator it creates a connection to _all_ nodes in cluster, discloses asterisks
* in file path and dispatch each file dynamically.
* On worker node it asks initiator about next task to process, processes it.
* This is repeated until the tasks are finished.
*/
template <typename Definition, typename Configuration>
class TableFunctionObjectStorageCluster : public ITableFunctionCluster<TableFunctionObjectStorage<Definition, Configuration>>
{
public:
    static constexpr auto name = Definition::name;
    static constexpr auto signature = Definition::signature;

    String getName() const override { return name; }
    String getSignature() const override { return signature; }

protected:
    using Base = TableFunctionObjectStorage<Definition, Configuration>;

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return Definition::storage_type_name; }

    bool hasStaticStructure() const override { return Base::getConfiguration()->structure != "auto"; }

    bool needStructureHint() const override { return Base::getConfiguration()->structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { Base::structure_hint = structure_hint_; }
};

#if USE_AWS_S3
using TableFunctionS3Cluster = TableFunctionObjectStorageCluster<S3ClusterDefinition, StorageS3Configuration>;
#endif

#if USE_AZURE_BLOB_STORAGE
using TableFunctionAzureBlobCluster = TableFunctionObjectStorageCluster<AzureClusterDefinition, StorageAzureConfiguration>;
#endif

#if USE_HDFS
using TableFunctionHDFSCluster = TableFunctionObjectStorageCluster<HDFSClusterDefinition, StorageHDFSConfiguration>;
#endif
}
