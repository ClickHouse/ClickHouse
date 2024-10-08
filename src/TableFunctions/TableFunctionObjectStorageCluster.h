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
};

struct S3ClusterDefinition
{
    static constexpr auto name = "s3Cluster";
    static constexpr auto storage_type_name = "S3Cluster";
};

struct HDFSClusterDefinition
{
    static constexpr auto name = "hdfsCluster";
    static constexpr auto storage_type_name = "HDFSCluster";
};

/**
* Class implementing s3/hdfs/azureBlobStorageCluster(...) table functions,
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

    String getName() const override { return name; }

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
