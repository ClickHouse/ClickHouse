#pragma once
#include "config.h"

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>


namespace DB
{

class Context;

class StorageS3Settings;
class StorageAzureBlobSettings;
class StorageS3Configuration;
class StorageAzureConfiguration;

ContextPtr getQueryOrGlobalContext();

/**
* Class implementing s3/hdfs/azureBlobStorageCluster(...) table functions,
* which allow to process many files from S3/HDFS/Azure blob storage on a specific cluster.
* On initiator it creates a connection to _all_ nodes in cluster, discloses asterisks
* in file path and dispatch each file dynamically.
* On worker node it asks initiator about next task to process, processes it.
* This is repeated until the tasks are finished.
*/
template <typename Definition, typename Configuration, bool is_data_lake = false>
class TableFunctionObjectStorageCluster : public ITableFunctionCluster<TableFunctionObjectStorage<Definition, Configuration, is_data_lake>>
{
public:
    static constexpr auto name = Definition::name;

    String getName() const override { return name; }

protected:
    using Base = TableFunctionObjectStorage<Definition, Configuration, is_data_lake>;

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return Definition::storage_engine_name; }
    const char * getNonClusteredStorageEngineName() const override { return Definition::non_clustered_storage_engine_name; }
    bool hasStaticStructure() const override { return Base::getConfiguration(getQueryOrGlobalContext())->structure != "auto"; }
    bool needStructureHint() const override { return Base::getConfiguration(getQueryOrGlobalContext())->structure == "auto"; }
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

/// Datalake cluster table function aliases are defined in their own headers:
/// TableFunctionIcebergCluster.h, TableFunctionDeltaLakeCluster.h,
/// TableFunctionHudiCluster.h, TableFunctionPaimonCluster.h

}
