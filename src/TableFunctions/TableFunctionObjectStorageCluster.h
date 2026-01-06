#pragma once
#include "config.h"

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>
#include <Common/CurrentThread.h>


namespace DB
{

class Context;

class StorageS3Settings;
class StorageAzureBlobSettings;
class StorageS3Configuration;
class StorageAzureConfiguration;

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
private:
    static ContextPtr getQueryOrGlobalContext()
    {
        if (auto query_context = CurrentThread::getQueryContext(); query_context != nullptr)
            return query_context;
        return Context::getGlobalContextInstance();
    }
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

#if USE_AVRO && USE_AWS_S3
using TableFunctionIcebergS3Cluster = TableFunctionObjectStorageCluster<IcebergS3ClusterDefinition, StorageS3IcebergConfiguration, true>;
using TableFunctionIcebergCluster = TableFunctionObjectStorageCluster<IcebergClusterDefinition, StorageS3IcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
using TableFunctionIcebergAzureCluster = TableFunctionObjectStorageCluster<IcebergAzureClusterDefinition, StorageAzureIcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_HDFS
using TableFunctionIcebergHDFSCluster = TableFunctionObjectStorageCluster<IcebergHDFSClusterDefinition, StorageHDFSIcebergConfiguration, true>;
#endif

#if USE_AVRO && USE_AWS_S3
using TableFunctionPaimonS3Cluster = TableFunctionObjectStorageCluster<PaimonS3ClusterDefinition, StorageS3PaimonConfiguration, true>;
using TableFunctionPaimonCluster = TableFunctionObjectStorageCluster<PaimonClusterDefinition, StorageS3PaimonConfiguration, true>;
#endif

#if USE_AVRO && USE_AZURE_BLOB_STORAGE
using TableFunctionPaimonAzureCluster = TableFunctionObjectStorageCluster<PaimonAzureClusterDefinition, StorageAzurePaimonConfiguration, true>;
#endif

#if USE_AVRO && USE_HDFS
using TableFunctionPaimonHDFSCluster = TableFunctionObjectStorageCluster<PaimonHDFSClusterDefinition, StorageHDFSPaimonConfiguration, true>;
#endif


#if USE_AWS_S3 && USE_PARQUET && USE_DELTA_KERNEL_RS
using TableFunctionDeltaLakeCluster = TableFunctionObjectStorageCluster<DeltaLakeClusterDefinition, StorageS3DeltaLakeConfiguration, true>;
using TableFunctionDeltaLakeS3Cluster = TableFunctionObjectStorageCluster<DeltaLakeS3ClusterDefinition, StorageS3DeltaLakeConfiguration, true>;
#endif

#if USE_PARQUET && USE_AZURE_BLOB_STORAGE
using TableFunctionDeltaLakeAzureCluster = TableFunctionObjectStorageCluster<DeltaLakeAzureClusterDefinition, StorageAzureDeltaLakeConfiguration, true>;
#endif

#if USE_AWS_S3
using TableFunctionHudiCluster = TableFunctionObjectStorageCluster<HudiClusterDefinition, StorageS3HudiConfiguration, true>;
#endif

}
