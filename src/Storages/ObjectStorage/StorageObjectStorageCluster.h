#pragma once

#include "config.h"

#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <TableFunctions/TableFunctionObjectStorageCluster.h>

namespace DB
{

class StorageS3Settings;
class StorageAzureBlobSettings;

class Context;

template <typename Definition, typename StorageSettings, typename Configuration>
class StorageObjectStorageCluster : public IStorageCluster
{
public:
    using Storage = StorageObjectStorage<StorageSettings>;

    StorageObjectStorageCluster(
        const String & cluster_name_,
        const Storage::ConfigurationPtr & configuration_,
        ObjectStoragePtr object_storage_,
        const String & engine_name_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return engine_name; }

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

    RemoteQueryExecutor::Extension
    getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

private:
    void updateBeforeRead(const ContextPtr & /* context */) override {}

    void addColumnsStructureToQuery(
        ASTPtr & query,
        const String & structure,
        const ContextPtr & context) override;

    const String & engine_name;
    const Storage::ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    NamesAndTypesList virtual_columns;
};


#if USE_AWS_S3
using StorageS3Cluster = StorageObjectStorageCluster<S3ClusterDefinition, S3StorageSettings, StorageS3Configuration>;
#endif
#if USE_AZURE_BLOB_STORAGE
using StorageAzureBlobCluster = StorageObjectStorageCluster<AzureClusterDefinition, AzureStorageSettings, StorageAzureBlobConfiguration>;
#endif
#if USE_HDFS
using StorageHDFSCluster = StorageObjectStorageCluster<HDFSClusterDefinition, HDFSStorageSettings, StorageHDFSConfiguration>;
#endif

}
