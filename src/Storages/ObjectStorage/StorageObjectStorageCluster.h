#pragma once
#include <Storages/IStorageCluster.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB
{

class Context;

class StorageObjectStorageCluster : public IStorageCluster
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    StorageObjectStorageCluster(
        const String & cluster_name_,
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    String getPathSample(StorageInMemoryMetadata metadata, ContextPtr context);

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context) override;

    const String engine_name;
    const StorageObjectStorage::ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    NamesAndTypesList virtual_columns;
};

}
