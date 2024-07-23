#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>
#include <optional>

#include "Client/Connection.h"
#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageAzureBlob.h>

namespace DB
{

class Context;

class StorageAzureBlobCluster : public IStorageCluster
{
public:
    StorageAzureBlobCluster(
        const String & cluster_name_,
        const StorageAzureBlob::Configuration & configuration_,
        std::unique_ptr<AzureObjectStorage> && object_storage_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const ContextPtr & context);

    std::string getName() const override { return "AzureBlobStorageCluster"; }

    RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

private:
    void updateBeforeRead(const ContextPtr & /*context*/) override {}

    void updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context) override;

    StorageAzureBlob::Configuration configuration;
    std::unique_ptr<AzureObjectStorage> object_storage;
};


}

#endif
