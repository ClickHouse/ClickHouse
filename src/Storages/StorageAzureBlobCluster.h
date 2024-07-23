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
        ContextPtr context_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "AzureBlobStorageCluster"; }

    NamesAndTypesList getVirtuals() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

private:
    void updateBeforeRead(const ContextPtr & /*context*/) override {}

    void addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context) override;

    StorageAzureBlob::Configuration configuration;
    NamesAndTypesList virtual_columns;
    std::unique_ptr<AzureObjectStorage> object_storage;
};


}

#endif
