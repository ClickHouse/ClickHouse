#pragma once

#include "config.h"

#include <memory>
#include <optional>

#include <Client/Connection.h>
#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageURL.h>

namespace DB
{

class Context;

class StorageURLCluster : public IStorageCluster
{
public:
    StorageURLCluster(
        const ContextPtr & context,
        const String & cluster_name_,
        const String & uri_,
        const String & format_,
        const String & compression_method,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const StorageURL::Configuration & configuration_);

    std::string getName() const override { return "URLCluster"; }
    RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

private:
    void updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context) override;

    String uri;
    String format_name;
};


}

