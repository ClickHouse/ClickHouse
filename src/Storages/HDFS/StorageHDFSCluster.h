#pragma once

#include "config.h"

#if USE_HDFS

#include <memory>
#include <optional>

#include <Client/Connection.h>
#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/HDFS/StorageHDFS.h>

namespace DB
{

class Context;

class StorageHDFSCluster : public IStorageCluster
{
public:
    StorageHDFSCluster(
        ContextPtr context_,
        const String & cluster_name_,
        const String & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & compression_method);

    std::string getName() const override { return "HDFSCluster"; }

    RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

private:
    void updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context) override;

    String uri;
    String format_name;
};


}

#endif
