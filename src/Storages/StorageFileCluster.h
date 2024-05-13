#pragma once

#include <Client/Connection.h>
#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageURL.h>

#include <memory>
#include <optional>

namespace DB
{

class Context;

class StorageFileCluster : public IStorageCluster
{
public:
    StorageFileCluster(
        const ContextPtr & context_,
        const String & cluster_name_,
        const String & filename_,
        const String & format_name_,
        const String & compression_method_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_);

    std::string getName() const override { return "FileCluster"; }
    RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

private:
    void updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context) override;

    Strings paths;
    String filename;
    String format_name;
};

}
