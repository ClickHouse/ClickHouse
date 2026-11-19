#pragma once


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
        const StorageURL::Configuration & configuration_,
        bool send_not_overridable_);

    std::string getName() const override { return "URLCluster"; }

    RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ActionsDAG * /* filter */,
        const ContextPtr & context,
        ClusterPtr,
        StorageMetadataPtr) const override;

private:
    void updateQueryToSendIfNeeded(
        ASTPtr & query,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        const String & target_cluster_name,
        bool add_structure_and_format) override;

    String uri;
    String format_name;
    /// Whether a locked key is sent to the nodes, decided when the columns were chosen.
    const bool send_not_overridable;
    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
};


}

