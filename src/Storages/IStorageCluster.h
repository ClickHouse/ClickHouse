#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;


/**
 *  Base cluster for Storages used in table functions like s3Cluster and hdfsCluster.
 *  Necessary for code simplification around parallel_distributed_insert_select.
 */
class IStorageCluster : public IStorage
{
public:
    IStorageCluster(
        const String & cluster_name_,
        const StorageID & table_id_,
        LoggerPtr log_);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    ClusterPtr getCluster(ContextPtr context) const;

    /// Query is needed for pruning by virtual columns (_file, _path)
    virtual RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ActionsDAG * filter_actions_dag,
        const ContextPtr & context,
        ClusterPtr cluster,
        StorageMetadataPtr storage_metadata_snapshot) const
        = 0;

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    bool isRemote() const final { return true; }
    bool supportsSubcolumns() const override  { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    /// Prepare the `SELECT ... FROM f(...)` query (`f` is a table function) for the other nodes of the cluster: add the
    /// structure and format arguments so that the nodes do not infer the schema again, and turn a plain table
    /// function (`url`, `s3`, ...) that `parallel_replicas_for_cluster_engines` converted into this cluster
    /// storage into its `*Cluster` variant with the cluster name argument, so that the nodes take their read
    /// tasks from the initiator instead of reading every file on their own. Called by `read` and by the
    /// distributed `INSERT ... SELECT` in `InterpreterInsertQuery`, which forwards the query the same way.
    virtual void updateQueryToSendIfNeeded(ASTPtr & /*query*/, const StorageSnapshotPtr & /*storage_snapshot*/, const ContextPtr & /*context*/) {}

protected:
    virtual void updateBeforeRead(const ContextPtr &) {}

    virtual void updateConfigurationIfNeeded(ContextPtr /* context */) {}

private:
    LoggerPtr log;
    String cluster_name;
};


}
