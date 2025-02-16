#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{


/**
 *  Base cluster for Storages used in table functions like s3Cluster and hdfsCluster
 *  Needed for code simplification around parallel_distributed_insert_select
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
    virtual RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const = 0;

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    bool isRemote() const final { return true; }
    bool supportsSubcolumns() const override  { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

protected:
    virtual void updateBeforeRead(const ContextPtr &) {}
    virtual void updateQueryToSendIfNeeded(ASTPtr & /*query*/, const StorageSnapshotPtr & /*storage_snapshot*/, const ContextPtr & /*context*/) {}

private:
    LoggerPtr log;
    String cluster_name;
};


}
