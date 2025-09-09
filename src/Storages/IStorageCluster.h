#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

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
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    ClusterPtr getCluster(ContextPtr context) const { return getClusterImpl(context, cluster_name); }

    /// Query is needed for pruning by virtual columns (_file, _path)
    virtual RemoteQueryExecutor::Extension getTaskIteratorExtension(
        const ActionsDAG::Node * predicate,
        const ContextPtr & context,
        ClusterPtr cluster) const = 0;

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    bool isRemote() const final { return true; }
    bool supportsSubcolumns() const override  { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    const String & getOriginalClusterName() const { return cluster_name; }
    virtual String getClusterName(ContextPtr /* context */) const { return getOriginalClusterName(); }

protected:
    virtual void updateBeforeRead(const ContextPtr &) {}
    virtual void updateQueryToSendIfNeeded(ASTPtr & /*query*/, const StorageSnapshotPtr & /*storage_snapshot*/, const ContextPtr & /*context*/) {}

    virtual void readFallBackToPure(
        QueryPlan & /* query_plan */,
        const Names & /* column_names */,
        const StorageSnapshotPtr & /* storage_snapshot */,
        SelectQueryInfo & /* query_info */,
        ContextPtr /* context */,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        size_t /* num_streams */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method readFallBackToPure is not supported by storage {}", getName());
    }
    
    virtual SinkToStoragePtr writeFallBackToPure(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*context*/,
        bool /*async_insert*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method writeFallBackToPure is not supported by storage {}", getName());
    }

private:
    static ClusterPtr getClusterImpl(ContextPtr context, const String & cluster_name_, size_t max_hosts = 0);

    LoggerPtr log;
    String cluster_name;
};


class ReadFromCluster : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromCluster"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromCluster(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<IStorageCluster> storage_,
        ASTPtr query_to_send_,
        QueryProcessingStage::Enum processed_stage_,
        ClusterPtr cluster_,
        LoggerPtr log_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , query_to_send(std::move(query_to_send_))
        , processed_stage(processed_stage_)
        , cluster(std::move(cluster_))
        , log(log_)
    {
    }

private:
    std::shared_ptr<IStorageCluster> storage;
    ASTPtr query_to_send;
    QueryProcessingStage::Enum processed_stage;
    ClusterPtr cluster;
    LoggerPtr log;

    std::optional<RemoteQueryExecutor::Extension> extension;

    void createExtension(const ActionsDAG::Node * predicate, size_t number_of_replicas);
    ContextPtr updateSettings(const Settings & settings);
};

}
