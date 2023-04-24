#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

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
        Poco::Logger * log_,
        bool structure_argument_was_provided_);

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
              ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, size_t /*num_streams*/) override;

    ClusterPtr getCluster(ContextPtr context) const;
    /// Query is needed for pruning by virtual columns (_file, _path)
    virtual RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, ContextPtr context) const = 0;

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    bool isRemote() const override { return true; }

protected:
    virtual void updateBeforeRead(const ContextPtr &) {}

    virtual void addColumnsStructureToQuery(ASTPtr & query, const String & structure) = 0;

private:
    Poco::Logger * log;
    String cluster_name;
    bool structure_argument_was_provided;
};


}
