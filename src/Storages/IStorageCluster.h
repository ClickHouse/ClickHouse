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
class IStorageCluster: public IStorage
{
public:

    explicit IStorageCluster(const StorageID & table_id_) : IStorage(table_id_) {}

    virtual ClusterPtr getCluster(ContextPtr context) const = 0;
    virtual RemoteQueryExecutor::Extension getTaskIteratorExtension(ContextPtr context) const = 0;

    bool isRemote() const override { return true; }
};


}
