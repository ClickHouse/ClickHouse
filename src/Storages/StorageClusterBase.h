#pragma once

#include "config.h"

#if USE_AWS_S3

#include <memory>
#include <optional>

#include "Client/Connection.h"
#include <Interpreters/Cluster.h>
#include <IO/S3Common.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageS3.h>

namespace DB
{

class Context;

class StorageClusterBase : public IStorageCluster
{
public:
    StorageClusterBase(
        String cluster_name,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        bool structure_argument_was_provided_);

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
              ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, size_t /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, ContextPtr context) const override;
    ClusterPtr getCluster(ContextPtr context) const override;

private:
    Poco::Logger * log;
    String cluster_name;
    bool structure_argument_was_provided;
};


}

#endif
