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
        String cluster_name_,
        const String & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & compression_method_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "HDFSCluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, size_t /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

    ClusterPtr getCluster(ContextPtr context) const override;
    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, ContextPtr context) const override;

private:
    String cluster_name;
    String uri;
    String format_name;
    String compression_method;
    bool structure_argument_was_provided;
};


}

#endif
