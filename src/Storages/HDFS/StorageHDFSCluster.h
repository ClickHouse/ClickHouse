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
        const String & compression_method_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "HDFSCluster"; }

    NamesAndTypesList getVirtuals() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

private:
    void addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context) override;

    String uri;
    String format_name;
    String compression_method;
    NamesAndTypesList virtual_columns;
};


}

#endif
