#pragma once

#include "config.h"

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
        ContextPtr context_,
        const String & cluster_name_,
        const String & uri_,
        const String & format_,
        const String & compression_method_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const StorageURL::Configuration & configuration_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "URLCluster"; }

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

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

