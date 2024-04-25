#pragma once

#include <Client/Connection.h>
#include <Interpreters/Cluster.h>
#include <Storages/IStorageCluster.h>
#include <Storages/StorageURL.h>

#include <memory>
#include <optional>

namespace DB
{

class Context;

class StorageFileCluster : public IStorageCluster
{
public:
    StorageFileCluster(
        ContextPtr context_,
        const String & cluster_name_,
        const String & filename_,
        const String & format_name_,
        const String & compression_method_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "FileCluster"; }

    NamesAndTypesList getVirtuals() const override { return virtual_columns; }

    RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

private:
    void addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context) override;

    Strings paths;
    String filename;
    String format_name;
    String compression_method;
    NamesAndTypesList virtual_columns;
};

}
