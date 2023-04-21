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
        String cluster_name_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const StorageURL::Configuration & configuration_,
        size_t table_function_max_arguments,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "URLCluster"; }

    NamesAndTypesList getVirtuals() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, ContextPtr context) const override;

private:
    String uri;

};


}

