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

class StorageS3Cluster : public IStorageCluster
{
public:
    StorageS3Cluster(
        const String & cluster_name_,
        const StorageS3::Configuration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "S3Cluster"; }

    NamesAndTypesList getVirtuals() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization() const override { return true; }

protected:
    void updateConfigurationIfChanged(ContextPtr local_context);

private:
    void updateBeforeRead(const ContextPtr & context) override { updateConfigurationIfChanged(context); }

    void addColumnsStructureToQuery(ASTPtr & query, const String & structure, const ContextPtr & context) override;

    StorageS3::Configuration s3_configuration;
    NamesAndTypesList virtual_columns;
};


}

#endif
