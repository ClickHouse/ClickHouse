#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Client/Connection.h>
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
        const ContextPtr & context_);

    std::string getName() const override { return "S3Cluster"; }

    RemoteQueryExecutor::Extension getTaskIteratorExtension(const ActionsDAG::Node * predicate, const ContextPtr & context) const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

protected:
    void updateConfigurationIfChanged(ContextPtr local_context);

private:
    void updateBeforeRead(const ContextPtr & context) override { updateConfigurationIfChanged(context); }

    void updateQueryToSendIfNeeded(ASTPtr & query, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context) override;

    StorageS3::Configuration s3_configuration;
};


}

#endif
