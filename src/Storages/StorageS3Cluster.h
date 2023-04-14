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
    struct Configuration : public StorageS3::Configuration
    {
        std::string cluster_name;
    };

    StorageS3Cluster(
        const Configuration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        bool structure_argument_was_provided_);

    std::string getName() const override { return "S3Cluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, size_t /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ASTPtr query, ContextPtr context) const override;
    ClusterPtr getCluster(ContextPtr context) const override;

protected:
    void updateConfigurationIfChanged(ContextPtr local_context);

private:
    Poco::Logger * log;
    StorageS3::Configuration s3_configuration;
    String cluster_name;
    String format_name;
    String compression_method;
    NamesAndTypesList virtual_columns;
    Block virtual_block;
    bool structure_argument_was_provided;
};


}

#endif
