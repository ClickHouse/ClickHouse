#pragma once

#include "config.h"

#if USE_AWS_S3

#include <memory>
#include <optional>

#include "Client/Connection.h"
#include <Interpreters/Cluster.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3.h>

namespace DB
{

class Context;

class StorageS3Cluster : public IStorage
{
public:
    StorageS3Cluster(
        const StorageS3ClusterConfiguration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override { return "S3Cluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, size_t /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

private:
    StorageS3::S3Configuration s3_configuration;

    String filename;
    String cluster_name;
    String format_name;
    String compression_method;
    NamesAndTypesList virtual_columns;
    Block virtual_block;
    bool add_columns_structure_to_query = false;
};


}

#endif
