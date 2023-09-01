#pragma once

#include <Common/config.h>

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
        const String & filename_,
        const String & access_key_id_,
        const String & secret_access_key_,
        const StorageID & table_id_,
        String cluster_name_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        const String & compression_method_);

    std::string getName() const override { return "S3Cluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, unsigned /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

    RemoteQueryExecutor::Extension getTaskIteratorExtension(ContextPtr context) const override;
    ClusterPtr getCluster(ContextPtr context) const override;
private:
    StorageS3::S3Configuration s3_configuration;
    String filename;
    String cluster_name;
    String format_name;
    String compression_method;
    NamesAndTypesList virtual_columns;
    Block virtual_block;

    mutable ClusterPtr cluster;
    mutable std::shared_ptr<StorageS3Source::DisclosedGlobIterator> iterator;
    mutable std::shared_ptr<StorageS3Source::IteratorWrapper> callback;

    void createIteratorAndCallback(ASTPtr query, ContextPtr context) const;
};


}

#endif
