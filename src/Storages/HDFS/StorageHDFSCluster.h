#pragma once

#include <Common/config.h>

#if USE_HDFS

#include <memory>
#include <optional>

#include <base/shared_ptr_helper.h>

#include <Client/Connection.h>
#include <Interpreters/Cluster.h>
#include <Storages/HDFS/StorageHDFS.h>

namespace DB
{

class Context;

class StorageHDFSCluster : public shared_ptr_helper<StorageHDFSCluster>, public IStorage
{
    friend struct shared_ptr_helper<StorageHDFSCluster>;
public:
    std::string getName() const override { return "HDFSCluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, unsigned /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageHDFSCluster(
        String cluster_name_,
        const String & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & compression_method_);

private:
    String cluster_name;
    String uri;
    String format_name;
    String compression_method;
};


}

#endif
