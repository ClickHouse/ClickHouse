#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <memory>
#include <optional>

#include <base/shared_ptr_helper.h>

#include "Client/Connection.h"
#include <Interpreters/Cluster.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3.h>

namespace DB
{

class Context;

class StorageS3Cluster : public shared_ptr_helper<StorageS3Cluster>, public IStorage
{
    friend struct shared_ptr_helper<StorageS3Cluster>;
public:
    std::string getName() const override { return "S3Cluster"; }

    Pipe read(const Names &, const StorageSnapshotPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, unsigned /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageS3Cluster(
        const String & filename_,
        const String & access_key_id_,
        const String & secret_access_key_,
        const StorageID & table_id_,
        String cluster_name_,
        const String & format_name_,
        UInt64 max_connections_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_,
        const String & compression_method_);

private:
    StorageS3::ClientAuthentication client_auth;

    String filename;
    String cluster_name;
    String format_name;
    String compression_method;
};


}

#endif
