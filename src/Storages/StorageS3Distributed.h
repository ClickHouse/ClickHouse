#pragma once

#include "Client/Connection.h"
#include "Interpreters/Cluster.h"
#include "Storages/IStorage.h"
#include "Storages/StorageS3.h"

#include <memory>
#include <optional>
#include "ext/shared_ptr_helper.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class Context;


class StorageS3Distributed : public ext::shared_ptr_helper<StorageS3Distributed>, public IStorage 
{
    friend struct ext::shared_ptr_helper<StorageS3Distributed>;
public:
    std::string getName() const override { return "S3Distributed"; }

    Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;


protected:
    StorageS3Distributed(
        const S3::URI & uri_,
        const String & access_key_id_,
        const String & secret_access_key_,
        const StorageID & table_id_,
        String cluster_name_,
        const String & format_name_,
        UInt64 max_connections_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_,
        const String & compression_method_ = "");

private:
    /// Connections from initiator to other nodes
    std::vector<std::shared_ptr<Connection>> connections;
    std::string cluster_name;
    ClusterPtr cluster;

    /// This will be used on non-initiator nodes.
    std::optional<Cluster::Address> initiator;
    std::shared_ptr<Connection> initiator_connection;
    StorageS3::ClientAuthentificaiton client_auth;

    String format_name;
    String compression_method;
};


}
