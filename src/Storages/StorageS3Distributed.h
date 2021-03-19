#pragma once

#include "Client/Connection.h"
#include "Interpreters/Cluster.h"
#include "Storages/IStorage.h"

#include <memory>
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
    StorageS3Distributed(const StorageID & table_id_, std::string cluster_name_, const Context & context);

private:
    std::vector<std::shared_ptr<Connection>> connections;
    std::string cluster_name;
    ClusterPtr cluster;
};


}
