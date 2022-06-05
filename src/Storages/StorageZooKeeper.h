#pragma once

#include <Storages/IStorage.h>
#include "Common/ZooKeeper/ZooKeeper.h"
#include "ExternalDataSourceConfiguration.h"
#include "base/types.h"


namespace DB
{
class StorageZooKeeper final : public IStorage
{
public:
    StorageZooKeeper(
        const StorageID & table_id,
        const std::string & host_,
        const UInt16 & port_,
        const std::string & path_,
        const std::string & options_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "ZooKeeper"; }
    static StorageZooKeeperConfiguration getConfiguration(ASTs engine_args, ContextPtr context_);
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;
    


private:
    zkutil::ZooKeeperPtr getSession();
    const std::string host;
    const UInt16 port;
    const std::string path;
    const std::string options;
    zkutil::ZooKeeper zookeeper;
    zkutil::ZooKeeperPtr connection;
    // ContextSharedPart * shared;
    mutable std::mutex zookeeper_mutex;


};
}
