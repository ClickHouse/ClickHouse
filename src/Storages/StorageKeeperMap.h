#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>

namespace DB
{

// KV store using (Zoo|CH)Keeper 
class StorageKeeperMap final : public IStorage
{
public:
    // TODO(antonio2368): add setting to control creating if keeper_path doesn't exist
    StorageKeeperMap(
            std::string_view keeper_path_,
            ContextPtr context,
            const StorageID & table_id
    );

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;
 
    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context) override;

    std::string getName() const override
    {
        return "KeeperMap";
    }

    static NamesAndTypesList getNamesAndTypes();

    zkutil::ZooKeeperPtr & getClient();

    const std::string & rootKeeperPath() const;
private:

    std::string keeper_path;
    zkutil::ZooKeeperPtr zookeeper_client;
};

}
