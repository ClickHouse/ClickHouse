#pragma once

#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IKVStorage.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include "Common/PODArray_fwd.h"
#include <Common/ZooKeeper/ZooKeeper.h>

#include <span>

namespace DB
{

// KV store using (Zoo|CH)Keeper
class StorageKeeperMap final : public IKeyValueStorage
{
public:
    // TODO(antonio2368): add setting to control creating if keeper_path doesn't exist
    StorageKeeperMap(
        ContextPtr context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata,
        std::string_view primary_key_,
        std::string_view keeper_path_,
        const std::string & hosts,
        bool create_missing_root_path);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    std::string getName() const override { return "KeeperMap"; }
    Names getPrimaryKey() const override { return {primary_key}; }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map) const override;
    Chunk getBySerializedKeys(std::span<const std::string> keys, PaddedPODArray<UInt8> * null_map) const;

    bool supportsParallelInsert() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & node, ContextPtr /*query_context*/, const StorageMetadataPtr & /*metadata_snapshot*/) const override
    {
        return node->getColumnName() == primary_key;
    }

    zkutil::ZooKeeperPtr & getClient() const;
    const std::string & rootKeeperPath() const;
    std::string fullPathForKey(std::string_view key) const;

private:
    std::string keeper_path;
    std::string primary_key;

    mutable zkutil::ZooKeeperPtr zookeeper_client;
};

}
