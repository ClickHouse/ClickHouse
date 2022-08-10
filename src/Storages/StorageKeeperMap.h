#pragma once

#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IKVStorage.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/PODArray_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <span>

namespace DB
{

// KV store using (Zoo|CH)Keeper
class StorageKeeperMap final : public IKeyValueStorage
{
public:
    StorageKeeperMap(
        ContextPtr context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        std::string_view primary_key_,
        std::string_view root_path_,
        const std::string & hosts,
        bool create_missing_root_path,
        size_t keys_limit,
        bool remove_existing_data);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr & , ContextPtr, TableExclusiveLockHolder &) override;
    void drop() override;

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

    const std::string & lockPath() const;
    UInt64 keysLimit() const;

private:
    std::string root_path;
    std::string primary_key;
    std::string metadata_path;
    std::string lock_path;
    UInt64 keys_limit{0};

    mutable zkutil::ZooKeeperPtr zookeeper_client;

    Poco::Logger * log;
};

}
