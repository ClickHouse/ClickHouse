#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IKeyValueEntity.h>

#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/PODArray_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <span>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
}

// KV store using (Zoo|CH)Keeper
class StorageKeeperMap final : public IStorage, public IKeyValueEntity, WithContext
{
public:
    StorageKeeperMap(
        ContextPtr context_,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        std::string_view primary_key_,
        const std::string & root_path_,
        UInt64 keys_limit_);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;
    void drop() override;

    std::string getName() const override { return "KeeperMap"; }
    Names getPrimaryKey() const override { return {primary_key}; }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const override;
    Chunk getBySerializedKeys(std::span<const std::string> keys, PaddedPODArray<UInt8> * null_map) const;

    Block getSampleBlock(const Names &) const override;

    void checkTableCanBeRenamed(const StorageID & new_name) const override;
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    bool supportsParallelInsert() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & node, ContextPtr /*query_context*/, const StorageMetadataPtr & /*metadata_snapshot*/) const override
    {
        return node->getColumnName() == primary_key;
    }

    zkutil::ZooKeeperPtr getClient() const;
    const std::string & dataPath() const;
    std::string fullPathForKey(std::string_view key) const;

    UInt64 keysLimit() const;

    template <bool throw_on_error>
    void checkTable() const
    {
        auto is_table_valid = isTableValid();
        if (!is_table_valid.has_value())
        {
            static constexpr std::string_view error_msg = "Failed to activate table because of connection issues. It will be activated "
                                                          "once a connection is established and metadata is verified";
            if constexpr (throw_on_error)
                throw Exception(ErrorCodes::INVALID_STATE, error_msg);
            else
            {
                LOG_ERROR(log, fmt::runtime(error_msg));
                return;
            }
        }

        if (!*is_table_valid)
        {
            static constexpr std::string_view error_msg
                = "Failed to activate table because of invalid metadata in ZooKeeper. Please DETACH table";
            if constexpr (throw_on_error)
                throw Exception(ErrorCodes::INVALID_STATE, error_msg);
            else
            {
                LOG_ERROR(log, fmt::runtime(error_msg));
                return;
            }
        }
    }

private:
    bool dropTable(zkutil::ZooKeeperPtr zookeeper, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock);

    std::optional<bool> isTableValid() const;

    std::string root_path;
    std::string primary_key;

    std::string data_path;

    std::string metadata_path;

    std::string tables_path;
    std::string table_path;

    std::string dropped_path;
    std::string dropped_lock_path;

    std::string zookeeper_name;

    std::string metadata_string;

    uint64_t keys_limit{0};

    mutable std::mutex zookeeper_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};

    mutable std::mutex init_mutex;
    mutable std::optional<bool> table_is_valid;

    Poco::Logger * log;
};

}
