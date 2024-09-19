#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IKeyValueEntity.h>

#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/PODArray_fwd.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Backups/IBackup.h>
#include <Backups/WithRetries.h>

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
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;
    void drop() override;

    std::string getName() const override { return "KeeperMap"; }
    Names getPrimaryKey() const override { return {primary_key}; }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const override;
    Chunk getBySerializedKeys(
        std::span<const std::string> keys, PaddedPODArray<UInt8> * null_map, bool with_version, const ContextPtr & local_context) const;

    Block getSampleBlock(const Names &) const override;

    void checkTableCanBeRenamed(const StorageID & new_name) const override;
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;

    bool supportsParallelInsert() const override { return true; }
    bool supportsDelete() const override { return true; }

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    zkutil::ZooKeeperPtr getClient() const;
    const std::string & dataPath() const;
    std::string fullPathForKey(std::string_view key) const;

    UInt64 keysLimit() const;

    template <bool throw_on_error>
    void checkTable(const ContextPtr & local_context) const
    {
        auto current_table_status = getTableStatus(local_context);
        if (table_status == TableStatus::UNKNOWN)
        {
            static constexpr auto error_msg = "Failed to activate table because of connection issues. It will be activated "
                                                          "once a connection is established and metadata is verified";
            if constexpr (throw_on_error)
                throw Exception(ErrorCodes::INVALID_STATE, error_msg);
            else
            {
                LOG_ERROR(log, error_msg);
                return;
            }
        }

        if (current_table_status != TableStatus::VALID)
        {
            static constexpr auto error_msg
                = "Failed to activate table because of invalid metadata in ZooKeeper. Please DROP/DETACH table";
            if constexpr (throw_on_error)
                throw Exception(ErrorCodes::INVALID_STATE, error_msg);
            else
            {
                LOG_ERROR(log, error_msg);
                return;
            }
        }
    }

private:
    bool dropTable(zkutil::ZooKeeperPtr zookeeper, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock);

    enum class TableStatus : uint8_t
    {
        UNKNOWN,
        INVALID_METADATA,
        INVALID_KEEPER_STRUCTURE,
        VALID
    };

    TableStatus getTableStatus(const ContextPtr & context) const;

    void restoreDataImpl(
        const BackupPtr & backup,
        const String & data_path_in_backup,
        std::shared_ptr<WithRetries> with_retries,
        bool allow_non_empty_tables,
        const DiskPtr & temporary_disk);

    std::string zk_root_path;
    std::string primary_key;

    std::string zk_data_path;
    std::string zk_metadata_path;
    std::string zk_tables_path;

    std::string table_unique_id;
    std::string zk_table_path;

    std::string zk_dropped_path;
    std::string zk_dropped_lock_path;

    std::string zookeeper_name;

    std::string metadata_string;

    uint64_t keys_limit{0};

    mutable std::mutex zookeeper_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};

    mutable std::mutex init_mutex;

    mutable TableStatus table_status{TableStatus::UNKNOWN};

    LoggerPtr log;
};

}
