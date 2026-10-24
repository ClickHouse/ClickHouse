#pragma once

#include <functional>
#include <mutex>

#include <Storages/StorageProxy.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
}

/// Lazily creates underlying storage for tables in databases with `lazy_load_tables` setting.
/// Similar to `StorageTableFunctionProxy`, but for real on-disk tables.
class StorageTableProxy final : public StorageProxy
{
public:
    StorageTableProxy(const StorageID & table_id_, std::function<StoragePtr()> get_nested_, ColumnsDescription cached_columns)
        : StorageProxy(table_id_)
        , get_nested(std::move(get_nested_))
        , log(getLogger("StorageTableProxy (" + table_id_.getFullTableName() + ")"))
    {
        StorageInMemoryMetadata cached_metadata;
        cached_metadata.setColumns(std::move(cached_columns));
        setInMemoryMetadata(cached_metadata);
    }

    std::string getName() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->getName();
        return "TableProxy";
    }

    /// Same forwarding as `getInMemoryMetadataPtr` below, for the same reason: the proxy's own
    /// metadata is seeded from the `CREATE TABLE` query and never carries a unique key.
    bool hasUniqueKey() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->hasUniqueKey();
        return IStorage::hasUniqueKey();
    }

    /// Forward the metadata query to the nested storage once it has been materialized.
    /// `IStorage::metadata` on the proxy itself is only seeded with the columns from the
    /// `CREATE TABLE` query and is updated lazily in `StorageProxy::alter` *after*
    /// `nested->alter` returns. With long-running alters (e.g. `RENAME COLUMN`
    /// while merges are stopped), the nested storage's in-memory metadata can be
    /// updated by `setProperties` while the proxy's cached copy still reflects the
    /// pre-alter schema. A concurrent `INSERT` would then resolve column names against
    /// the proxy's stale metadata but build the sink from the nested's current metadata,
    /// causing a `Block structure mismatch` `LOGICAL_ERROR` in `Chain::addSink`.
    /// Forwarding here keeps metadata observers in sync with the nested storage.
    StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr context_, bool bypass_metadata_cache) const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->getInMemoryMetadataPtr(context_, bypass_metadata_cache);
        return IStorage::getInMemoryMetadataPtr(context_, bypass_metadata_cache);
    }

    /// The underlying storage if it has already been materialized, and nullptr otherwise. Lets a caller
    /// that recognizes an engine by downcasting see through this stand-in without loading a table that
    /// has not been accessed yet.
    StoragePtr tryGetNested() const override
    {
        std::lock_guard lock{nested_mutex};
        return nested;
    }

    bool isLazyStandIn() const override { return true; }

    /// The same, but never waits for a materialization: `nested_mutex` is held for the whole first-access
    /// materialization, which is unbounded (it reads the data parts and starts the table up). A caller
    /// that holds a wider lock - the database mutex in `DatabaseWithOwnTablesBase::getTablesIterator` -
    /// must not block on it, or a single table being loaded stalls every query on the database. So this
    /// reads `nested` under `nested_ptr_mutex`, which is only held to copy the pointer. A table that is
    /// being materialized right now is reported as not materialized yet, exactly as an untouched one is,
    /// but a table that is already materialized is always reported as such, whatever other thread holds
    /// `nested_mutex` at the moment: `DatabaseAtomic::detachTable` relies on it to track the right object.
    StoragePtr tryGetNestedWithoutWaiting() const
    {
        std::lock_guard lock{nested_ptr_mutex};
        return nested;
    }

    /// Only meaningful for a materialized table: for an untouched one there is nothing to notify, and
    /// waking the background tasks of a table that nobody has asked for would load every lazy table.
    void onActionLockRemove(StorageActionBlockType action_type) override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->onActionLockRemove(action_type);
    }

    /// The analyzer and the interpreters refresh the external metadata (e.g. the current snapshot and schema of
    /// an Iceberg table) on the catalog object, which is this stand-in, so forward it to a materialized table,
    /// or it keeps the state it was loaded with forever. An untouched table is left alone: it reads the current
    /// state when it is materialized, and the refresh is also asked for by `DESCRIBE`, which must not load it.
    void updateExternalDynamicMetadataIfExists(ContextPtr context_) override
    {
        if (auto materialized = tryGetNested())
            materialized->updateExternalDynamicMetadataIfExists(context_);
    }

    /// Asked by every interpreter inside a transaction before the operation, which materializes the
    /// nested table anyway - a lazy `MergeTree` table must not be rejected as an engine without them.
    bool supportsTransactions() const override { return getNested()->supportsTransactions(); }

    /// Forwarded here rather than in `StorageProxy`: there `getNested` of `StorageTableFunctionProxy` would run
    /// the table function, which must not happen for `BACKUP` of a table whose source is missing.
    /// Not forwarding these makes `BACKUP` write the table definition with no data at all, and the
    /// restore of such a backup silently succeeds with an empty table. `supportsBackupPartition` is
    /// answered by `BackupEntriesCollector` and `RestorerFromBackup` before they reach the two data
    /// methods below, so it has to be forwarded as well - otherwise `BACKUP TABLE ... PARTITION ...`
    /// is rejected with `Table engine TableProxy doesn't support partitions`.
    bool supportsBackupPartition() const override { return getNested()->supportsBackupPartition(); }

    /// `DatabaseWithOwnTablesBase::getTablesForBackup` calls it on the catalog object before the data is
    /// collected, e.g. `ReplicatedMergeTree` puts the current metadata from ZooKeeper into the backed up
    /// `CREATE TABLE` query here.
    void applyMetadataChangesToCreateQueryForBackup(const ASTPtr & create_query) const override
    {
        getNested()->applyMetadataChangesToCreateQueryForBackup(create_query);
    }

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override
    {
        getNested()->backupData(backup_entries_collector, data_path_in_backup, partitions);
    }

    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override
    {
        getNested()->restoreDataFromBackup(restorer, data_path_in_backup, partitions);
    }

    /// `RestorerFromBackup` calls it on the same object it restored the data into, after the data of all
    /// tables has been restored, e.g. `EmbeddedRocksDB` reopens its read-only handles here.
    void finalizeRestoreFromBackup() override { getNested()->finalizeRestoreFromBackup(); }

    StoragePtr getNested() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested;

        /// `DETACH`, `DROP` and the server shutdown call `shutdown` before they stop tracking the table,
        /// and they track only what exists by then: the stand-in, or the storage behind it if it has been
        /// materialized. A storage started up after that would run its background tasks for a table that
        /// is no longer attached, outside of the tracking that `cleanupDetachedTables` and the dropped
        /// tables queue rely on. The flags cover a stand-in that is detached or dropped without `shutdown`.
        if (shutdown_called || is_detached || is_dropped)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", getStorageID().getNameForLogs());

        LOG_TRACE(log, "Loading lazy table on first access");

        auto nested_storage = get_nested();
        /// The catalog keeps handing out this stand-in, while the database iterator hands out the storage
        /// behind it, see `unwrapMaterializedLazyTable`. `DROP` and `TRUNCATE` lock the former, `BACKUP`
        /// and `Merge` lock the latter, so the two must share the lock to exclude each other.
        nested_storage->shareDropLockWith(*this);
        nested_storage->startup();
        nested_storage->renameInMemory(getStorageID());
        {
            std::lock_guard ptr_lock{nested_ptr_mutex};
            nested = nested_storage;
        }
        get_nested = {};
        return nested;
    }

    bool storesDataOnDisk() const override { return true; }
    /// Report the restrictive answer instead of loading the nested table just to inspect its on-disk data.
    bool hasUnreplicatedTableDataOnDisk() const override { return true; }
    StoragePolicyPtr getStoragePolicy() const override { return nullptr; }
    bool isView() const override { return false; }

    /// Startup is deferred until first access via `getNested`.
    void startup() override { }

    void shutdown(bool is_drop) override
    {
        std::lock_guard lock{nested_mutex};
        shutdown_called = true;
        if (nested)
            nested->shutdown(is_drop);
    }

    void flushAndPrepareForShutdown() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->flushAndPrepareForShutdown();
    }

    void drop() override
    {
        std::lock_guard lock{nested_mutex};

        if (nested)
        {
            nested->drop();
            return;
        }

        try
        {
            LOG_TRACE(log, "Loading table for drop without startup");

            if (!get_nested)
            {
                LOG_WARNING(log, "Cannot load table for drop, data cleanup will be handled by the database engine");
                return;
            }

            auto nested_storage = get_nested();
            nested_storage->drop();
            get_nested = {};
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to load table for drop: {}. "
                             "Data cleanup will be handled by the database engine.",
                        getCurrentExceptionMessage(false));
        }
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        auto storage = getNested();
        const auto nested_metadata = storage->getInMemoryMetadataPtr(context, false);
        auto nested_snapshot = storage->getStorageSnapshot(nested_metadata, context);
        storage->read(query_plan, column_names, nested_snapshot, query_info, context,
                      processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context,
        bool async_insert) override
    {
        auto storage = getNested();
        auto nested_metadata = storage->getInMemoryMetadataPtr(context, false);
        return storage->write(query, nested_metadata, context, async_insert);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        std::lock_guard lock{nested_mutex};
        IStorage::renameInMemory(new_table_id); // NOLINT(bugprone-parent-virtual-call)
        if (nested)
            nested->renameInMemory(new_table_id);
    }

    /// A table that is still a stand-in has nothing to answer with, and is deliberately not loaded to ask.
    void checkTableCanBeRenamedByDatabaseRename(const String & new_database_name) const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->checkTableCanBeRenamedByDatabaseRename(new_database_name);
    }

    void checkTableCanBeDropped(ContextPtr query_context) const override
    {
        getNested()->checkTableCanBeDropped(query_context);
    }

    void checkTableSizeBelowDropLimit(ContextPtr query_context) const override
    {
        getNested()->checkTableSizeBelowDropLimit(query_context);
    }

    std::optional<UInt64> totalRows(ContextPtr query_context) const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->totalRows(query_context);
        return std::nullopt;
    }

    std::optional<UInt64> totalBytes(ContextPtr query_context) const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->totalBytes(query_context);
        return std::nullopt;
    }

    std::optional<UInt64> lifetimeRows() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->lifetimeRows();
        return std::nullopt;
    }

    std::optional<UInt64> lifetimeBytes() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->lifetimeBytes();
        return std::nullopt;
    }

private:
    mutable std::recursive_mutex nested_mutex; /// Guards both `get_nested` and `nested`.
    mutable std::function<StoragePtr()> get_nested; /// Factory that creates the real storage. Cleared after first use.
    /// Guards `nested` only, and is held only to read or assign the pointer, never during the materialization.
    /// `nested` is assigned under both mutexes, so holding either one is enough to read it.
    mutable std::mutex nested_ptr_mutex;
    mutable StoragePtr nested; /// The materialized real storage, set on first access.
    bool shutdown_called = false; /// Forbids the first materialization, see `getNested`. Guarded by `nested_mutex`.
    LoggerPtr log;
};

/// The storage that actually implements the table engine, for a caller that recognizes an engine by
/// downcasting the object it got from the catalog or from a database iterator. A table of a database
/// with `lazy_load_tables` is kept there as a stand-in, and the catalog keeps the stand-in even after
/// the real storage has been materialized. A table that has not been materialized yet is returned as
/// is: materializing it here would load every lazy table of the server, which is what the setting
/// exists to avoid. With `wait_for_materialization` the call waits for a materialization that is in
/// flight in another thread; pass `false` where a wider lock is held, see `tryGetNestedWithoutWaiting`.
inline StoragePtr unwrapMaterializedLazyTable(const StoragePtr & storage, bool wait_for_materialization = true)
{
    if (const auto * proxy = dynamic_cast<const StorageTableProxy *>(storage.get()))
    {
        if (auto nested = wait_for_materialization ? proxy->tryGetNested() : proxy->tryGetNestedWithoutWaiting())
            return nested;
    }

    return storage;
}

}
