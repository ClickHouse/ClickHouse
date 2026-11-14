#pragma once

#include <atomic>
#include <functional>

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageProxy.h>
#include <base/isSharedPtrUnique.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Lazily creates underlying storage for tables in databases with `lazy_load_tables` setting.
/// Similar to `StorageTableFunctionProxy`, but for real on-disk tables.
class StorageTableProxy final : public StorageProxy
{
public:
    StorageTableProxy(
        const StorageID & table_id_, std::function<StoragePtr()> get_nested_, ColumnsDescription cached_columns, String engine_name_)
        : StorageProxy(table_id_)
        , get_nested(std::move(get_nested_))
        , engine_name(std::move(engine_name_))
        , stores_data_on_disk(engineStoresDataOnDisk(engine_name))
        , supports_replication(engineSupportsReplication(engine_name))
        , checks_database_rename(engineChecksDatabaseRename(engine_name))
        , log(getLogger("StorageTableProxy (" + table_id_.getFullTableName() + ")"))
    {
        StorageInMemoryMetadata cached_metadata;
        cached_metadata.setColumns(std::move(cached_columns));
        setInMemoryMetadata(cached_metadata);
    }

    /// The proxy is an implementation detail, so report the engine from the `CREATE` query until
    /// the real storage can answer for itself. `DETACH` asks this with the database mutex held (to update the
    /// attached-table metrics), so it does not wait for a load in progress, like `storesDataOnDisk`.
    std::string getName() const override
    {
        auto storage = nestedIfResolved();
        return storage ? storage->getName() : engine_name;
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

    StoragePtr tryGetNested() const override
    {
        return nestedIfResolved();
    }

    bool isLazyStandIn() const override { return true; }

    bool isNestedInUse() const override
    {
        std::lock_guard lock{resolved_nested_mutex};
        return nested && !isSharedPtrUnique(nested);
    }

    StoragePtr getNested() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested;

        LOG_TRACE(log, "Loading lazy table on first access");

        /// A storage built for a `DROP` that did not go through is started and used from here on.
        auto nested_storage = built_for_drop ? std::exchange(built_for_drop, nullptr) : get_nested();
        nested_storage->startup();
        {
            /// Renamed under the same lock as `renameInMemory` below, so that a concurrent `RENAME` either sees
            /// the new storage or is seen by it.
            std::lock_guard resolved_lock{resolved_nested_mutex};
            nested_storage->renameInMemory(getStorageID());
            nested = nested_storage;
        }
        get_nested = {};
        is_loaded.store(true, std::memory_order_release);
        return nested;
    }

    /// Answered from the engine while the storage does not exist, like `getName`.
    bool storesDataOnDisk() const override
    {
        auto storage = nestedIfResolved();
        return storage ? storage->storesDataOnDisk() : stores_data_on_disk;
    }

    /// The loaded storage, or nullptr if the table has not been loaded yet. Unlike `tryGetNested`,
    /// never waits for a load in progress: the database calls this with its own mutex held, see
    /// `DatabaseWithOwnTablesBase::replaceLoadedLazyTableUnlocked`.
    StoragePtr getLoadedTable() const
    {
        if (!is_loaded.load(std::memory_order_acquire))
            return nullptr;
        /// Assigned once, before `is_loaded` is set, and never changed after that.
        return nested;
    }

    /// Only asked about a table that is about to be read or written inside a transaction, or written
    /// to on a read-only disk, so loading it costs nothing.
    bool supportsTransactions() const override { return getNested()->supportsTransactions(); }
    bool isStaticStorage() const override { return getNested()->isStaticStorage(); }

    /// Report the restrictive answer instead of loading the nested table just to inspect its on-disk data.
    bool hasUnreplicatedTableDataOnDisk() const override
    {
        std::lock_guard lock{nested_mutex};
        return nested ? nested->hasUnreplicatedTableDataOnDisk() : true;
    }

    bool isView() const override { return false; }

    void cancelBackgroundActivity() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->cancelBackgroundActivity();
    }

    /// Nothing runs in a table that was never started, like in a table created after a global `SYSTEM STOP`.
    ActionLock getActionLock(StorageActionBlockType action_type) override
    {
        std::lock_guard lock{nested_mutex};
        return nested ? nested->getActionLock(action_type) : ActionLock{};
    }

    /// Nothing was locked in a table that was never started, but a loaded one has to resume its background work.
    /// Does not wait for a load in progress, see `getLoadedTable`.
    void onActionLockRemove(StorageActionBlockType action_type) override
    {
        if (auto loaded = getLoadedTable())
            loaded->onActionLockRemove(action_type);
    }

    /// The answers that exist only once the storage does. An observer that walks every table gets
    /// the empty answer rather than loading one. These take only `resolved_nested_mutex` and do not wait
    /// for a load in progress: `RENAME` asks `getDataPaths` with the database mutex held (to create the symlink
    /// of the data directory), while loading the table holds `nested_mutex` and can take the database mutex.
    template <typename Ask>
    auto answerIfLoaded(Ask && ask) const
    {
        auto storage = nestedIfResolved();
        using Answer = decltype(ask(*storage));
        return storage ? ask(*storage) : Answer{};
    }

    /// `system.tables` reads these, so answering must not load the table. While the storage does not
    /// exist they are answered from the `CREATE` query, like `getName`, or reported as unknown.
    StoragePolicyPtr getStoragePolicy() const override
    {
        auto storage = nestedIfResolved();
        return storage ? storage->getStoragePolicy() : nullptr;
    }

    Strings getDataPaths() const override
    {
        return answerIfLoaded([](const IStorage & storage) { return storage.getDataPaths(); });
    }

    bool supportsReplication() const override
    {
        auto storage = nestedIfResolved();
        return storage ? storage->supportsReplication() : supports_replication;
    }

    /// Startup is deferred until first access via `getNested`.
    void startup() override { }

    void shutdown(bool is_drop) override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->shutdown(is_drop);
    }

    void flushAndPrepareForShutdown() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->flushAndPrepareForShutdown();
    }

    /// `DROP` drops the inner tables a storage owns through this, before the storage itself.
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override
    {
        std::lock_guard lock{nested_mutex};
        if (auto storage = tryGetNestedForDrop())
            storage->dropInnerTableIfAny(sync, local_context);
    }

    void drop() override
    {
        std::lock_guard lock{nested_mutex};

        if (nested)
        {
            nested->drop();
            return;
        }

        auto storage = tryGetNestedForDrop();
        if (!storage)
        {
            LOG_WARNING(log, "Cannot load table for drop, data cleanup will be handled by the database engine");
            return;
        }

        try
        {
            storage->drop();
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to drop table: {}. Data cleanup will be handled by the database engine.",
                        getCurrentExceptionMessage(false));
        }
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        auto storage = getNested();
        storage->read(query_plan, column_names, getNestedSnapshot(storage, storage_snapshot, context), query_info, context,
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

    /// `RENAME` calls this, `checkTableCanBeRenamed` and `storesDataOnDisk` with the database mutex held, while loading
    /// the table holds `nested_mutex` and can take the database mutex (a constraint with a scalar subquery looks up
    /// a table in the same database). So these take only `resolved_nested_mutex`.
    void renameInMemory(const StorageID & new_table_id) override
    {
        std::lock_guard lock{resolved_nested_mutex};
        IStorage::renameInMemory(new_table_id); // NOLINT(bugprone-parent-virtual-call)
        if (nested)
            nested->renameInMemory(new_table_id);
    }

    /// `DatabaseAtomic::renameTable` loads the table before taking the database mutex, so the storage exists here
    /// and its restrictions apply. Other callers do not hold the database mutex, and loading is the cost of the check.
    void checkTableCanBeRenamed(const StorageID & new_name) const override
    {
        auto storage = nestedIfResolved();
        if (!storage)
            storage = getNested();
        storage->checkTableCanBeRenamed(new_name);
    }

    /// Whether the table has to be loaded before its database is renamed, so that the engine can answer
    /// `checkTableCanBeRenamedByDatabaseRename`. It cannot be loaded there, with the database mutex held.
    bool needsLoadForDatabaseRename() const
    {
        return checks_database_rename && !nestedIfResolved();
    }

    /// A stand-in of an engine that never refuses the new name is deliberately not loaded to ask. One that may refuse
    /// it, such as `S3Queue` whose Keeper path expands the `{database}` macro, is loaded by `DatabaseAtomic::renameDatabase`
    /// in advance. Should it still be a stand-in (a concurrent `ATTACH TABLE`), the rename is refused rather than
    /// allowed unchecked: that could silently move the table to another Keeper path, losing its progress.
    void checkTableCanBeRenamedByDatabaseRename(const String & new_database_name) const override
    {
        if (auto storage = nestedIfResolved())
            storage->checkTableCanBeRenamedByDatabaseRename(new_database_name);
        else if (checks_database_rename)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot rename the database of table {} to {}, because the table is not loaded yet. Retry the query",
                getStorageID().getNameForLogs(), backQuoteIfNeed(new_database_name));
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
        return answerIfLoaded([&](const IStorage & storage) { return storage.totalRows(query_context); });
    }

    std::optional<UInt64> totalRowsByPartitionPredicate(const ActionsDAG & filter, ContextPtr query_context) const override
    {
        return answerIfLoaded([&](const IStorage & storage) { return storage.totalRowsByPartitionPredicate(filter, query_context); });
    }

    std::optional<UInt64> totalBytesUncompressed(const Settings & settings) const override
    {
        return answerIfLoaded([&](const IStorage & storage) { return storage.totalBytesUncompressed(settings); });
    }

    /// `system.data_skipping_indices` and `system.columns` ask every table for these, so answering
    /// must not load one.
    IndexSizeByName getSecondaryIndexSizes() const override
    {
        return answerIfLoaded([](const IStorage & storage) { return storage.getSecondaryIndexSizes(); });
    }

    ColumnSizeByName getColumnSizes() const override
    {
        return answerIfLoaded([](const IStorage & storage) { return storage.getColumnSizes(); });
    }

    ColumnSizeByName getColumnSizes(const Names & columns, bool calculate_subcolumn_sizes) const override
    {
        return answerIfLoaded([&](const IStorage & storage) { return storage.getColumnSizes(columns, calculate_subcolumn_sizes); });
    }

    SerializationInfoByName getSerializationHints() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->getSerializationHints();
        return SerializationInfoByName{{}};
    }

    std::optional<UInt64> totalBytes(ContextPtr query_context) const override
    {
        return answerIfLoaded([&](const IStorage & storage) { return storage.totalBytes(query_context); });
    }

    std::optional<UInt64> lifetimeRows() const override
    {
        return answerIfLoaded([](const IStorage & storage) { return storage.lifetimeRows(); });
    }

    std::optional<UInt64> lifetimeBytes() const override
    {
        return answerIfLoaded([](const IStorage & storage) { return storage.lifetimeBytes(); });
    }

private:
    static bool engineStoresDataOnDisk(const String & engine_name_)
    {
        const auto * features = StorageFactory::instance().tryGetStorageFeatures(engine_name_);
        return features && features->stores_data_on_disk;
    }

    /// The replicated MergeTree engines are registered with `supports_replication`. A data lake engine replicates
    /// as well, since its data is shared (`StorageObjectStorage::supportsReplication`), but it is not registered so,
    /// and is recognized by the settings it declares instead. This is what the storage says once it exists.
    static bool engineSupportsReplication(const String & engine_name_)
    {
        const auto * features = StorageFactory::instance().tryGetStorageFeatures(engine_name_);
        return features
            && (features->supports_replication || features->has_builtin_setting_fn == &DataLakeStorageSettings::hasBuiltin);
    }

    static bool engineChecksDatabaseRename(const String & engine_name_)
    {
        const auto * features = StorageFactory::instance().tryGetStorageFeatures(engine_name_);
        return features && features->checks_database_rename;
    }

    /// The storage of a table that was never loaded, built without startup for `DROP` and kept so that
    /// `dropInnerTableIfAny` and `drop` work on the same one. Null when it cannot be built.
    StoragePtr tryGetNestedForDrop()
    {
        if (nested)
            return nested;

        if (!built_for_drop && get_nested)
        {
            try
            {
                LOG_TRACE(log, "Loading table for drop without startup");
                built_for_drop = get_nested();
                get_nested = {};
            }
            catch (...)
            {
                LOG_WARNING(log, "Failed to load table for drop: {}. Data cleanup will be handled by the database engine.",
                            getCurrentExceptionMessage(false));
            }
        }
        return built_for_drop;
    }

    /// The storage if it is already loaded, otherwise null. Does not wait for a load in progress.
    StoragePtr nestedIfResolved() const
    {
        std::lock_guard lock{resolved_nested_mutex};
        return nested;
    }

    mutable std::recursive_mutex nested_mutex; /// Guards `get_nested`, `built_for_drop` and loading `nested`.
    /// Guards `nested` for the callers that hold the database mutex. `nested` is written while holding both mutexes.
    /// Nothing that may take `nested_mutex` or a database mutex is called while holding it.
    mutable std::mutex resolved_nested_mutex;
    mutable std::function<StoragePtr()> get_nested; /// Factory that creates the real storage. Cleared after first use.
    const String engine_name; /// Engine from the `CREATE` query, reported until the real storage exists.
    const bool stores_data_on_disk; /// What the engine keeps its data on, reported until the real storage exists.
    const bool supports_replication; /// Whether the engine replicates, reported until the real storage exists.
    const bool checks_database_rename; /// See `needsLoadForDatabaseRename`.
    mutable StoragePtr nested; /// The materialized real storage, set on first access.
    mutable StoragePtr built_for_drop; /// Built without startup for `DROP`, see `tryGetNestedForDrop`.
    mutable std::atomic<bool> is_loaded{false}; /// Set once `nested` has been assigned, see `getLoadedTable`.
    LoggerPtr log;
};

}
