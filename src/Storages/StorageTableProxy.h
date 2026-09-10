#pragma once

#include <functional>

#include <Storages/StorageProxy.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

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
    StoragePtr tryGetNested() const
    {
        std::lock_guard lock{nested_mutex};
        return nested;
    }

    /// The same, but never waits: `nested_mutex` is held for the whole first-access materialization,
    /// which is unbounded (it reads the data parts and starts the table up). A caller that holds a
    /// wider lock - the database mutex in `DatabaseWithOwnTablesBase::getTablesIterator` - must not
    /// block on it, or a single table being loaded stalls every query on the database. A table that
    /// is being materialized right now is reported as not materialized yet, exactly as an untouched
    /// one is.
    StoragePtr tryGetNestedWithoutWaiting() const
    {
        std::unique_lock lock{nested_mutex, std::try_to_lock};
        if (!lock.owns_lock())
            return nullptr;
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

    StoragePtr getNested() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested;

        LOG_TRACE(log, "Loading lazy table on first access");

        auto nested_storage = get_nested();
        nested_storage->startup();
        nested_storage->renameInMemory(getStorageID());
        nested = nested_storage;
        get_nested = {};
        return nested;
    }

    bool storesDataOnDisk() const override { return true; }
    StoragePolicyPtr getStoragePolicy() const override { return nullptr; }
    bool isView() const override { return false; }

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
    mutable StoragePtr nested; /// The materialized real storage, set on first access.
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

/// The same, for a caller that is going to touch the data anyway and so can afford to materialize the
/// table: besides defeating a downcast, an untouched stand-in also answers metadata queries from the
/// columns cached out of the `CREATE TABLE` query alone, without the projections and the indices.
inline StoragePtr materializeLazyTable(const StoragePtr & storage)
{
    if (const auto * proxy = dynamic_cast<const StorageTableProxy *>(storage.get()))
        return proxy->getNested();

    return storage;
}

}
