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
    StorageTableProxy(
        const StorageID & table_id_,
        std::function<StoragePtr()> get_nested_,
        ColumnsDescription cached_columns,
        std::function<bool()> may_need_database_rename_guard_)
        : StorageProxy(table_id_)
        , get_nested(std::move(get_nested_))
        , may_need_database_rename_guard(std::move(may_need_database_rename_guard_))
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

        StoragePtr nested_storage;
        /// Only *materializing* the nested storage is allowed to fail closed. A failure of the
        /// nested `drop()` itself must propagate: otherwise a failed drop is reported as success,
        /// and `dropSkipsDataDirectoryCleanup` then tells `DatabaseCatalog::dropTableFinally` to
        /// skip per-disk cleanup and finalize the catalog drop, leaving half-dropped / leaked
        /// shared-storage state.
        try
        {
            LOG_TRACE(log, "Loading table for drop without startup");

            if (!get_nested)
            {
                LOG_WARNING(log, "Cannot load table for drop, data ownership is unknown");
                drop_load_failed = true;
                return;
            }

            nested_storage = get_nested();
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to load table for drop: {}. "
                             "Cleanup of disks with shared metadata will be skipped to avoid "
                             "destructive fallback on shared object storage.",
                        getCurrentExceptionMessage(false));
            /// We could not determine the underlying storage. Report the ownership as unknown
            /// (see `dropDataOwnershipUnknown`) instead of skipping cleanup entirely: the
            /// nested storage may be a `MergeTree` with `leader_election = 1` whose data lives
            /// on shared object storage and is owned by another node — but it may equally be an
            /// ordinary local table, and a blanket skip would leak its `store/<uuid>` directory
            /// permanently on a transient load failure. The catalog cleans node-local disks and
            /// skips only disks with shared metadata in this case.
            drop_load_failed = true;
            return;
        }

        /// Outside the catch: a real drop failure here propagates rather than being converted into
        /// a finalized catalog drop.
        nested_storage->drop();
        get_nested = {};
        /// Keep the dropped storage around so `dropSkipsDataDirectoryCleanup`
        /// can delegate to it: `DatabaseCatalog::dropTableFinally` queries it
        /// right after `drop()` returns to decide whether to skip per-disk
        /// cleanup. Without this, an unloaded `MergeTree` with
        /// `leader_election = 1` would fall back to the unsafe default.
        nested = std::move(nested_storage);
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

    /// Must materialize the nested storage: the default `Atomic` database renames a table
    /// via `checkTableCanBeRenamed` + `renameInMemory` and never calls `rename`, so a no-op
    /// here would let a rename bypass nested-storage guards (e.g. the `leader_election`
    /// rejection in `StorageMergeTree::checkTableCanBeRenamed`) for lazily loaded on-disk tables.
    void checkTableCanBeRenamed(const StorageID & new_name) const override
    {
        getNested()->checkTableCanBeRenamed(new_name);
    }

    /// Same reasoning as `checkTableCanBeRenamed`: the nested-storage guard (the
    /// `leader_election` rejection) must not be bypassed for a lazily loaded on-disk table.
    /// Materializing unconditionally is too blunt here, though: unlike `checkTableCanBeRenamed`,
    /// which runs for the single table being renamed, `RENAME DATABASE` calls this for *every*
    /// table, so it would run the load factory and `startup()` across the whole database. An
    /// otherwise-allowed rename would then depend on each unrelated table starting successfully
    /// — an unloaded `ReplicatedMergeTree` would have to reach Keeper — and a rename ultimately
    /// rejected because of one `leader_election` table would still have started every table
    /// before it.
    ///
    /// `IStorage::checkTableCanBeRenamedByDatabaseRename` is a no-op everywhere except
    /// `StorageMergeTree`, where it only throws under `leader_election`, so the storage-free
    /// predicate below decides it exactly. It is true only when the `CREATE` query names an
    /// engine that `StorageMergeTree` backs *and* `leader_election` resolves to on for that
    /// table (from its own `SETTINGS`, falling back to the server-wide `merge_tree` default);
    /// when either half is false, the nested call is a no-op by construction and skipping it
    /// changes nothing. In particular a lazy `ReplicatedMergeTree`, whose hook is the no-op
    /// one, is not materialized just because the server-wide default happens to be on — which
    /// would fail the rename outright, since that engine refuses to attach under the setting.
    void checkTableCanBeRenamedByDatabaseRename() const override
    {
        {
            std::lock_guard lock{nested_mutex};
            if (!nested && !may_need_database_rename_guard())
                return;
        }
        getNested()->checkTableCanBeRenamedByDatabaseRename();
    }

    bool dropSkipsDataDirectoryCleanup() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->dropSkipsDataDirectoryCleanup();
        /// When the nested storage could not be materialized during `drop()`, the ownership of
        /// the data is *unknown* (see `dropDataOwnershipUnknown` below), which is weaker than a
        /// full cleanup skip: skipping everything here would permanently leak `store/<uuid>` of
        /// an ordinary local table on a transient load failure.
        return false;
    }

    bool dropDataOwnershipUnknown() const override
    {
        std::lock_guard lock{nested_mutex};
        /// Set by `drop()` when it could not materialize the nested storage. The catalog then
        /// cleans node-local disks but skips disks with shared metadata, so a destructive
        /// `removeRecursive` never runs against a path that may be on shared object storage
        /// owned by a `leader_election = 1` peer (see `drop()` for details).
        return !nested && drop_load_failed;
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
    mutable std::recursive_mutex nested_mutex; /// Guards `get_nested`, `nested`, and `drop_load_failed`.
    mutable std::function<StoragePtr()> get_nested; /// Factory that creates the real storage. Cleared after first use.
    mutable StoragePtr nested; /// The materialized real storage, set on first access.
    bool drop_load_failed = false; /// `drop()` could not load the lazy table; force fail-closed cleanup decision.
    /// Storage-free answer to "could `checkTableCanBeRenamedByDatabaseRename` reject this
    /// table?", resolved from the `CREATE` query's engine and settings and the server-wide
    /// `merge_tree` defaults. Lets `RENAME DATABASE` skip materializing tables that cannot
    /// carry the `leader_election` guard. Never `nullptr`.
    std::function<bool()> may_need_database_rename_guard;
    LoggerPtr log;
};

}
