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
        auto nested_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
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
        auto nested_metadata = storage->getInMemoryMetadataPtr();
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

}
