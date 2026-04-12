#pragma once
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageProxy.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}

using GetNestedStorageFunc = std::function<StoragePtr()>;

/// Lazily creates underlying storage.
class StorageTableFunctionProxy final : public StorageProxy
{
public:
    StorageTableFunctionProxy(const StorageID & table_id_, GetNestedStorageFunc get_nested_, ColumnsDescription cached_columns)
    : StorageProxy(table_id_), get_nested(std::move(get_nested_))
    {
        StorageInMemoryMetadata cached_metadata;
        cached_metadata.setColumns(std::move(cached_columns));
        setInMemoryMetadata(cached_metadata);
    }

    StoragePtr getNestedImpl() const
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested;

        auto nested_storage = get_nested();
        nested_storage->startup();
        nested_storage->renameInMemory(getStorageID());
        nested = nested_storage;
        get_nested = {};
        return nested;
    }

    StoragePtr getNested() const override
    {
        StoragePtr nested_storage = getNestedImpl();
        assert(!nested_storage->getStoragePolicy());
        assert(!nested_storage->storesDataOnDisk());
        return nested_storage;
    }

    /// Table functions cannot have storage policy and cannot store data on disk.
    /// We may check if table is readonly or stores data on disk on DROP TABLE.
    /// Avoid loading nested table by returning nullptr/false for all table functions.
    StoragePolicyPtr getStoragePolicy() const override { return nullptr; }
    bool storesDataOnDisk() const override { return false; }
    bool supportsReplication() const override { return false; }

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
            nested->drop();
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
        getNested()->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override
    {
        return getNested()->write(query, metadata_snapshot, context, async_insert);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            StorageProxy::renameInMemory(new_table_id);
        else
            IStorage::renameInMemory(new_table_id); /// NOLINT
    }

    bool isView() const override { return false; }
    void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const override {}

private:
    mutable std::recursive_mutex nested_mutex;
    mutable GetNestedStorageFunc get_nested;
    mutable StoragePtr nested;
};

}
