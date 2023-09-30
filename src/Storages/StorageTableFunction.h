#pragma once
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageProxy.h>
#include <Common/CurrentThread.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}

using GetNestedStorageFunc = std::function<StoragePtr()>;

/// Lazily creates underlying storage.
/// Adds ConversionTransform in case of structure mismatch.
class StorageTableFunctionProxy final : public StorageProxy
{
public:
    StorageTableFunctionProxy(const StorageID & table_id_, GetNestedStorageFunc get_nested_,
            ColumnsDescription cached_columns, bool add_conversion_ = true)
    : StorageProxy(table_id_), get_nested(std::move(get_nested_)), add_conversion(add_conversion_)
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

    String getName() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->getName();
        return StorageProxy::getName();
    }

    void startup() override { }
    void shutdown() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->shutdown();
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
        auto storage = getNested();
        auto nested_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
        storage->read(query_plan, column_names, nested_snapshot, query_info, context,
                                  processed_stage, max_block_size, num_streams);
        if (add_conversion)
        {
            auto from_header = query_plan.getCurrentDataStream().header;
            auto to_header = getHeaderForProcessingStage(column_names, storage_snapshot,
                                                         query_info, context, processed_stage);

            auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                    from_header.getColumnsWithTypeAndName(),
                    to_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Name);

            auto step = std::make_unique<ExpressionStep>(
                query_plan.getCurrentDataStream(),
                convert_actions_dag);

            step->setStepDescription("Converting columns");
            query_plan.addStep(std::move(step));
        }
    }

    SinkToStoragePtr write(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            ContextPtr context,
            bool async_insert) override
    {
        auto storage = getNested();
        auto cached_structure = metadata_snapshot->getSampleBlock();
        auto actual_structure = storage->getInMemoryMetadataPtr()->getSampleBlock();
        if (!blocksHaveEqualStructure(actual_structure, cached_structure) && add_conversion)
        {
            throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Source storage and table function have different structure");
        }
        return storage->write(query, metadata_snapshot, context, async_insert);
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
    const bool add_conversion;
};

}
